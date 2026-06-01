package offer

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/thinkinbig/rt-llm-proxy/internal/auth"
	"github.com/thinkinbig/rt-llm-proxy/internal/identity"
	"github.com/thinkinbig/rt-llm-proxy/internal/metrics"
	"github.com/thinkinbig/rt-llm-proxy/internal/modelcb"
	"github.com/thinkinbig/rt-llm-proxy/internal/ratelimit"
	"github.com/thinkinbig/rt-llm-proxy/internal/rtc"
	"github.com/thinkinbig/rt-llm-proxy/internal/sidechannel"
	"github.com/thinkinbig/rt-llm-proxy/internal/transcript"
)

// Handler serves the WebRTC SDP offer endpoint (POST /?model=...).
type Handler struct {
	Limiter   *ratelimit.Limiter
	Auth      *auth.Authenticator
	Publisher sidechannel.Publisher
	Breakers  *modelcb.Manager
	Hub       *rtc.Hub
	Models    ModelFactory
	Replay    ReplayConfig
	// TrustProxy controls how the rate-limit key is derived. When false (the
	// safe default), the client-controlled X-Forwarded-For header is ignored and
	// only the TCP peer (RemoteAddr) is used, so a client cannot bypass the
	// limiter by spoofing a unique header per request. Enable only behind a
	// reverse proxy that sets a trustworthy X-Forwarded-For.
	TrustProxy bool
}

// metricsReplayObserver bridges offer.ReplayObserver to internal/metrics.
type metricsReplayObserver struct{}

func (metricsReplayObserver) ObserveAttempt(source string) { metrics.ObserveReplayAttempt(source) }
func (metricsReplayObserver) ObserveHit(source string, d time.Duration) {
	metrics.ObserveReplayHit(source, d)
}
func (metricsReplayObserver) ObserveTimeout(source string) { metrics.ObserveReplayTimeout(source) }
func (metricsReplayObserver) ObserveError(source string)   { metrics.ObserveReplayError(source) }

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/demo/", http.StatusFound)
		return
	}

	ip := h.clientIP(r)
	if ok, _ := h.Limiter.Allow(r.Context(), ip); !ok {
		http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}

	provider, err := ParseProvider(r.URL.Query().Get("model"))
	if err != nil {
		http.Error(w, "unknown model", http.StatusBadRequest)
		return
	}

	if h.Breakers != nil && provider != "loopback" {
		d := h.Breakers.Allow(provider, time.Now())
		if !d.Allowed {
			writeCircuitHeaders(w, d)
			http.Error(w, "model circuit open", http.StatusServiceUnavailable)
			return
		}
	}

	m, err := h.Models.New(context.Background(), provider)
	if h.Breakers != nil && provider != "loopback" {
		h.Breakers.Record(provider, err, time.Now())
	}
	if err != nil {
		log.Printf("model connect: %v", err)
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	// Guard the freshly-dialed model connection so the reconnect-header error
	// returns below can't leak it. Ownership transfers to Hub.Serve (which then
	// closes m on its own error paths), so served flips to true at that call.
	served := false
	defer func() {
		if !served {
			m.Close()
		}
	}()

	userID := h.Auth.UserID(r)
	newSessionID := identity.SessionID(uuid.NewString())

	headers, err := ParseReplayHeaders(
		r.Header.Get("X-Session-ID"),
		r.Header.Get("X-Last-Seq"),
		r.Header.Get("X-Replay-Version"),
	)
	if err != nil {
		var pe *ProtocolInvalidError
		if errors.As(err, &pe) {
			w.Header().Set("X-Replay-Version", ReplayProtocolVersion())
			w.Header().Set("X-Replay-Status", "protocol_invalid")
			http.Error(w, pe.Message, http.StatusBadRequest)
			return
		}
	}

	var kafka KafkaReplayer
	if rp, ok := h.Publisher.(sidechannel.Replayer); ok {
		kafka = rp
	}
	replay, err := ResolveReplay(
		r.Context(),
		provider,
		userID,
		headers,
		h.Replay,
		h.Hub,
		kafka,
		metricsReplayObserver{},
		newSessionID,
	)
	if err != nil {
		var pe *ProtocolInvalidError
		if errors.As(err, &pe) {
			w.Header().Set("X-Replay-Version", ReplayProtocolVersion())
			w.Header().Set("X-Replay-Status", "protocol_invalid")
			http.Error(w, pe.Message, http.StatusBadRequest)
			return
		}
	}

	sessionMeta := transcript.SessionMeta{
		SessionID: replay.SessionID,
		UserID:    userID,
		Provider:  provider,
	}

	// Ownership of m passes to Serve here: it closes m on its own error paths and
	// the session closes it on success, so the defer guard must not also close it.
	served = true
	answer, err := h.Hub.Serve(context.Background(), string(body), m, rtc.SessionInfo{
		ID:             replay.SessionID,
		UserID:         userID,
		Provider:       provider,
		StartSeq:       replay.StartSeq,
		InitialHistory: replay.InitialHistory,
		Replay:         replay.ReplayLines,
		Transcript:     sidechannel.Tap(h.Publisher, sessionMeta),
		OnModelFault:   h.reportModelFault(provider),
	})
	if err != nil {
		log.Printf("rtc serve: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/sdp")
	w.Header().Set("X-Session-ID", string(replay.SessionID))
	w.Header().Set("X-Replay-Version", ReplayProtocolVersion())
	w.Header().Set("X-Replay-Status", replay.Status)
	io.WriteString(w, answer)
}

func writeCircuitHeaders(w http.ResponseWriter, d modelcb.Decision) {
	w.Header().Set("X-Model-CB-State", string(d.State))
	w.Header().Set("X-Model-CB-Reason", d.Reason)
	if d.RetryAfter > 0 {
		retrySec := int(d.RetryAfter / time.Second)
		if retrySec <= 0 {
			retrySec = 1
		}
		w.Header().Set("Retry-After", strconv.Itoa(retrySec))
	}
}

// reportModelFault returns the OnModelFault callback for a session: it feeds an
// early provider-stream failure into the circuit breaker, mirroring how a
// connect failure is recorded. Returns nil when there is nothing to report to
// (no breaker, or loopback), which also disables the bridge-side reporting.
func (h *Handler) reportModelFault(provider string) func(error) {
	if h.Breakers == nil || provider == "loopback" {
		return nil
	}
	return func(err error) {
		h.Breakers.Record(provider, err, time.Now())
	}
}

// clientIP returns the client address used as the rate-limit key. The
// client-controlled X-Forwarded-For header is only consulted when TrustProxy is
// set (i.e. we sit behind a reverse proxy that appends it); otherwise it is
// ignored so a client cannot mint a fresh limiter bucket per request by varying
// the header. When trusted, the rightmost entry is the hop our proxy observed.
func (h *Handler) clientIP(r *http.Request) string {
	if h.TrustProxy {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			parts := strings.Split(xff, ",")
			if ip := strings.TrimSpace(parts[len(parts)-1]); ip != "" {
				return ip
			}
		}
	}
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}
