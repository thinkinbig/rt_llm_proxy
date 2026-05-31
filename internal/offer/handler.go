package offer

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/thinkinbig/rt-llm-proxy/internal/auth"
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

	ip := ClientIP(r)
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

	userID := h.Auth.UserID(r)
	newSessionID := uuid.NewString()

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

	answer, err := h.Hub.Serve(context.Background(), string(body), m, rtc.SessionInfo{
		ID:             replay.SessionID,
		UserID:         userID,
		Provider:       provider,
		StartSeq:       replay.StartSeq,
		InitialHistory: replay.InitialHistory,
		Replay:         replay.ReplayLines,
		Transcript:     sidechannel.Tap(h.Publisher, sessionMeta),
	})
	if err != nil {
		log.Printf("rtc serve: %v", err)
		m.Close()
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/sdp")
	w.Header().Set("X-Session-ID", replay.SessionID)
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

// ClientIP returns the client address for rate limiting.
func ClientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return xff
	}
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}
