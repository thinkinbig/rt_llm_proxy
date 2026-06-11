package offer

import (
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/thinkinbig/rt-llm-proxy/internal/auth"
	"github.com/thinkinbig/rt-llm-proxy/internal/metrics"
	"github.com/thinkinbig/rt-llm-proxy/internal/modelcb"
	"github.com/thinkinbig/rt-llm-proxy/internal/ratelimit"
	"github.com/thinkinbig/rt-llm-proxy/internal/sidechannel"
)

// Handler serves the WebRTC SDP offer endpoint (POST /?model=...).
type Handler struct {
	*Intake
	// TrustProxy controls how the rate-limit key is derived. When false (the
	// safe default), the client-controlled X-Forwarded-For header is ignored and
	// only the TCP peer (RemoteAddr) is used, so a client cannot bypass the
	// limiter by spoofing a unique header per request. Enable only behind a
	// reverse proxy that sets a trustworthy X-Forwarded-For.
	TrustProxy bool
}

// NewHandler builds a Handler with an Intake wired from the same dependencies.
func NewHandler(in Intake, trustProxy bool) *Handler {
	return &Handler{Intake: &in, TrustProxy: trustProxy}
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

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}

	res := h.ServeOffer(IntakeRequest{
		Ctx:                 r.Context(),
		ClientIP:            h.clientIP(r),
		Model:               r.URL.Query().Get("model"),
		OfferSDP:            body,
		UserID:              h.Auth.UserID(r),
		SessionIDHeader:     r.Header.Get("X-Session-ID"),
		LastSeqHeader:       r.Header.Get("X-Last-Seq"),
		ReplayVersionHeader: r.Header.Get("X-Replay-Version"),
		ListenerBriefHeader: r.Header.Get("X-Listener-Brief"),
	})
	writeIntakeResult(w, res)
}

func writeIntakeResult(w http.ResponseWriter, res IntakeResult) {
	for k, v := range res.Headers {
		w.Header().Set(k, v)
	}
	if res.Status == 0 {
		res.Status = http.StatusOK
	}
	if res.Status >= 400 {
		http.Error(w, res.Body, res.Status)
		return
	}
	w.WriteHeader(res.Status)
	io.WriteString(w, res.Body)
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

// HandlerFields groups dependencies for NewHandler (cmd/proxy wiring).
type HandlerFields struct {
	Limiter    *ratelimit.Limiter
	Auth       *auth.Authenticator
	Publisher   sidechannel.Publisher
	ReplayIndex Replayer
	Guard       *modelcb.Manager
	Hub         MediaHub
	Models      ModelFactory
	Replay      ReplayConfig
	TrustProxy bool
}

// Build constructs a Handler from HandlerFields.
func (f HandlerFields) Build() *Handler {
	return NewHandler(Intake{
		Limiter:   f.Limiter,
		Auth:      f.Auth,
		Publisher:   f.Publisher,
		ReplayIndex: f.ReplayIndex,
		Guard:       f.Guard,
		Hub:       f.Hub,
		Models:    f.Models,
		Replay:    f.Replay,
	}, f.TrustProxy)
}
