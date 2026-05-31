// Command proxy is a real-time LLM proxy: browsers connect over WebRTC, the
// proxy terminates the peer connection and bridges audio to a streaming LLM
// provider's WebSocket API. Pick a provider with ?model=gemini|doubao.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/thinkinbig/rt-llm-proxy/internal/adaptive"
	"github.com/thinkinbig/rt-llm-proxy/internal/audio"
	"github.com/thinkinbig/rt-llm-proxy/internal/auth"
	"github.com/thinkinbig/rt-llm-proxy/internal/metrics"
	"github.com/thinkinbig/rt-llm-proxy/internal/model"
	"github.com/thinkinbig/rt-llm-proxy/internal/model/doubao"
	"github.com/thinkinbig/rt-llm-proxy/internal/model/gemini"
	"github.com/thinkinbig/rt-llm-proxy/internal/model/loopback"
	"github.com/thinkinbig/rt-llm-proxy/internal/modelcb"
	"github.com/thinkinbig/rt-llm-proxy/internal/ratelimit"
	"github.com/thinkinbig/rt-llm-proxy/internal/rtc"
	"github.com/thinkinbig/rt-llm-proxy/internal/sidechannel"
)

func main() {
	loadDotenv(".env")

	addr := flag.String("addr", ":8080", "listen address")
	redisAddr := flag.String("redis", "", "redis address for rate limiting (empty = disabled)")
	rlMax := flag.Int("rl-max", 10, "max sessions per client per window")
	rlWindow := flag.Duration("rl-window", time.Minute, "rate limit window")
	scMode := flag.String("sidechannel", "off", "transcript side-channel: off|stdout|kafka")
	kafkaBrokers := flag.String("kafka", "", "kafka seed brokers (csv) for -sidechannel=kafka")
	kafkaTopic := flag.String("kafka-topic", "transcripts", "kafka topic for transcript events")
	replayKafka := flag.Bool("replay-kafka", false, "enable best-effort cross-node replay from Kafka on reconnect")
	replayTimeout := flag.Duration("replay-timeout", 300*time.Millisecond, "replay timeout budget when -replay-kafka=true")
	replayLimit := flag.Int("replay-limit", 100, "max replay transcript lines on reconnect")
	modelCBEnable := flag.Bool("model-cb", true, "enable model connect circuit breaker")
	modelCBOpenAfter := flag.Int("model-cb-open-after", 5, "consecutive failures before opening model circuit")
	modelCBOpenFor := flag.Duration("model-cb-open-for", 30*time.Second, "open-state duration for transient model failures")
	modelCBHalfOpenSuccess := flag.Int("model-cb-half-open-success", 3, "successful half-open probes required to close model circuit")
	modelCBAuthOpenFor := flag.Duration("model-cb-auth-open-for", 5*time.Minute, "open-state duration for auth failures (401/403)")
	modelCBOpenAfterGemini := flag.Int("model-cb-open-after-gemini", 0, "override model-cb-open-after for gemini (0 = default)")
	modelCBOpenForGemini := flag.Duration("model-cb-open-for-gemini", 0, "override model-cb-open-for for gemini (0 = default)")
	modelCBHalfOpenSuccessGemini := flag.Int("model-cb-half-open-success-gemini", 0, "override model-cb-half-open-success for gemini (0 = default)")
	modelCBAuthOpenForGemini := flag.Duration("model-cb-auth-open-for-gemini", 0, "override model-cb-auth-open-for for gemini (0 = default)")
	modelCBOpenAfterDoubao := flag.Int("model-cb-open-after-doubao", 0, "override model-cb-open-after for doubao (0 = default)")
	modelCBOpenForDoubao := flag.Duration("model-cb-open-for-doubao", 0, "override model-cb-open-for for doubao (0 = default)")
	modelCBHalfOpenSuccessDoubao := flag.Int("model-cb-half-open-success-doubao", 0, "override model-cb-half-open-success for doubao (0 = default)")
	modelCBAuthOpenForDoubao := flag.Duration("model-cb-auth-open-for-doubao", 0, "override model-cb-auth-open-for for doubao (0 = default)")
	adminAddr := flag.String("admin", "", "admin listen address for /stats + /debug/pprof (empty = off)")
	opusComplexity := flag.Int("opus-complexity", -1, "Opus encoder complexity 0-10 (-1 = libopus default; lower = less CPU)")
	adaptiveMode := flag.String("adaptive", "off", "adaptive Opus complexity under load: off|sessions|drift")
	flag.Parse()

	audio.SetEncoderComplexity(*opusComplexity) // -1 leaves the default

	limiter := ratelimit.New(*redisAddr, *rlMax, *rlWindow)
	// DevVerifier treats the bearer token as the user id; a real deployment
	// injects a verifier that validates a signed token.
	authn := auth.New(auth.DevVerifier{})
	publisher := newPublisher(*scMode, *kafkaBrokers, *kafkaTopic) // nil = off
	breakers := newModelBreakers(*modelCBEnable, modelCBConfigArgs{
		OpenAfter:       *modelCBOpenAfter,
		OpenFor:         *modelCBOpenFor,
		HalfOpenSuccess: *modelCBHalfOpenSuccess,
		AuthOpenFor:     *modelCBAuthOpenFor,
		Gemini: modelcb.Config{
			OpenAfter:       *modelCBOpenAfterGemini,
			OpenFor:         *modelCBOpenForGemini,
			HalfOpenSuccess: *modelCBHalfOpenSuccessGemini,
			AuthOpenFor:     *modelCBAuthOpenForGemini,
		},
		Doubao: modelcb.Config{
			OpenAfter:       *modelCBOpenAfterDoubao,
			OpenFor:         *modelCBOpenForDoubao,
			HalfOpenSuccess: *modelCBHalfOpenSuccessDoubao,
			AuthOpenFor:     *modelCBAuthOpenForDoubao,
		},
	})
	hub, err := rtc.NewHub()
	if err != nil {
		log.Fatalf("init webrtc: %v", err)
	}

	// Adaptive Opus complexity: shed CPU under load, restore quality when idle.
	adaptiveCtl := newAdaptive(*adaptiveMode, hub)
	if adaptiveCtl != nil {
		defer adaptiveCtl.Close()
	}

	// Admin/observability on a separate listener, off the media+control path.
	if *adminAddr != "" {
		go serveAdmin(*adminAddr, hub, publisher, breakers)
	}

	mux := http.NewServeMux()
	mux.Handle("/demo/", http.StripPrefix("/demo/", http.FileServer(http.Dir("demo"))))
	mux.HandleFunc("/", offerHandler(limiter, authn, publisher, breakers, hub, replayConfig{
		Enabled: *replayKafka,
		Timeout: *replayTimeout,
		Limit:   *replayLimit,
	}))

	srv := &http.Server{Addr: *addr, Handler: mux}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		log.Println("shutting down: closing active sessions")
		hub.CloseAll()
		if publisher != nil {
			publisher.Close()
		}
		sdCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(sdCtx)
	}()

	log.Printf("rt-llm-proxy listening on %s", *addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

// serveAdmin runs the observability endpoints on their own listener: /stats
// (JSON snapshot for the load test) and /debug/pprof (CPU/heap profiling, where
// the Opus cgo cost shows up). Kept off the main server so it never shares a
// listener with media or control traffic.
func serveAdmin(addr string, hub *rtc.Hub, publisher sidechannel.Publisher, breakers *modelcb.Manager) {
	mux := http.NewServeMux()
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		var dropped uint64
		if d, ok := publisher.(interface{ Dropped() uint64 }); ok {
			dropped = d.Dropped()
		}
		modelCB := map[string]any{}
		if breakers != nil {
			modelCB = map[string]any{"providers": breakers.Stats()}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"goroutines":          runtime.NumGoroutine(),
			"sessions":            hub.Count(),
			"opus_complexity":     audio.EncoderComplexity(),
			"frame_interval":      metrics.FrameIntervalBuckets(),
			"replay":              metrics.ReplayStats(),
			"model_cb":            modelCB,
			"sidechannel_dropped": dropped,
		})
	})
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	log.Printf("admin (stats + pprof) listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("admin server: %v", err)
	}
}

// newAdaptive starts the chosen adaptive-complexity controller, or returns nil
// for "off". Thresholds are demo-tuned for a single modest node; a real deploy
// calibrates them from the capacity benchmark (docs/bench).
func newAdaptive(mode string, hub *rtc.Hub) interface{ Close() } {
	const interval = 250 * time.Millisecond
	comps := []int{10, 5, 3} // best -> floor
	switch mode {
	case "off":
		return nil
	case "sessions":
		return adaptive.NewSession(hub.Count, audio.SetEncoderComplexity,
			comps, []int{40, 90}, []int{30, 75}, interval)
	case "drift":
		return adaptive.NewDrift(metrics.FrameIntervalBuckets, audio.SetEncoderComplexity,
			comps, 0.10, 0.03, 4, interval)
	default:
		log.Fatalf("unknown -adaptive %q (want off|sessions|drift)", mode)
		return nil
	}
}

// newPublisher builds the transcript side-channel publisher. "off" returns nil
// (the side-channel is disabled, zero overhead), mirroring how an empty -redis
// disables rate limiting.
func newPublisher(mode, brokers, topic string) sidechannel.Publisher {
	switch mode {
	case "off":
		return nil
	case "stdout":
		return sidechannel.Stdout{}
	case "kafka":
		k, err := sidechannel.NewKafka(strings.Split(brokers, ","), topic)
		if err != nil {
			log.Fatalf("sidechannel kafka: %v", err)
		}
		return k
	default:
		log.Fatalf("unknown -sidechannel %q (want off|stdout|kafka)", mode)
		return nil
	}
}

// offerHandler accepts a WebRTC SDP offer (POST /?model=...), spins up the
// chosen provider, and returns the answer SDP.
type replayConfig struct {
	Enabled bool
	Timeout time.Duration
	Limit   int
}

type modelCBConfigArgs struct {
	OpenAfter       int
	OpenFor         time.Duration
	HalfOpenSuccess int
	AuthOpenFor     time.Duration
	Gemini          modelcb.Config
	Doubao          modelcb.Config
}

func newModelBreakers(enabled bool, args modelCBConfigArgs) *modelcb.Manager {
	if !enabled {
		return nil
	}
	base := modelcb.Config{
		OpenAfter:       args.OpenAfter,
		OpenFor:         args.OpenFor,
		HalfOpenSuccess: args.HalfOpenSuccess,
		AuthOpenFor:     args.AuthOpenFor,
	}
	return modelcb.New(base, map[string]modelcb.Config{
		"gemini": args.Gemini,
		"doubao": args.Doubao,
	})
}

func offerHandler(limiter *ratelimit.Limiter, authn *auth.Authenticator, publisher sidechannel.Publisher, breakers *modelcb.Manager, hub *rtc.Hub, replay replayConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Redirect(w, r, "/demo/", http.StatusFound)
			return
		}

		ip := clientIP(r)
		// Allow fails open on Redis errors (returns ok=true), so gate on ok only.
		if ok, _ := limiter.Allow(r.Context(), ip); !ok {
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read body", http.StatusBadRequest)
			return
		}

		// The session outlives this request, so use a background context.
		provider := r.URL.Query().Get("model")
		switch provider {
		case "gemini", "":
			provider = "gemini"
		case "doubao", "loopback":
		default:
			http.Error(w, "unknown model", http.StatusBadRequest)
			return
		}

		if breakers != nil && provider != "loopback" {
			d := breakers.Allow(provider, time.Now())
			if !d.Allowed {
				w.Header().Set("X-Model-CB-State", string(d.State))
				w.Header().Set("X-Model-CB-Reason", d.Reason)
				if d.RetryAfter > 0 {
					retrySec := int(d.RetryAfter / time.Second)
					if retrySec <= 0 {
						retrySec = 1
					}
					w.Header().Set("Retry-After", strconv.Itoa(retrySec))
				}
				http.Error(w, "model circuit open", http.StatusServiceUnavailable)
				return
			}
		}

		var m model.Model
		switch provider {
		case "doubao":
			m, err = doubao.New(context.Background())
		case "loopback":
			m = loopback.New() // fake provider for load testing; no upstream
		case "gemini":
			m, err = gemini.New(context.Background())
		}
		if breakers != nil && provider != "loopback" {
			breakers.Record(provider, err, time.Now())
		}
		if err != nil {
			log.Printf("model connect: %v", err)
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}

		userID := authn.UserID(r)
		sessionID := uuid.NewString()
		startSeq := uint64(0)
		replayStatus := "disabled"
		if replay.Enabled {
			replayStatus = "miss"
		}
		var initialHistory, replayLines []rtc.TranscriptLine
		reqID := strings.TrimSpace(r.Header.Get("X-Session-ID"))
		reqSeq := strings.TrimSpace(r.Header.Get("X-Last-Seq"))
		reqReplayVersion := strings.TrimSpace(r.Header.Get("X-Replay-Version"))
		replayRequested := reqID != "" || reqSeq != "" || reqReplayVersion != ""
		if replayRequested {
			// Strict reconnect protocol: both id+seq are required.
			if reqID == "" || reqSeq == "" {
				replayStatus = "miss"
			} else if reqReplayVersion != "" && reqReplayVersion != "1" {
				w.Header().Set("X-Replay-Version", "1")
				w.Header().Set("X-Replay-Status", "protocol_invalid")
				http.Error(w, "unsupported X-Replay-Version", http.StatusBadRequest)
				return
			} else {
				lastSeq, err := strconv.ParseUint(reqSeq, 10, 64)
				if err != nil {
					w.Header().Set("X-Replay-Version", "1")
					w.Header().Set("X-Replay-Status", "protocol_invalid")
					http.Error(w, "invalid X-Last-Seq", http.StatusBadRequest)
					return
				}
				if knownProvider, maxSeq, known := hub.SessionState(reqID); known {
					if lastSeq > maxSeq {
						w.Header().Set("X-Replay-Version", "1")
						w.Header().Set("X-Replay-Status", "protocol_invalid")
						http.Error(w, "X-Last-Seq exceeds known max seq", http.StatusBadRequest)
						return
					}
					if knownProvider != provider {
						replayStatus = "miss"
					}
				}
				if replayStatus != "miss" {
					memStart := time.Now()
					metrics.ObserveReplayAttempt("memory")
					if full, missing, baseSeq, ok := hub.Resume(reqID, provider, lastSeq); ok {
						metrics.ObserveReplayHit("memory", time.Since(memStart))
						sessionID = reqID
						startSeq = baseSeq
						initialHistory = full
						replayLines = missing
						replayStatus = "memory_hit"
					} else {
						if !replay.Enabled {
							replayStatus = "disabled"
						} else if rp, ok := publisher.(sidechannel.Replayer); ok {
							metrics.ObserveReplayAttempt("kafka")
							kStart := time.Now()
							replayCtx, cancel := context.WithTimeout(r.Context(), replay.Timeout)
							evs, err := rp.Replay(replayCtx, reqID, provider, lastSeq, replay.Limit)
							cancel()
							if err != nil {
								if errors.Is(err, context.DeadlineExceeded) || errors.Is(replayCtx.Err(), context.DeadlineExceeded) {
									metrics.ObserveReplayTimeout("kafka")
									replayStatus = "kafka_timeout"
								} else {
									log.Printf("replay kafka: %v", err)
									metrics.ObserveReplayError("kafka")
									replayStatus = "kafka_error"
								}
							} else if len(evs) > 0 {
								metrics.ObserveReplayHit("kafka", time.Since(kStart))
								sessionID = reqID
								if lastSeq > 0 {
									startSeq = lastSeq
								}
								for _, ev := range evs {
									line := rtc.TranscriptLine{
										Seq:  ev.GetSeq(),
										Role: roleForReplay(ev.GetRole()),
										Text: ev.GetText(),
									}
									replayLines = append(replayLines, line)
									initialHistory = append(initialHistory, line)
									if ev.GetSeq() > startSeq {
										startSeq = ev.GetSeq()
									}
								}
								replayStatus = "kafka_hit"
							} else {
								replayStatus = "miss"
							}
						}
					}
				}
			}
		}

		// Mint/select the id here (not in Serve) so we can stamp it onto the
		// side-channel wrapper before the bridge ever touches the model. The
		// bridge stays oblivious to the side-channel (ARCHITECTURE §2).
		m = sidechannel.Wrap(m, publisher, sidechannel.Meta{
			SessionID: sessionID, UserID: userID, Provider: provider,
		})

		answer, err := hub.Serve(context.Background(), string(body), m, rtc.SessionInfo{
			ID: sessionID, UserID: userID, Provider: provider,
			StartSeq: startSeq, InitialHistory: initialHistory, Replay: replayLines,
		})
		if err != nil {
			log.Printf("rtc serve: %v", err)
			m.Close()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/sdp")
		w.Header().Set("X-Session-ID", sessionID)
		w.Header().Set("X-Replay-Version", "1")
		w.Header().Set("X-Replay-Status", replayStatus)
		io.WriteString(w, answer)
	}
}

func roleForReplay(r sidechannel.Role) string {
	switch r {
	case sidechannel.Role_ROLE_USER:
		return "user"
	case sidechannel.Role_ROLE_MODEL:
		return "model"
	default:
		return "model"
	}
}

// loadDotenv reads KEY=VALUE lines from path (if it exists) into the process
// environment. Existing env vars win, so a real shell export overrides .env.
// Tolerates a leading "export ", blank lines, # comments, and quoted values.
func loadDotenv(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		k = strings.TrimSpace(k)
		v = strings.Trim(strings.TrimSpace(v), `"'`)
		if _, exists := os.LookupEnv(k); !exists {
			os.Setenv(k, v)
		}
	}
}

func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return xff
	}
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}
