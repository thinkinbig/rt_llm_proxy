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
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/thinkinbig/rt-llm-proxy/internal/adaptive"
	"github.com/thinkinbig/rt-llm-proxy/internal/audio"
	"github.com/thinkinbig/rt-llm-proxy/internal/auth"
	"github.com/thinkinbig/rt-llm-proxy/internal/metrics"
	"github.com/thinkinbig/rt-llm-proxy/internal/modelcb"
	"github.com/thinkinbig/rt-llm-proxy/internal/offer"
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
	authn := auth.New(auth.DevVerifier{})
	publisher := newPublisher(*scMode, *kafkaBrokers, *kafkaTopic)
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

	adaptiveCtl := newAdaptive(*adaptiveMode, hub)
	if adaptiveCtl != nil {
		defer adaptiveCtl.Close()
	}

	if *adminAddr != "" {
		go serveAdmin(*adminAddr, hub, publisher, breakers)
	}

	offerHandler := &offer.Handler{
		Limiter:   limiter,
		Auth:      authn,
		Publisher: publisher,
		Breakers:  breakers,
		Hub:       hub,
		Models:    offer.ProdModelFactory{},
		Replay: offer.ReplayConfig{
			Enabled: *replayKafka,
			Timeout: *replayTimeout,
			Limit:   *replayLimit,
		},
	}

	mux := http.NewServeMux()
	mux.Handle("/demo/", http.StripPrefix("/demo/", http.FileServer(http.Dir("demo"))))
	mux.Handle("/", offerHandler)

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

func newAdaptive(mode string, hub *rtc.Hub) interface{ Close() } {
	const interval = 250 * time.Millisecond
	comps := []int{10, 5, 3}
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
