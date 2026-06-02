package main

import (
	"context"
	"encoding/json"
	"errors"
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

func runProxy(cfg runConfig) error {
	audio.SetEncoderComplexity(cfg.OpusComplexity) // -1 leaves the default

	limiter := ratelimit.New(cfg.RedisAddr, cfg.RLMax, cfg.RLWindow)
	authn := auth.New(auth.DevVerifier{})
	publisher := newPublisher(cfg.SidechannelMode, cfg.KafkaBrokers, cfg.KafkaTopic)
	var replayIndex offer.Replayer
	if cfg.ReplayURL != "" {
		replayIndex = sidechannel.NewReplayClient(cfg.ReplayURL)
	}
	breakers := newModelBreakers(cfg.ModelCBEnable, cfg.ModelCB)
	hub, err := rtc.NewHub(os.Getenv("PUBLIC_IP"))
	if err != nil {
		return err
	}

	adaptiveCtl := newAdaptive(cfg.AdaptiveMode, hub)
	if adaptiveCtl != nil {
		defer adaptiveCtl.Close()
	}

	if cfg.AdminAddr != "" {
		go serveAdmin(cfg.AdminAddr, hub, publisher, breakers)
	}

	offerHandler := offer.HandlerFields{
		Limiter:     limiter,
		Auth:        authn,
		Publisher:   publisher,
		ReplayIndex: replayIndex,
		Guard:       breakers,
		Hub:         hub,
		Models: offer.ProdModelFactory{
			Cascade: offer.CascadeConfig{
				WhisperURL: cfg.CascadeWhisperURL,
				LLMURL:     cfg.CascadeLLMURL,
				LLMModel:   cfg.CascadeLLMModel,
				TTSURL:     cfg.CascadeTTSURL,
				System:     cfg.CascadeSystem,
			},
		},
		TrustProxy: cfg.TrustProxy,
		Replay: offer.ReplayConfig{
			Enabled: cfg.ReplayURL != "",
			Timeout: cfg.ReplayTimeout,
			Limit:   cfg.ReplayLimit,
		},
	}.Build()

	mux := http.NewServeMux()
	mux.Handle("/demo/", http.StripPrefix("/demo/", http.FileServer(http.Dir("demo"))))
	mux.Handle("/", offerHandler)
	srv := &http.Server{Addr: cfg.Addr, Handler: mux}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go gracefulShutdown(ctx, srv, hub, publisher)

	log.Printf("rt-llm-proxy listening on %s", cfg.Addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func gracefulShutdown(ctx context.Context, srv *http.Server, hub *rtc.Hub, publisher sidechannel.Publisher) {
	<-ctx.Done()
	log.Println("shutting down")
	// Stop accepting new offers and let in-flight handlers finish first, so
	// no new session is added while CloseAll waits for the live ones to drain.
	sdCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(sdCtx)
	// Tear down live sessions and wait for their goroutines to exit before
	// closing the publisher — otherwise a late transcript Publish could race
	// the publisher shutdown.
	hub.CloseAll()
	if publisher != nil {
		_ = publisher.Close()
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
		_ = json.NewEncoder(w).Encode(map[string]any{
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
