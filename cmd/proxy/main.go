// Command proxy is a real-time LLM proxy: browsers connect over WebRTC, the
// proxy terminates the peer connection and bridges audio to a streaming LLM
// provider's WebSocket API. Pick a provider with ?model=gemini|doubao.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/thinkinbig/rt-llm-proxy/internal/model"
	"github.com/thinkinbig/rt-llm-proxy/internal/ratelimit"
	"github.com/thinkinbig/rt-llm-proxy/internal/rtc"
)

func main() {
	loadDotenv(".env")

	addr := flag.String("addr", ":8080", "listen address")
	redisAddr := flag.String("redis", "", "redis address for rate limiting (empty = disabled)")
	rlMax := flag.Int("rl-max", 10, "max sessions per client per window")
	rlWindow := flag.Duration("rl-window", time.Minute, "rate limit window")
	flag.Parse()

	limiter := ratelimit.New(*redisAddr, *rlMax, *rlWindow)
	hub, err := rtc.NewHub()
	if err != nil {
		log.Fatalf("init webrtc: %v", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/demo/", http.StripPrefix("/demo/", http.FileServer(http.Dir("demo"))))
	mux.HandleFunc("/", offerHandler(limiter, hub))

	srv := &http.Server{Addr: *addr, Handler: mux}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		log.Println("shutting down: closing active sessions")
		hub.CloseAll()
		sdCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(sdCtx)
	}()

	log.Printf("rt-llm-proxy listening on %s", *addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

// offerHandler accepts a WebRTC SDP offer (POST /?model=...), spins up the
// chosen provider, and returns the answer SDP.
func offerHandler(limiter *ratelimit.Limiter, hub *rtc.Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Redirect(w, r, "/demo/", http.StatusFound)
			return
		}

		ip := clientIP(r)
		if ok, err := limiter.Allow(r.Context(), ip); err != nil {
			http.Error(w, "rate limiter unavailable", http.StatusInternalServerError)
			return
		} else if !ok {
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read body", http.StatusBadRequest)
			return
		}

		// The session outlives this request, so use a background context.
		var m model.Model
		switch r.URL.Query().Get("model") {
		case "doubao":
			m, err = model.NewDoubao(context.Background())
		case "gemini", "":
			m, err = model.NewGemini(context.Background())
		default:
			http.Error(w, "unknown model", http.StatusBadRequest)
			return
		}
		if err != nil {
			log.Printf("model connect: %v", err)
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}

		answer, err := hub.Serve(context.Background(), string(body), m)
		if err != nil {
			log.Printf("rtc serve: %v", err)
			m.Close()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/sdp")
		io.WriteString(w, answer)
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
