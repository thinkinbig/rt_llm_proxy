// Command replay consumes transcript side-channel events from Kafka, keeps a
// bounded in-memory index, and serves reconnect replay queries over HTTP.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/thinkinbig/rt-llm-proxy/internal/replayindex"
	"github.com/thinkinbig/rt-llm-proxy/internal/sidechannel"
)

func main() {
	addr := flag.String("addr", ":8090", "HTTP listen address")
	kafkaBrokers := flag.String("kafka", "", "kafka seed brokers (csv)")
	kafkaTopic := flag.String("kafka-topic", "transcripts", "kafka topic to consume")
	group := flag.String("group", "replay-index", "kafka consumer group id")
	maxSessions := flag.Int("max-sessions", 10_000, "max indexed sessions")
	maxLines := flag.Int("max-lines", 512, "max transcript lines per session")
	sessionTTL := flag.Duration("session-ttl", 24*time.Hour, "index TTL since last event")
	flag.Parse()

	if *kafkaBrokers == "" {
		log.Fatal("missing -kafka")
	}

	store := replayindex.NewStore(replayindex.Config{
		MaxSessions:     *maxSessions,
		MaxLinesPerSess: *maxLines,
		SessionTTL:      *sessionTTL,
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*kafkaBrokers, ",")...),
		kgo.ConsumerGroup(*group),
		kgo.ConsumeTopics(*kafkaTopic),
		kgo.FetchMaxWait(250*time.Millisecond),
	)
	if err != nil {
		log.Fatalf("kafka client: %v", err)
	}
	defer cl.Close()

	go consume(ctx, cl, store)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/stats", func(w http.ResponseWriter, _ *http.Request) {
		sessions, events := store.Stats()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"sessions": sessions,
			"events":   events,
		})
	})
	mux.HandleFunc("/v1/replay", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		sessionID := strings.TrimSpace(r.URL.Query().Get("session_id"))
		userID := strings.TrimSpace(r.URL.Query().Get("user_id"))
		provider := strings.TrimSpace(r.URL.Query().Get("provider"))
		afterSeq, err := strconv.ParseUint(r.URL.Query().Get("after_seq"), 10, 64)
		if err != nil {
			http.Error(w, "invalid after_seq", http.StatusBadRequest)
			return
		}
		limit, err := strconv.Atoi(r.URL.Query().Get("limit"))
		if err != nil || limit <= 0 {
			limit = 256
		}
		evs := store.Query(sessionID, userID, provider, afterSeq, limit)
		out := make([]json.RawMessage, 0, len(evs))
		for _, ev := range evs {
			b, err := protojson.Marshal(ev)
			if err != nil {
				http.Error(w, "encode event", http.StatusInternalServerError)
				return
			}
			out = append(out, b)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"events": out})
	})

	srv := &http.Server{Addr: *addr, Handler: mux}
	go func() {
		log.Printf("replay-index listening on %s (topic=%s group=%s)", *addr, *kafkaTopic, *group)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down")
	sdCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(sdCtx)
}

func consume(ctx context.Context, cl *kgo.Client, store *replayindex.Store) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		fetches := cl.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("kafka fetch: %v", err)
			continue
		}
		fetches.EachRecord(func(rec *kgo.Record) {
			ev := &sidechannel.TranscriptEvent{}
			if err := proto.Unmarshal(rec.Value, ev); err != nil {
				return
			}
			store.Ingest(ev)
		})
	}
}
