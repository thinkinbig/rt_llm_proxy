package sidechannel

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"google.golang.org/protobuf/proto"

	"github.com/thinkinbig/rt-llm-proxy/internal/identity"
)

// kafkaBuffer bounds the in-process queue in front of the producer. When it
// fills (a slow/broken broker) Publish drops rather than blocking — the media
// path must never wait on Kafka.
const kafkaBuffer = 1024
const replayTailWindow = 4096

// Kafka publishes transcript events to a topic, partitioned by user id (falling
// back to session id when anonymous) for per-key ordering. Delivery is
// at-most-once and lossy under pressure: acks=1, no idempotence, drop-on-full.
type Kafka struct {
	brokers []string
	cl      *kgo.Client
	topic   string
	ch      chan *TranscriptEvent
	done    chan struct{}
	dropped atomic.Uint64

	// mu guards closed and serializes channel sends against Close so a late
	// Publish on a shutting-down server cannot send on a closed channel (panic).
	// RLock lets concurrent sessions publish in parallel; Close takes the
	// write lock once.
	mu     sync.RWMutex
	closed bool
}

// NewKafka connects a producer and starts its drain goroutine. acks=1 trades
// durability for latency (a side-channel is not worth acks=all); idempotence is
// disabled because it requires acks=all.
func NewKafka(brokers []string, topic string) (*Kafka, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.LeaderAck()),
		kgo.DisableIdempotentWrite(),
		kgo.ProducerLinger(5*time.Millisecond),
	)
	if err != nil {
		return nil, err
	}
	k := &Kafka{
		brokers: append([]string(nil), brokers...),
		cl:      cl,
		topic:   topic,
		ch:      make(chan *TranscriptEvent, kafkaBuffer),
		done:    make(chan struct{}),
	}
	go k.run()
	return k, nil
}

// Publish enqueues an event, dropping it (and counting the drop) if the buffer
// is full. Non-blocking by construction.
func (k *Kafka) Publish(ev *TranscriptEvent) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	if k.closed {
		k.dropped.Add(1)
		return
	}
	select {
	case k.ch <- ev:
	default:
		k.dropped.Add(1)
	}
}

func (k *Kafka) run() {
	defer close(k.done)
	for ev := range k.ch {
		val, err := proto.Marshal(ev)
		if err != nil {
			continue
		}
		k.cl.Produce(context.Background(), &kgo.Record{
			Topic: k.topic,
			Key:   []byte(partitionKey(ev)),
			Value: val,
		}, nil) // async, fire-and-forget; loss is acceptable for the side-channel
	}
}

// Replay reads transcript events for one session with seq > afterSeq. It is a
// best-effort bounded scan from the tail of each topic partition, intended for
// reconnect restore (not a full historical export).
func (k *Kafka) Replay(ctx context.Context, sessionID identity.SessionID, userID identity.UserID, provider string, afterSeq uint64, limit int) ([]*TranscriptEvent, error) {
	if sessionID == "" || userID.Anonymous() {
		return nil, nil
	}
	if limit <= 0 {
		limit = 256
	}
	parts, err := k.partitions(ctx)
	if err != nil {
		return nil, err
	}
	if len(parts) == 0 {
		return nil, nil
	}

	assign := map[string]map[int32]kgo.Offset{k.topic: {}}
	for _, p := range parts {
		assign[k.topic][p] = kgo.NewOffset().AtEnd().Relative(-replayTailWindow)
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(k.brokers...),
		kgo.ConsumePartitions(assign),
		kgo.FetchMaxWait(100*time.Millisecond),
		kgo.FetchMaxBytes(1<<20),
	)
	if err != nil {
		return nil, err
	}
	defer cl.Close()

	deadline := time.NewTimer(600 * time.Millisecond)
	defer deadline.Stop()
	seen := 0
	limitScan := replayTailWindow * len(parts)
	matched := make([]*TranscriptEvent, 0, limit)
	for {
		if len(matched) >= limit || seen >= limitScan {
			break
		}
		pollCtx, cancel := context.WithTimeout(ctx, 120*time.Millisecond)
		fetches := cl.PollFetches(pollCtx)
		cancel()
		if err := fetches.Err(); err != nil {
			// Timeout / no records yet is expected when scanning tails.
			if err == context.DeadlineExceeded || err == context.Canceled {
				select {
				case <-deadline.C:
					return sortedBySeq(matched), nil
				default:
					continue
				}
			}
			return nil, err
		}
		if fetches.Empty() {
			select {
			case <-deadline.C:
				return sortedBySeq(matched), nil
			default:
				continue
			}
		}
		fetches.EachRecord(func(rec *kgo.Record) {
			seen++
			ev := &TranscriptEvent{}
			if err := proto.Unmarshal(rec.Value, ev); err != nil {
				return
			}
			if ev.GetSessionId() != string(sessionID) || ev.GetUserId() != string(userID) || ev.GetProvider() != provider || ev.GetSeq() <= afterSeq {
				return
			}
			dup := false
			for _, e := range matched {
				if e.GetSeq() == ev.GetSeq() {
					dup = true
					break
				}
			}
			if !dup {
				matched = append(matched, ev)
			}
		})
	}
	return sortedBySeq(matched), nil
}

func sortedBySeq(events []*TranscriptEvent) []*TranscriptEvent {
	sort.Slice(events, func(i, j int) bool { return events[i].GetSeq() < events[j].GetSeq() })
	return events
}

func (k *Kafka) partitions(ctx context.Context) ([]int32, error) {
	req := kmsg.NewMetadataRequest()
	topicReq := kmsg.NewMetadataRequestTopic()
	topicReq.Topic = &k.topic
	req.Topics = append(req.Topics, topicReq)
	resp, err := req.RequestWith(ctx, k.cl)
	if err != nil {
		return nil, err
	}
	if len(resp.Topics) == 0 || resp.Topics[0].Topic == nil || *resp.Topics[0].Topic != k.topic {
		return nil, nil
	}
	t := resp.Topics[0]
	if t.ErrorCode != 0 {
		return nil, kerr.ErrorForCode(t.ErrorCode)
	}
	parts := make([]int32, 0, len(t.Partitions))
	for _, p := range t.Partitions {
		if p.ErrorCode != 0 {
			continue
		}
		parts = append(parts, p.Partition)
	}
	return parts, nil
}

// partitionKey keeps one user's events ordered in a single partition. Anonymous
// events (empty user id) fall back to the session id so they spread across
// partitions instead of all hashing to the empty key — avoiding a hot partition
// that would also falsely interleave unrelated anonymous sessions.
func partitionKey(ev *TranscriptEvent) string {
	if ev.GetUserId() != "" {
		return ev.GetUserId()
	}
	return ev.GetSessionId()
}

// Dropped returns how many events have been dropped due to a full buffer.
func (k *Kafka) Dropped() uint64 { return k.dropped.Load() }

// Close stops the drain goroutine and flushes the producer. It is idempotent
// and, via mu, guarantees no Publish is mid-send on k.ch when it is closed.
func (k *Kafka) Close() error {
	k.mu.Lock()
	if k.closed {
		k.mu.Unlock()
		return nil
	}
	k.closed = true
	close(k.ch)
	k.mu.Unlock()
	<-k.done
	k.cl.Close()
	return nil
}
