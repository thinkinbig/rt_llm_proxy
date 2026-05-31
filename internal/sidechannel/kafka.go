package sidechannel

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// kafkaBuffer bounds the in-process queue in front of the producer. When it
// fills (a slow/broken broker) Publish drops rather than blocking — the media
// path must never wait on Kafka.
const kafkaBuffer = 1024

// Kafka publishes transcript events to a topic, partitioned by user id (falling
// back to session id when anonymous) for per-key ordering. Delivery is
// at-most-once and lossy under pressure: acks=1, no idempotence, drop-on-full.
type Kafka struct {
	cl      *kgo.Client
	topic   string
	ch      chan *TranscriptEvent
	done    chan struct{}
	dropped atomic.Uint64
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
		cl:    cl,
		topic: topic,
		ch:    make(chan *TranscriptEvent, kafkaBuffer),
		done:  make(chan struct{}),
	}
	go k.run()
	return k, nil
}

// Publish enqueues an event, dropping it (and counting the drop) if the buffer
// is full. Non-blocking by construction.
func (k *Kafka) Publish(ev *TranscriptEvent) {
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

// Close stops the drain goroutine and flushes the producer.
func (k *Kafka) Close() error {
	close(k.ch)
	<-k.done
	k.cl.Close()
	return nil
}
