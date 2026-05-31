package sidechannel

import (
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/thinkinbig/rt-llm-proxy/internal/model"
)

// Meta is the per-session identity stamped onto every event from that session.
type Meta struct {
	SessionID string
	UserID    string
	Provider  string
}

// transcriber mirrors rtc's optional STT interface. The bridge type-asserts the
// model to this to decide whether to forward transcripts, so the wrapper must
// satisfy it exactly when (and only when) the inner model does — hence the two
// wrapper types and the assertion in Wrap. Go cannot conditionally implement an
// interface, so this split is load-bearing, not stylistic.
type transcriber interface {
	RecvText() (string, error)
}

// observing wraps a model.Model and taps SendText (user input) into the
// side-channel. The media path (SendAudio/Recv/Close) is passed straight
// through. seq is monotonic per session so a consumer can spot dropped events.
type observing struct {
	model.Model
	pub  Publisher
	meta Meta
	seq  atomic.Uint64
}

func (o *observing) SendText(text string) error {
	o.emit(Role_ROLE_USER, text)
	return o.Model.SendText(text)
}

func (o *observing) emit(role Role, text string) {
	o.pub.Publish(&TranscriptEvent{
		SchemaVersion: 1,
		EventId:       uuid.NewString(),
		SessionId:     o.meta.SessionID,
		UserId:        o.meta.UserID,
		Seq:           o.seq.Add(1),
		Role:          role,
		Text:          text,
		Ts:            time.Now().UnixMilli(),
		Provider:      o.meta.Provider,
	})
}

// observingTranscriber additionally taps RecvText (model output). It exists so
// the wrapper satisfies the bridge's transcriber assertion exactly when the
// inner model does.
type observingTranscriber struct {
	*observing
	inner transcriber
}

func (o *observingTranscriber) RecvText() (string, error) {
	line, err := o.inner.RecvText()
	if err == nil {
		o.observing.emit(Role_ROLE_MODEL, line)
	}
	return line, err
}

// Wrap decorates m so its transcripts flow to pub. If pub is nil the model is
// returned unchanged (zero overhead when the side-channel is off). The returned
// model implements RecvText iff m does, preserving the bridge's behavior.
func Wrap(m model.Model, pub Publisher, meta Meta) model.Model {
	if pub == nil {
		return m
	}
	o := &observing{Model: m, pub: pub, meta: meta}
	if t, ok := m.(transcriber); ok {
		return &observingTranscriber{observing: o, inner: t}
	}
	return o
}
