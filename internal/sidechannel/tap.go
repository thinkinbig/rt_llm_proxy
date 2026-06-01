package sidechannel

import (
	"time"

	"github.com/google/uuid"

	"github.com/thinkinbig/rt-llm-proxy/internal/transcript"
)

// Tap publishes transcript lines to the side-channel using the seq assigned by
// the Bridge recorder. It implements transcript.Listener.
func Tap(pub Publisher, meta transcript.SessionMeta) transcript.Listener {
	if pub == nil {
		return transcript.NopListener{}
	}
	return &tap{pub: pub, meta: meta}
}

type tap struct {
	pub  Publisher
	meta transcript.SessionMeta
}

func (t *tap) OnLine(meta transcript.SessionMeta, line transcript.Line) {
	role := Role_ROLE_MODEL
	if line.Role == "user" {
		role = Role_ROLE_USER
	}
	t.pub.Publish(&TranscriptEvent{
		SchemaVersion: 1,
		EventId:       uuid.NewString(),
		SessionId:     string(meta.SessionID),
		UserId:        string(meta.UserID),
		Seq:           line.Seq,
		Role:          role,
		Text:          line.Text,
		Ts:            time.Now().UnixMilli(),
		Provider:      meta.Provider,
	})
}

// LineFromEvent converts a replayed side-channel event into a transcript line.
func LineFromEvent(ev *TranscriptEvent) transcript.Line {
	return transcript.Line{
		Seq:  ev.GetSeq(),
		Role: roleString(ev.GetRole()),
		Text: ev.GetText(),
	}
}

func roleString(r Role) string {
	switch r {
	case Role_ROLE_USER:
		return "user"
	default:
		return "model"
	}
}
