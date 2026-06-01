// Package transcript defines the session-scoped transcript line and the
// recording seam shared by the Bridge, side-channel, and reconnect replay.
//
// Provider adapters emit model.Transcript (role + text, no seq). The Bridge
// assigns seq at the single recording point and produces transcript.Line values
// consumed by the data channel, in-memory history, and side-channel listeners.
package transcript

import "github.com/thinkinbig/rt-llm-proxy/internal/identity"

// Line is one transcript turn within a session. Seq is monotonic per session
// and is the authority for reconnect (X-Last-Seq) and side-channel ordering.
type Line struct {
	Seq  uint64 `json:"seq"`
	Role string `json:"role"` // "user" | "model"
	Text string `json:"text"`
}

// SessionMeta identifies the session that produced a line.
type SessionMeta struct {
	SessionID identity.SessionID
	UserID    identity.UserID
	Provider  string
}

// Listener receives lines after the Bridge records them. Implementations must
// not block the caller; the media path calls this on the hot transcript path.
type Listener interface {
	OnLine(meta SessionMeta, line Line)
}

// NopListener discards all lines. Used when no side-channel is configured.
type NopListener struct{}

func (NopListener) OnLine(SessionMeta, Line) {}
