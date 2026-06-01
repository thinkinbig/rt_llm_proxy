package rtc

import (
	"context"
	"sync"

	"github.com/pion/webrtc/v4"

	"github.com/thinkinbig/rt-llm-proxy/internal/model"
)

// sessionScope is the RAII handle for one bridge: media context, peer connection,
// and model. Call commit before Serve returns success; otherwise defer abort
// tears everything down without registering the session.
type sessionScope struct {
	hub       *Hub
	ctx       context.Context
	cancel    context.CancelFunc
	pc        *webrtc.PeerConnection
	model     model.Model
	sess      *session
	once      sync.Once
	committed bool
}

func newSessionScope(h *Hub, pc *webrtc.PeerConnection, m model.Model, sess *session) *sessionScope {
	ctx, cancel := context.WithCancel(context.Background())
	return &sessionScope{
		hub:    h,
		ctx:    ctx,
		cancel: cancel,
		pc:     pc,
		model:  m,
		sess:   sess,
	}
}

func (s *sessionScope) mediaCtx() context.Context { return s.ctx }

// commit registers the session with the hub and wires sess.cleanup to Close.
func (s *sessionScope) commit() {
	s.committed = true
	s.hub.add(s.sess)
	s.sess.cleanup = s.Close
}

// abortIfUncommitted closes pc/model and cancels media if Serve failed before commit.
func (s *sessionScope) abortIfUncommitted() {
	if !s.committed {
		s.Close()
	}
}

// Close is idempotent: cancel media, close model and pc, remove from hub if committed.
func (s *sessionScope) Close() {
	s.once.Do(func() {
		s.cancel()
		s.model.Close()
		s.pc.Close()
		if s.committed {
			s.hub.remove(s.sess)
		}
	})
}
