package rtc

import (
	"slices"
	"sync"
	"time"

	"github.com/thinkinbig/rt-llm-proxy/internal/identity"
	"github.com/thinkinbig/rt-llm-proxy/internal/transcript"
)

type sessionArchive struct {
	provider string
	userID   identity.UserID
	history  []transcript.Line
	maxSeq   uint64
	expiry   time.Time
}

// sessionArchiveStore owns bounded in-memory replay archives for disconnected
// sessions. It is intentionally separate from Hub's live-connection registry.
type sessionArchiveStore struct {
	mu       sync.Mutex
	archives map[identity.SessionID]sessionArchive
	ttl      time.Duration
	now      func() time.Time
}

func newSessionArchiveStore(ttl time.Duration, now func() time.Time) *sessionArchiveStore {
	return &sessionArchiveStore{
		archives: make(map[identity.SessionID]sessionArchive),
		ttl:      ttl,
		now:      now,
	}
}

func (s *sessionArchiveStore) put(sessionID identity.SessionID, provider string, userID identity.UserID, history []transcript.Line, maxSeq uint64) {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, arch := range s.archives {
		if now.After(arch.expiry) {
			delete(s.archives, id)
		}
	}
	s.archives[sessionID] = sessionArchive{
		provider: provider,
		userID:   userID,
		history:  slices.Clone(history),
		maxSeq:   maxSeq,
		expiry:   now.Add(s.ttl),
	}
}

func (s *sessionArchiveStore) resume(sessionID identity.SessionID, userID identity.UserID, provider string, afterSeq uint64) (full, replay []transcript.Line, startSeq uint64, ok bool) {
	s.mu.Lock()
	arch, exists := s.archives[sessionID]
	now := s.now()
	if !exists || arch.userID != userID || arch.provider != provider || now.After(arch.expiry) {
		s.mu.Unlock()
		return nil, nil, 0, false
	}
	full = slices.Clone(arch.history)
	startSeq = arch.maxSeq
	s.mu.Unlock()

	for _, line := range full {
		if line.Seq > afterSeq {
			replay = append(replay, line)
		}
	}
	return full, replay, startSeq, true
}

func (s *sessionArchiveStore) state(sessionID identity.SessionID, userID identity.UserID) (provider string, maxSeq uint64, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	arch, exists := s.archives[sessionID]
	if !exists || arch.userID != userID || s.now().After(arch.expiry) {
		return "", 0, false
	}
	return arch.provider, arch.maxSeq, true
}
