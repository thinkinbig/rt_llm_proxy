// Package replayindex holds an in-memory transcript index consumed from the
// side-channel Kafka topic and queried by the replay service on reconnect.
package replayindex

import (
	"sort"
	"sync"
	"time"

	"github.com/thinkinbig/rt-llm-proxy/internal/sidechannel"
)

// Config bounds the in-memory index.
type Config struct {
	MaxSessions      int
	MaxLinesPerSess  int
	SessionTTL       time.Duration
	Now              func() time.Time
}

// Store indexes transcript events by session_id for bounded reconnect replay.
type Store struct {
	cfg      Config
	mu       sync.RWMutex
	sessions map[string]*sessionIndex
}

type sessionIndex struct {
	userID   string
	provider string
	events   []*sidechannel.TranscriptEvent
	maxSeq   uint64
	expiry   time.Time
}

// NewStore creates an empty index store.
func NewStore(cfg Config) *Store {
	if cfg.MaxSessions <= 0 {
		cfg.MaxSessions = 10_000
	}
	if cfg.MaxLinesPerSess <= 0 {
		cfg.MaxLinesPerSess = 512
	}
	if cfg.SessionTTL <= 0 {
		cfg.SessionTTL = 24 * time.Hour
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Store{
		cfg:      cfg,
		sessions: make(map[string]*sessionIndex),
	}
}

// Ingest records one side-channel event. Anonymous sessions are ignored because
// reconnect replay requires an authenticated user_id binding.
func (s *Store) Ingest(ev *sidechannel.TranscriptEvent) {
	if ev == nil || ev.GetSessionId() == "" || ev.GetUserId() == "" {
		return
	}
	now := s.cfg.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.evictExpired(now)

	idx := s.sessions[ev.GetSessionId()]
	if idx == nil {
		idx = &sessionIndex{
			userID:   ev.GetUserId(),
			provider: ev.GetProvider(),
			expiry:   now.Add(s.cfg.SessionTTL),
		}
		s.sessions[ev.GetSessionId()] = idx
		if len(s.sessions) > s.cfg.MaxSessions {
			s.evictOldest()
		}
	}
	idx.userID = ev.GetUserId()
	idx.provider = ev.GetProvider()
	idx.expiry = now.Add(s.cfg.SessionTTL)

	for _, existing := range idx.events {
		if existing.GetSeq() == ev.GetSeq() {
			return
		}
	}
	idx.events = append(idx.events, ev)
	sort.Slice(idx.events, func(i, j int) bool {
		return idx.events[i].GetSeq() < idx.events[j].GetSeq()
	})
	if len(idx.events) > s.cfg.MaxLinesPerSess {
		idx.events = idx.events[len(idx.events)-s.cfg.MaxLinesPerSess:]
	}
	if ev.GetSeq() > idx.maxSeq {
		idx.maxSeq = ev.GetSeq()
	}
}

// Query returns events for one session with seq > afterSeq, sorted by seq.
func (s *Store) Query(sessionID, userID, provider string, afterSeq uint64, limit int) []*sidechannel.TranscriptEvent {
	if sessionID == "" || userID == "" {
		return nil
	}
	if limit <= 0 {
		limit = 256
	}
	now := s.cfg.Now()
	s.mu.RLock()
	idx := s.sessions[sessionID]
	if idx == nil || idx.userID != userID || idx.provider != provider || now.After(idx.expiry) {
		s.mu.RUnlock()
		return nil
	}
	out := make([]*sidechannel.TranscriptEvent, 0, limit)
	for _, ev := range idx.events {
		if ev.GetSeq() <= afterSeq {
			continue
		}
		out = append(out, ev)
		if len(out) >= limit {
			break
		}
	}
	s.mu.RUnlock()
	return out
}

// Stats returns coarse index counters for health checks.
func (s *Store) Stats() (sessions int, events int) {
	now := s.cfg.Now()
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, idx := range s.sessions {
		if now.After(idx.expiry) {
			continue
		}
		sessions++
		events += len(idx.events)
	}
	return sessions, events
}

func (s *Store) evictExpired(now time.Time) {
	for id, idx := range s.sessions {
		if now.After(idx.expiry) {
			delete(s.sessions, id)
		}
	}
}

func (s *Store) evictOldest() {
	var oldestID string
	var oldestExpiry time.Time
	first := true
	for id, idx := range s.sessions {
		if first || idx.expiry.Before(oldestExpiry) {
			oldestID = id
			oldestExpiry = idx.expiry
			first = false
		}
	}
	if oldestID != "" {
		delete(s.sessions, oldestID)
	}
}
