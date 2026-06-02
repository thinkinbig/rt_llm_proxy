package offer

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/thinkinbig/rt-llm-proxy/internal/identity"
	"github.com/thinkinbig/rt-llm-proxy/internal/sidechannel"
	"github.com/thinkinbig/rt-llm-proxy/internal/transcript"
)

const replayProtocolVersion = "1"

// ReplayConfig controls optional cross-node replay via the replay-index service.
type ReplayConfig struct {
	Enabled bool
	Timeout time.Duration
	Limit   int
}

// ReplayHeaders are the reconnect headers from the client offer request.
type ReplayHeaders struct {
	Requested bool
	SessionID identity.SessionID
	LastSeq   uint64
	Version   string
	// Incomplete is true when any reconnect header was sent but SessionID or
	// LastSeq (as raw strings) was missing — treated as a miss, not an error.
	Incomplete bool
}

// ProtocolInvalidError is returned when reconnect headers fail strict validation.
type ProtocolInvalidError struct {
	Message string
}

func (e *ProtocolInvalidError) Error() string { return e.Message }

// SessionLookup reads session state for reconnect replay. userID is the
// authenticated identity of the reconnecting request; implementations must only
// return a session that belongs to that same user (ownership check).
type SessionLookup interface {
	SessionState(sessionID identity.SessionID, userID identity.UserID) (provider string, maxSeq uint64, ok bool)
	Resume(sessionID identity.SessionID, userID identity.UserID, provider string, afterSeq uint64) (full, replay []transcript.Line, startSeq uint64, ok bool)
}

// Replayer loads transcript events from the replay-index service. It must only
// return events whose user id matches userID.
type Replayer interface {
	Replay(ctx context.Context, sessionID identity.SessionID, userID identity.UserID, provider string, afterSeq uint64, limit int) ([]*sidechannel.TranscriptEvent, error)
}

// ReplayObserver records replay attempts for metrics. Tests use a noop impl.
type ReplayObserver interface {
	ObserveAttempt(source string)
	ObserveHit(source string, d time.Duration)
	ObserveTimeout(source string)
	ObserveError(source string)
}

type noopReplayObserver struct{}

func (noopReplayObserver) ObserveAttempt(string)            {}
func (noopReplayObserver) ObserveHit(string, time.Duration) {}
func (noopReplayObserver) ObserveTimeout(string)            {}
func (noopReplayObserver) ObserveError(string)              {}

// ReplayOutcome is the reconnect transcript state applied to a new session.
type ReplayOutcome struct {
	SessionID      identity.SessionID
	StartSeq       uint64
	InitialHistory []transcript.Line
	ReplayLines    []transcript.Line
	Status         string
}

// ParseReplayHeaders interprets reconnect headers from the offer request.
func ParseReplayHeaders(sessionID, lastSeqStr, version string) (ReplayHeaders, error) {
	sessionID = strings.TrimSpace(sessionID)
	lastSeqStr = strings.TrimSpace(lastSeqStr)
	version = strings.TrimSpace(version)
	requested := sessionID != "" || lastSeqStr != "" || version != ""
	if !requested {
		return ReplayHeaders{}, nil
	}
	if version != "" && version != replayProtocolVersion {
		return ReplayHeaders{}, &ProtocolInvalidError{Message: "unsupported X-Replay-Version"}
	}
	if sessionID == "" || lastSeqStr == "" {
		return ReplayHeaders{Requested: true, Incomplete: true, Version: version}, nil
	}
	lastSeq, err := strconv.ParseUint(lastSeqStr, 10, 64)
	if err != nil {
		return ReplayHeaders{}, &ProtocolInvalidError{Message: "invalid X-Last-Seq"}
	}
	return ReplayHeaders{
		Requested: true,
		SessionID: identity.SessionID(sessionID),
		LastSeq:   lastSeq,
		Version:   version,
	}, nil
}

// ResolveReplay decides session id, transcript history, and replay status.
func ResolveReplay(
	ctx context.Context,
	provider string,
	userID identity.UserID,
	headers ReplayHeaders,
	cfg ReplayConfig,
	store SessionLookup,
	index Replayer,
	obs ReplayObserver,
	newSessionID identity.SessionID,
) (ReplayOutcome, error) {
	if obs == nil {
		obs = noopReplayObserver{}
	}
	out := ReplayOutcome{
		SessionID: newSessionID,
		Status:    "disabled",
	}
	if cfg.Enabled {
		out.Status = "miss"
	}
	if !headers.Requested || headers.Incomplete {
		return out, nil
	}
	// Anonymous sessions are not reconnectable: without an authenticated
	// identity to bind ownership to, anyone holding a session id could resume
	// (and forcibly take over) another anonymous caller's session. Treat the
	// reconnect as a plain miss and mint a fresh session.
	if userID.Anonymous() {
		return out, nil
	}

	if knownProvider, maxSeq, known := store.SessionState(headers.SessionID, userID); known {
		if headers.LastSeq > maxSeq {
			return out, &ProtocolInvalidError{Message: "X-Last-Seq exceeds known max seq"}
		}
		if knownProvider != provider {
			return out, nil // status stays miss
		}
	}

	memStart := time.Now()
	obs.ObserveAttempt("memory")
	if full, missing, baseSeq, ok := store.Resume(headers.SessionID, userID, provider, headers.LastSeq); ok {
		obs.ObserveHit("memory", time.Since(memStart))
		out.SessionID = headers.SessionID
		out.StartSeq = baseSeq
		out.InitialHistory = full
		out.ReplayLines = missing
		out.Status = "memory_hit"
		return out, nil
	}

	if !cfg.Enabled {
		out.Status = "disabled"
		return out, nil
	}
	if index == nil {
		return out, nil
	}

	obs.ObserveAttempt("index")
	kStart := time.Now()
	replayCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	evs, err := index.Replay(replayCtx, headers.SessionID, userID, provider, headers.LastSeq, cfg.Limit)
	cancel()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(replayCtx.Err(), context.DeadlineExceeded) {
			obs.ObserveTimeout("index")
			out.Status = "index_timeout"
		} else {
			obs.ObserveError("index")
			out.Status = "index_error"
		}
		return out, nil
	}
	if len(evs) == 0 {
		return out, nil
	}

	obs.ObserveHit("index", time.Since(kStart))
	out.SessionID = headers.SessionID
	startSeq := headers.LastSeq
	for _, ev := range evs {
		line := sidechannel.LineFromEvent(ev)
		out.ReplayLines = append(out.ReplayLines, line)
		out.InitialHistory = append(out.InitialHistory, line)
		if ev.GetSeq() > startSeq {
			startSeq = ev.GetSeq()
		}
	}
	out.StartSeq = startSeq
	out.Status = "index_hit"
	return out, nil
}

// ReplayProtocolVersion is the supported X-Replay-Version value.
func ReplayProtocolVersion() string { return replayProtocolVersion }
