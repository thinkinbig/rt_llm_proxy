package offer

import (
	"context"
	"encoding/base64"
	"errors"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/thinkinbig/rt-llm-proxy/internal/auth"
	"github.com/thinkinbig/rt-llm-proxy/internal/identity"
	"github.com/thinkinbig/rt-llm-proxy/internal/model"
	"github.com/thinkinbig/rt-llm-proxy/internal/modelcb"
	"github.com/thinkinbig/rt-llm-proxy/internal/ratelimit"
	"github.com/thinkinbig/rt-llm-proxy/internal/rtc"
	"github.com/thinkinbig/rt-llm-proxy/internal/sidechannel"
	"github.com/thinkinbig/rt-llm-proxy/internal/transcript"
)

// Intake is the session offer intake module: control-plane policy (rate limit,
// provider guard, reconnect replay) then Bridge.Serve. HTTP adapters map
// requests into IntakeRequest and write IntakeResult.
type Intake struct {
	Limiter   *ratelimit.Limiter
	Auth      *auth.Authenticator
	Publisher sidechannel.Publisher
	// ReplayIndex queries the replay-index service for cross-node reconnect restore.
	ReplayIndex Replayer
	// Memory fetches the per-user listener brief injected at session start; nil
	// falls back to the dev X-Listener-Brief header.
	Memory   MemoryProvider
	Guard    *modelcb.Manager
	Hub      MediaHub
	Models   ModelFactory
	Replay   ReplayConfig
	Observer ReplayObserver
}

// IntakeRequest is the HTTP-agnostic offer input.
type IntakeRequest struct {
	Ctx                 context.Context
	ClientIP            string
	Model               string
	OfferSDP            []byte
	UserID              identity.UserID
	SessionIDHeader     string
	LastSeqHeader       string
	ReplayVersionHeader string
	// ListenerBriefHeader is the base64 (std) per-session system suffix injected
	// by the trusted orchestrator (X-Listener-Brief). Decoded best-effort.
	ListenerBriefHeader string
}

// maxListenerBrief caps the decoded per-session system suffix to bound context
// cost and limit injection size.
const maxListenerBrief = 8 << 10 // 8 KiB

// decodeListenerBrief decodes the base64 X-Listener-Brief header into the
// per-session system suffix. Best-effort: a malformed header yields "" rather
// than failing the session; oversize input is truncated to valid UTF-8.
func decodeListenerBrief(header string) string {
	if header == "" {
		return ""
	}
	b, err := base64.StdEncoding.DecodeString(header)
	if err != nil {
		log.Printf("offer: bad X-Listener-Brief base64: %v", err)
		return ""
	}
	if len(b) > maxListenerBrief {
		b = b[:maxListenerBrief]
	}
	return strings.ToValidUTF8(string(b), "")
}

// IntakeResult is the offer outcome for the HTTP adapter to write.
type IntakeResult struct {
	Status  int
	Headers map[string]string
	Body    string
}

// ServeOffer runs the control-plane chain and starts the Bridge on success.
func (in *Intake) ServeOffer(req IntakeRequest) IntakeResult {
	ctx := req.Ctx
	if ctx == nil {
		ctx = context.Background()
	}
	// Model sessions outlive the HTTP request/response exchange. Detach model
	// dialing from request cancellation so the media bridge doesn't inherit an
	// already-finished request context and tear down immediately.
	modelCtx := context.WithoutCancel(ctx)
	obs := in.Observer
	if obs == nil {
		obs = metricsReplayObserver{}
	}

	if ok, _ := in.Limiter.Allow(ctx, req.ClientIP); !ok {
		return IntakeResult{
			Status: 429,
			Body:   "rate limit exceeded",
		}
	}

	provider, err := ParseProvider(req.Model)
	if err != nil {
		return IntakeResult{Status: 400, Body: "unknown model"}
	}

	newSessionID := identity.SessionID(uuid.NewString())

	headers, err := ParseReplayHeaders(
		req.SessionIDHeader,
		req.LastSeqHeader,
		req.ReplayVersionHeader,
	)
	if err != nil {
		if pe := protocolInvalid(err); pe != nil {
			return replayProtocolReject(pe.Message)
		}
	}

	now := time.Now()
	if d := in.Guard.AllowDial(provider, now); !d.Allowed {
		return circuitReject(d)
	}

	// Resolve reconnect replay before dialing the model: the restored history
	// must be available at construction for adapters that seed dialogue context
	// at session start (doubao's dialog_context). ResolveReplay does not touch
	// the model, so it can run first; a memory_hit takes over the old live
	// session, but that session's transcript is archived on cleanup, so a failed
	// dial below loses no history (the client can restore from the archive).
	replay, err := ResolveReplay(
		ctx,
		provider,
		req.UserID,
		headers,
		in.Replay,
		in.Hub,
		in.ReplayIndex,
		obs,
		newSessionID,
	)
	if err != nil {
		if pe := protocolInvalid(err); pe != nil {
			return replayProtocolReject(pe.Message)
		}
	}

	params := model.SessionParams{SystemSuffix: resolveBrief(ctx, in.Memory, req.UserID, req.ListenerBriefHeader)}
	if params.SystemSuffix != "" {
		log.Printf("offer: applying listener brief (%d bytes) provider=%s", len(params.SystemSuffix), provider)
	}
	m, err := in.Models.New(modelCtx, provider, restoredTurns(replay.InitialHistory), params)
	in.Guard.RecordDial(provider, err, now)
	if err != nil {
		log.Printf("model connect: %v", err)
		return IntakeResult{Status: 502, Body: err.Error()}
	}

	// served tracks whether m's ownership has left this function. It flips true
	// the moment Serve is called: Serve owns m from then on (closes it on error,
	// keeps it on success), so we must not also close it here — that would be a
	// double Close. Until then, any early return owns m and must release it.
	served := false
	defer func() {
		if !served {
			m.Close()
		}
	}()

	sessionMeta := transcript.SessionMeta{
		SessionID: replay.SessionID,
		UserID:    req.UserID,
		Provider:  provider,
	}

	served = true
	answer, err := in.Hub.Serve(string(req.OfferSDP), m, rtc.SessionInfo{
		ID:             replay.SessionID,
		UserID:         req.UserID,
		Provider:       provider,
		StartSeq:       replay.StartSeq,
		InitialHistory: replay.InitialHistory,
		Replay:         replay.ReplayLines,
		Transcript:     sidechannel.Tap(in.Publisher, sessionMeta),
		StreamFaultAt:  streamFaultBinder(in.Guard, provider),
	})
	if err != nil {
		log.Printf("rtc serve: %v", err)
		return IntakeResult{Status: 500, Body: err.Error()}
	}

	return IntakeResult{
		Status: 200,
		Headers: map[string]string{
			"Content-Type":     "application/sdp",
			"X-Session-ID":     string(replay.SessionID),
			"X-Replay-Version": ReplayProtocolVersion(),
			"X-Replay-Status":  replay.Status,
		},
		Body: answer,
	}
}

// restoredTurns maps reconnect-restored transcript lines to the provider-agnostic
// turns a ModelFactory threads into construction (doubao's dialog_context).
func restoredTurns(lines []transcript.Line) []model.RestoredTurn {
	if len(lines) == 0 {
		return nil
	}
	turns := make([]model.RestoredTurn, 0, len(lines))
	for _, l := range lines {
		turns = append(turns, model.RestoredTurn{Role: l.Role, Text: l.Text})
	}
	return turns
}

func streamFaultBinder(guard *modelcb.Manager, provider string) func(time.Time) func(bool, error) {
	if guard == nil {
		return nil
	}
	return guard.StreamFaultBinder(provider)
}

func protocolInvalid(err error) *ProtocolInvalidError {
	var pe *ProtocolInvalidError
	if errors.As(err, &pe) {
		return pe
	}
	return nil
}

func replayProtocolReject(msg string) IntakeResult {
	return IntakeResult{
		Status: 400,
		Headers: map[string]string{
			"X-Replay-Version": ReplayProtocolVersion(),
			"X-Replay-Status":  StatusProtocolInvalid,
		},
		Body: msg,
	}
}

func circuitReject(d modelcb.Decision) IntakeResult {
	h := map[string]string{
		"X-Model-CB-State":  string(d.State),
		"X-Model-CB-Reason": d.Reason,
	}
	if d.RetryAfter > 0 {
		sec := int(d.RetryAfter / time.Second)
		if sec <= 0 {
			sec = 1
		}
		h["Retry-After"] = strconv.Itoa(sec)
	}
	return IntakeResult{
		Status:  503,
		Headers: h,
		Body:    "model circuit open",
	}
}
