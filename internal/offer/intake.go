package offer

import (
	"context"
	"errors"
	"log"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/thinkinbig/rt-llm-proxy/internal/auth"
	"github.com/thinkinbig/rt-llm-proxy/internal/identity"
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
	Guard       *modelcb.Manager
	Hub         MediaHub
	Models      ModelFactory
	Replay      ReplayConfig
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

	now := time.Now()
	if d := in.Guard.AllowDial(provider, now); !d.Allowed {
		return circuitReject(d)
	}

	m, err := in.Models.New(modelCtx, provider)
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
			"X-Replay-Status":  "protocol_invalid",
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
