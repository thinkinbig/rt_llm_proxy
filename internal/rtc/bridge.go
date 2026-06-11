// Package rtc terminates browser WebRTC peer connections and bridges their
// audio (and data-channel text) to a model.Model. It is a Go port of the
// reference proxy.py: decode inbound Opus -> PCM -> model; model PCM -> Opus ->
// browser. No STUN/TURN is configured (host candidates only), matching the
// reference's iceServers=[] — the proxy is not NAT-traversal infrastructure.
//
// A Hub owns the shared pion API (built once with an Opus-tuned MediaEngine)
// and tracks live sessions so they can be torn down on shutdown.
package rtc

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"

	"github.com/thinkinbig/rt-llm-proxy/internal/audio"
	"github.com/thinkinbig/rt-llm-proxy/internal/identity"
	"github.com/thinkinbig/rt-llm-proxy/internal/metrics"
	"github.com/thinkinbig/rt-llm-proxy/internal/model"
	"github.com/thinkinbig/rt-llm-proxy/internal/rtc/dcproto"
	"github.com/thinkinbig/rt-llm-proxy/internal/transcript"
)

const (
	frameSamples = audio.OpusRate * 20 / 1000 // 960 samples = 20ms @ 48kHz
	frameDur     = 20 * time.Millisecond

	// Advertised on the answer so the browser encodes mic audio with FEC + DTX
	// and a narrowband-ish cap, matching the reference's SDP rewrite.
	opusFmtp = "minptime=10;useinbandfec=1;usedtx=1;maxaveragebitrate=16000"

	// archiveTTL bounds how long a disconnected session's transcript is kept for
	// reconnect. It doubles as the reconnect deadline: a client that does not
	// reconnect within this window loses its in-memory resume state. Without it
	// the archives map would grow unbounded on a long-lived host.
	archiveTTL = 5 * time.Minute

)

// Hub holds the shared WebRTC API and the registry of active sessions, keyed by
// server-minted session id. It doubles as the SessionManager.
type Hub struct {
	api   *webrtc.API
	mu    sync.Mutex
	conns map[identity.SessionID]*session
	// archives owns disconnected-session replay state.
	archives *sessionArchiveStore
	// wg tracks the per-session media/transcript goroutines so CloseAll can wait
	// for them to drain before the process tears down shared dependencies (e.g.
	// the side-channel publisher).
	wg sync.WaitGroup
}

// session is one live bridge. id is minted server-side; userID is the resolved
// identity ("" = anonymous).
type session struct {
	id        identity.SessionID
	userID    identity.UserID
	provider  string
	createdAt time.Time
	rec       *transcript.Recorder
	cleanup   func()
}

// NewHub builds the pion API with a custom MediaEngine (Opus + our fmtp) and
// the default interceptors.
//
// publicIP, if non-empty, is added as a NAT1To1 mapping so pion advertises it
// in ICE candidates instead of the private interface address. Required when the
// proxy runs behind a cloud VM's 1:1 NAT (e.g. Volcano Engine / AWS / GCP) and
// browsers connect from the public internet.
func NewHub(publicIP string) (*Hub, error) {
	me := &webrtc.MediaEngine{}
	if err := me.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:    webrtc.MimeTypeOpus,
			ClockRate:   48000,
			Channels:    2,
			SDPFmtpLine: opusFmtp,
		},
		PayloadType: 111,
	}, webrtc.RTPCodecTypeAudio); err != nil {
		return nil, err
	}
	ir := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(me, ir); err != nil {
		return nil, err
	}
	se := &webrtc.SettingEngine{}
	if publicIP != "" {
		se.SetICEAddressRewriteRules(webrtc.ICEAddressRewriteRule{ //nolint:errcheck
			External:        []string{publicIP},
			AsCandidateType: webrtc.ICECandidateTypeHost,
		})
	}
	api := webrtc.NewAPI(
		webrtc.WithMediaEngine(me),
		webrtc.WithInterceptorRegistry(ir),
		webrtc.WithSettingEngine(*se),
	)
	return &Hub{
		api:      api,
		conns:    make(map[identity.SessionID]*session),
		archives: newSessionArchiveStore(archiveTTL, time.Now),
	}, nil
}

// CloseAll tears down every active session and waits for their goroutines to
// exit (used on graceful shutdown). Callers must stop accepting new sessions
// (e.g. http.Server.Shutdown) before calling this, otherwise the wait can race
// a freshly added session.
func (h *Hub) CloseAll() {
	h.mu.Lock()
	snapshot := make([]*session, 0, len(h.conns))
	for _, s := range h.conns {
		snapshot = append(snapshot, s)
	}
	h.mu.Unlock()
	for _, s := range snapshot {
		s.cleanup()
	}
	h.wg.Wait()
}

func (h *Hub) add(s *session) {
	h.mu.Lock()
	h.conns[s.id] = s
	h.mu.Unlock()
}

func (h *Hub) remove(s *session) {
	snapshot := s.rec.FullHistory()
	maxSeq := s.rec.MaxSeq()
	h.mu.Lock()
	delete(h.conns, s.id)
	h.mu.Unlock()
	if len(snapshot) > 0 {
		h.archives.put(s.id, s.provider, s.userID, snapshot, maxSeq)
	}
}

// Count returns the number of currently active sessions.
func (h *Hub) Count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.conns)
}

// Resume returns transcript lines whose seq is greater than afterSeq for a
// known session owned by userID. If the old session is still active, it is
// cleaned up first so the reconnect owns that session id. A session is resumed
// only when both provider and userID match; a mismatch (or an empty userID, or
// an expired archive) is reported as not-found so it degrades to a fresh
// session rather than leaking or hijacking another user's transcript.
func (h *Hub) Resume(sessionID identity.SessionID, userID identity.UserID, provider string, afterSeq uint64) (full, replay []transcript.Line, startSeq uint64, ok bool) {
	if sessionID == "" || userID.Anonymous() {
		return nil, nil, 0, false
	}
	var takeover func()
	var active *session
	h.mu.Lock()
	if s, exists := h.conns[sessionID]; exists {
		if s.provider != provider || s.userID != userID {
			h.mu.Unlock()
			return nil, nil, 0, false
		}
		active = s
		takeover = s.cleanup
	}
	h.mu.Unlock()
	if active != nil {
		full = active.rec.FullHistory()
		startSeq = active.rec.MaxSeq()
		if takeover != nil {
			takeover()
		}
		for _, line := range full {
			if line.Seq > afterSeq {
				replay = append(replay, line)
			}
		}
		return full, replay, startSeq, true
	}
	return h.archives.resume(sessionID, userID, provider, afterSeq)
}

// SessionState returns provider/max seq for a session id owned by userID, from
// active sessions or archives. It enforces the same ownership rule as Resume:
// an empty userID, a user mismatch, or an expired archive is reported as
// not-found.
func (h *Hub) SessionState(sessionID identity.SessionID, userID identity.UserID) (provider string, maxSeq uint64, ok bool) {
	if sessionID == "" || userID.Anonymous() {
		return "", 0, false
	}
	var active *session
	h.mu.Lock()
	if s, exists := h.conns[sessionID]; exists {
		if s.userID != userID {
			h.mu.Unlock()
			return "", 0, false
		}
		active = s
	}
	h.mu.Unlock()
	if active != nil {
		return active.provider, active.rec.MaxSeq(), true
	}
	return h.archives.state(sessionID, userID)
}

// SessionInfo is the identity the handler resolved for a new session: the
// server-minted id, the user id ("" = anonymous), and the chosen provider.
type SessionInfo struct {
	ID             identity.SessionID
	UserID         identity.UserID
	Provider       string
	StartSeq       uint64
	InitialHistory []transcript.Line
	Replay         []transcript.Line
	Transcript     transcript.Listener
	// StreamFaultAt, if set, is called once the session start time is known.
	// It returns a reporter for early provider stream faults (see
	// modelcb.EarlyFaultWindow). nil disables reporting (e.g. loopback).
	StreamFaultAt func(sessionStart time.Time) func(producedAudio bool, err error)
}

type sampleWriter interface {
	WriteSample(media.Sample) error
}

type pcmReceiver interface {
	Recv() ([]int16, error)
}

type frameEncoder interface {
	Encode([]int16) ([]byte, error)
}

// textSender is the narrow data-channel surface the outbound text pumps need:
// *webrtc.DataChannel satisfies it, and tests fake it without a PeerConnection.
type textSender interface {
	SendText(string) error
	ReadyState() webrtc.DataChannelState
}

// rtpReader / frameDecoder / audioSink are the narrow inbound-audio surfaces:
// *webrtc.TrackRemote, the opus decoder, and model.Model satisfy them so
// readInboundLoop's decode/forward logic is testable against fakes.
type rtpReader interface {
	ReadRTP() (*rtp.Packet, interceptor.Attributes, error)
}

type frameDecoder interface {
	Decode([]byte) ([]int16, error)
}

type audioSink interface {
	SendAudio([]int16) error
}

// Serve sets up the peer connection for one session and returns the answer SDP.
// Media bridging runs in background goroutines that tear down when the
// connection drops, the model session ends, or the hub is closed.
// Serve takes ownership of m: on every error return it closes m (directly on the
// early setup failures, via sessionScope.Close once committed), and on success
// the session owns and closes it. Callers must not close m after calling Serve.
// Session media lifetime is independent of any HTTP request context (see sessionScope).
func (h *Hub) Serve(offerSDP string, m model.Model, info SessionInfo) (string, error) {
	pc, err := h.api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		m.Close()
		return "", err
	}

	// Outbound track: model audio -> browser.
	out, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2},
		"audio", "rt-llm-proxy")
	if err != nil {
		pc.Close()
		m.Close()
		return "", err
	}
	sender, err := pc.AddTrack(out)
	if err != nil {
		pc.Close()
		m.Close()
		return "", err
	}

	meta := transcript.SessionMeta{
		SessionID: info.ID,
		UserID:    info.UserID,
		Provider:  info.Provider,
	}
	started := time.Now()
	sess := &session{
		id:        info.ID,
		userID:    info.UserID,
		provider:  info.Provider,
		createdAt: started,
		rec: transcript.NewRecorder(
			info.StartSeq,
			info.InitialHistory,
			256,
			meta,
			info.Transcript,
		),
	}
	// Resolve the model's optional capabilities once; the wiring below reads the
	// resolved set instead of scattering type assertions.
	caps := model.Resolve(m)

	// Reconnect: seed the freshly-dialed model with the restored conversation so
	// it resumes with dialogue context instead of amnesia. Best-effort and
	// provider-dependent — models that can't accept injected context (pure
	// speech-to-speech, e.g. doubao) don't implement ContextRestorer and are
	// skipped. The proxy recorder is already seeded above via InitialHistory.
	if caps.Restorer != nil && len(info.InitialHistory) > 0 {
		if err := caps.Restorer.RestoreContext(restoredTurns(info.InitialHistory)); err != nil {
			log.Printf("rtc: context restore: %v", err)
		}
	}

	scope := newSessionScope(h, pc, m, sess)
	defer scope.abortIfUncommitted()

	// Drain RTCP so the send buffer doesn't fill up.
	h.wg.Go(func() {
		buf := make([]byte, 1500)
		for {
			if _, _, err := sender.Read(buf); err != nil {
				return
			}
		}
	})

	// Inbound audio: browser mic -> model. Accept only the first audio track.
	var trackOnce sync.Once
	pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		if track.Kind() != webrtc.RTPCodecTypeAudio {
			return
		}
		trackOnce.Do(func() {
			h.wg.Go(func() { readInbound(track, m) })
		})
	})

	// Data channel: browser text/tool-results -> model; model transcripts/
	// tool-calls -> browser.
	var replayOnce sync.Once
	pc.OnDataChannel(func(dc *webrtc.DataChannel) {
		dc.OnMessage(func(msg webrtc.DataChannelMessage) {
			if !msg.IsString {
				return
			}
			// A tool-result envelope is routed to the model; anything else is
			// treated as user text input.
			if caps.Tools != nil {
				if res, ok := dcproto.Decode(msg.Data); ok {
					_ = caps.Tools.SendToolResult(res)
					return
				}
			}
			sess.rec.Record("user", string(msg.Data))
			_ = m.SendText(string(msg.Data))
		})
		if caps.Transcriber != nil || caps.Tools != nil {
			start := func() {
				if caps.Transcriber != nil {
					replayOnce.Do(func() { sendReplay(dc, info.Replay) })
					h.wg.Go(func() { forwardTranscripts(scope.mediaCtx(), dc, caps.Transcriber, sess) })
				}
				if caps.Tools != nil {
					h.wg.Go(func() { forwardToolCalls(scope.mediaCtx(), dc, caps.Tools) })
				}
			}
			if dc.ReadyState() == webrtc.DataChannelStateOpen {
				start()
			} else {
				dc.OnOpen(start)
			}
		}
	})

	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		log.Printf("rtc: connection state %s", s)
		switch s {
		case webrtc.PeerConnectionStateFailed,
			webrtc.PeerConnectionStateClosed,
			webrtc.PeerConnectionStateDisconnected:
			sess.cleanup()
		}
	})

	var reportStreamFault func(producedAudio bool, err error)
	if info.StreamFaultAt != nil {
		reportStreamFault = info.StreamFaultAt(started)
	}
	// Outbound pump: model audio -> browser.
	h.wg.Go(func() { writeOutbound(scope.mediaCtx(), out, m, reportStreamFault, info.ID) })

	// VAD interruption monitor: only when the model resolved as an Interrupter
	// (implements it AND reports interruption active for this session).
	if caps.Interrupter != nil {
		h.wg.Go(func() { monitorInterruption(scope.mediaCtx(), caps.Interrupter) })
	}

	if err := pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer, SDP: offerSDP,
	}); err != nil {
		return "", err
	}
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		return "", err
	}
	gather := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(answer); err != nil {
		return "", err
	}
	<-gather // non-trickle: return the full SDP with candidates

	scope.commit()
	return pc.LocalDescription().SDP, nil
}

func monitorInterruption(ctx context.Context, it model.Interrupter) {
	ticker := time.NewTicker(10 * time.Millisecond) // Poll for interruption at 100Hz
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			interrupted, err := it.RecvInterrupted()
			if err != nil {
				return
			}
			if interrupted {
				if err := it.HandleInterrupted(); err != nil {
					log.Printf("rtc: handle interrupted error: %v", err)
				}
			}
		}
	}
}

func readInbound(track *webrtc.TrackRemote, m model.Model) {
	dec, err := audio.NewDecoder()
	if err != nil {
		log.Printf("rtc: decoder: %v", err)
		return
	}
	readInboundLoop(track, dec, m)
}

func readInboundLoop(src rtpReader, dec frameDecoder, sink audioSink) {
	for {
		pkt, _, err := src.ReadRTP()
		if err != nil {
			return
		}
		if len(pkt.Payload) == 0 {
			continue
		}
		pcm, err := dec.Decode(pkt.Payload)
		if err != nil {
			continue
		}
		if err := sink.SendAudio(pcm); err != nil {
			return
		}
	}
}

func writeOutbound(ctx context.Context, out *webrtc.TrackLocalStaticSample, m model.Model, reportFault func(producedAudio bool, err error), sessionID identity.SessionID) {
	enc, err := audio.NewEncoder()
	if err != nil {
		log.Printf("rtc: encoder: %v", err)
		return
	}
	ticker := time.NewTicker(frameDur)
	defer ticker.Stop()
	writeOutboundLoop(ctx, out, m, enc, ticker.C, reportFault, time.Now, sessionID)
}

func writeOutboundLoop(
	ctx context.Context,
	out sampleWriter,
	recv pcmReceiver,
	enc frameEncoder,
	ticks <-chan time.Time,
	reportFault func(producedAudio bool, err error),
	now func() time.Time,
	sessionID identity.SessionID,
) {
	produced := false
	var buf []int16
	var last time.Time // wall clock of the previous frame emission
	exit := func(reason string, recvErr error) {
		metrics.RecordOutboundPumpExit(reason)
		if recvErr != nil {
			log.Printf("rtc: outbound pump exit session=%s reason=%s produced=%v err=%v",
				sessionID, reason, produced, recvErr)
		} else {
			log.Printf("rtc: outbound pump exit session=%s reason=%s produced=%v",
				sessionID, reason, produced)
		}
	}
	for {
		pcm, err := recv.Recv()
		if err != nil {
			if reportFault != nil {
				reportFault(produced, err)
			}
			exit("recv", err)
			return
		}
		buf = append(buf, pcm...)
		for len(buf) >= frameSamples {
			data, err := enc.Encode(buf[:frameSamples])
			buf = buf[frameSamples:]
			if err != nil {
				continue
			}
			if err := out.WriteSample(media.Sample{Data: data, Duration: frameDur}); err != nil {
				exit("write_sample", err)
				return
			}
			metrics.RecordOutboundFrameWritten()
			produced = true
			// Record the realized emission cadence (the §3.1 pacing SLO).
			at := now()
			if !last.IsZero() {
				metrics.ObserveFrameInterval(at.Sub(last))
			}
			last = at
			select {
			case <-ticks:
			case <-ctx.Done():
				exit("ctx", ctx.Err())
				return
			}
		}
	}
}

func forwardTranscripts(ctx context.Context, dc textSender, t model.Transcriber, sess *session) {
	for {
		tr, err := t.RecvTranscript()
		if err != nil {
			return
		}
		line := sess.rec.Record(tr.Role, tr.Text)
		for dc.ReadyState() != webrtc.DataChannelStateOpen {
			select {
			case <-ctx.Done():
				return
			case <-time.After(50 * time.Millisecond):
			}
		}
		if err := dc.SendText(dcproto.Encode(line)); err != nil {
			log.Printf("rtc: transcript send: %v", err)
			return
		}
	}
}

func sendReplay(dc textSender, replay []transcript.Line) {
	for _, line := range replay {
		if err := dc.SendText(dcproto.Encode(line)); err != nil {
			log.Printf("rtc: transcript replay send: %v", err)
			return
		}
	}
}

// forwardToolCalls relays model tool calls to the browser over the data channel,
// where the application (e.g. the DJ) executes the function and replies. Exits
// when the model closes (RecvToolCall errors) or the channel goes away.
func forwardToolCalls(ctx context.Context, dc textSender, td model.ToolDispatcher) {
	for {
		call, err := td.RecvToolCall()
		if err != nil {
			return
		}
		if dc.ReadyState() != webrtc.DataChannelStateOpen {
			return
		}
		if err := dc.SendText(dcproto.EncodeToolCall(call)); err != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

// restoredTurns maps recorded transcript lines to the provider-agnostic
// RestoredTurn turns a model.ContextRestorer accepts on reconnect.
func restoredTurns(lines []transcript.Line) []model.RestoredTurn {
	turns := make([]model.RestoredTurn, 0, len(lines))
	for _, l := range lines {
		turns = append(turns, model.RestoredTurn{Role: l.Role, Text: l.Text})
	}
	return turns
}
