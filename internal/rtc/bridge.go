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
	"encoding/json"
	"log"
	"slices"
	"sync"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"

	"github.com/thinkinbig/rt-llm-proxy/internal/audio"
	"github.com/thinkinbig/rt-llm-proxy/internal/metrics"
	"github.com/thinkinbig/rt-llm-proxy/internal/model"
)

const (
	frameSamples = audio.OpusRate * 20 / 1000 // 960 samples = 20ms @ 48kHz
	frameDur     = 20 * time.Millisecond

	// Advertised on the answer so the browser encodes mic audio with FEC + DTX
	// and a narrowband-ish cap, matching the reference's SDP rewrite.
	opusFmtp = "minptime=10;useinbandfec=1;usedtx=1;maxaveragebitrate=16000"
)

// Hub holds the shared WebRTC API and the registry of active sessions, keyed by
// server-minted session id. It doubles as the SessionManager.
type Hub struct {
	api   *webrtc.API
	mu    sync.Mutex
	conns map[string]*session
	// archives keeps a bounded transcript history for disconnected sessions so a
	// reconnecting client can resume from session_id + last_seq on this node.
	archives map[string]sessionArchive
}

// session is one live bridge. id is minted server-side; userID is the resolved
// identity ("" = anonymous); both ride the side-channel events.
type session struct {
	id        string
	userID    string
	provider  string
	createdAt time.Time
	mu        sync.Mutex
	seq       uint64
	history   []TranscriptLine
	histLimit int
	cleanup   func()
}

type sessionArchive struct {
	provider string
	history  []TranscriptLine
	maxSeq   uint64
}

// TranscriptLine is one line exchanged on the data channel, tracked with a
// per-session sequence so reconnect can replay missing lines.
type TranscriptLine struct {
	Seq  uint64 `json:"seq"`
	Role string `json:"role"`
	Text string `json:"text"`
}

// NewHub builds the pion API with a custom MediaEngine (Opus + our fmtp) and
// the default interceptors.
func NewHub() (*Hub, error) {
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
	api := webrtc.NewAPI(webrtc.WithMediaEngine(me), webrtc.WithInterceptorRegistry(ir))
	return &Hub{
		api:      api,
		conns:    make(map[string]*session),
		archives: make(map[string]sessionArchive),
	}, nil
}

// CloseAll tears down every active session (used on graceful shutdown).
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
}

func (h *Hub) add(s *session) {
	h.mu.Lock()
	h.conns[s.id] = s
	h.mu.Unlock()
}

func (h *Hub) remove(s *session) {
	snapshot := s.snapshot(0)
	maxSeq := s.maxSeq()
	h.mu.Lock()
	delete(h.conns, s.id)
	if len(snapshot) > 0 {
		h.archives[s.id] = sessionArchive{
			provider: s.provider,
			history:  snapshot,
			maxSeq:   maxSeq,
		}
	}
	h.mu.Unlock()
}

// Count returns the number of currently active sessions.
func (h *Hub) Count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.conns)
}

// Resume returns transcript lines whose seq is greater than afterSeq for a
// known session. If the old session is still active, it is cleaned up first so
// the reconnect owns that session id.
func (h *Hub) Resume(sessionID, provider string, afterSeq uint64) (full, replay []TranscriptLine, startSeq uint64, ok bool) {
	if sessionID == "" {
		return nil, nil, 0, false
	}
	var takeover func()
	var active *session
	var archived sessionArchive
	h.mu.Lock()
	if s, exists := h.conns[sessionID]; exists {
		if s.provider != provider {
			h.mu.Unlock()
			return nil, nil, 0, false
		}
		active = s
		takeover = s.cleanup
		ok = true
	} else if arch, exists := h.archives[sessionID]; exists {
		if arch.provider != provider {
			h.mu.Unlock()
			return nil, nil, 0, false
		}
		archived = arch
		ok = true
	}
	h.mu.Unlock()
	if !ok {
		return nil, nil, 0, false
	}
	if active != nil {
		full = active.snapshot(0)
		startSeq = active.maxSeq()
	} else {
		full = slices.Clone(archived.history)
		startSeq = archived.maxSeq
	}
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

// SessionState returns provider/max seq for a known session id from active
// sessions or archives.
func (h *Hub) SessionState(sessionID string) (provider string, maxSeq uint64, ok bool) {
	var active *session
	h.mu.Lock()
	if s, exists := h.conns[sessionID]; exists {
		active = s
	} else if arch, exists := h.archives[sessionID]; exists {
		h.mu.Unlock()
		return arch.provider, arch.maxSeq, true
	}
	h.mu.Unlock()
	if active != nil {
		return active.provider, active.maxSeq(), true
	}
	return "", 0, false
}

// SessionInfo is the identity the handler resolved for a new session: the
// server-minted id, the user id ("" = anonymous), and the chosen provider. The
// handler mints the id before Serve so it can wrap the model for the
// side-channel; Serve only records it in the registry.
type SessionInfo struct {
	ID             string
	UserID         string
	Provider       string
	StartSeq       uint64
	InitialHistory []TranscriptLine
	Replay         []TranscriptLine
}

// Serve sets up the peer connection for one session and returns the answer SDP.
// Media bridging runs in background goroutines that tear down when the
// connection drops, the model session ends, or the hub is closed.
func (h *Hub) Serve(ctx context.Context, offerSDP string, m model.Model, info SessionInfo) (string, error) {
	pc, err := h.api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		return "", err
	}

	// Outbound track: model audio -> browser.
	out, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2},
		"audio", "rt-llm-proxy")
	if err != nil {
		pc.Close()
		return "", err
	}
	sender, err := pc.AddTrack(out)
	if err != nil {
		pc.Close()
		return "", err
	}

	sctx, scancel := context.WithCancel(ctx)
	sess := &session{
		id:        info.ID,
		userID:    info.UserID,
		provider:  info.Provider,
		createdAt: time.Now(),
		seq:       info.StartSeq,
		history:   slices.Clone(info.InitialHistory),
		histLimit: 256,
	}
	var cleanupOnce sync.Once
	sess.cleanup = func() {
		cleanupOnce.Do(func() {
			scancel()
			m.Close()
			pc.Close()
			h.remove(sess)
		})
	}
	h.add(sess)

	// Drain RTCP so the send buffer doesn't fill up.
	go func() {
		buf := make([]byte, 1500)
		for {
			if _, _, err := sender.Read(buf); err != nil {
				return
			}
		}
	}()

	// Inbound audio: browser mic -> model. Accept only the first audio track.
	var trackOnce sync.Once
	pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		if track.Kind() != webrtc.RTPCodecTypeAudio {
			return
		}
		trackOnce.Do(func() { go readInbound(track, m) })
	})

	// Data channel: browser text -> model; model transcripts -> browser.
	var replayOnce sync.Once
	pc.OnDataChannel(func(dc *webrtc.DataChannel) {
		dc.OnMessage(func(msg webrtc.DataChannelMessage) {
			if msg.IsString {
				sess.appendTranscript("user", string(msg.Data))
				_ = m.SendText(string(msg.Data))
			}
		})
		if t, ok := m.(transcriber); ok {
			start := func() {
				replayOnce.Do(func() { sendReplay(dc, info.Replay) })
				go forwardTranscripts(sctx, dc, t, sess)
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

	// Outbound pump: model audio -> browser.
	go writeOutbound(sctx, out, m)

	if err := pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer, SDP: offerSDP,
	}); err != nil {
		sess.cleanup()
		return "", err
	}
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		sess.cleanup()
		return "", err
	}
	gather := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(answer); err != nil {
		sess.cleanup()
		return "", err
	}
	<-gather // non-trickle: return the full SDP with candidates

	return pc.LocalDescription().SDP, nil
}

func readInbound(track *webrtc.TrackRemote, m model.Model) {
	dec, err := audio.NewDecoder()
	if err != nil {
		log.Printf("rtc: decoder: %v", err)
		return
	}
	for {
		pkt, _, err := track.ReadRTP()
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
		if err := m.SendAudio(pcm); err != nil {
			return
		}
	}
}

func writeOutbound(ctx context.Context, out *webrtc.TrackLocalStaticSample, m model.Model) {
	enc, err := audio.NewEncoder()
	if err != nil {
		log.Printf("rtc: encoder: %v", err)
		return
	}
	// A single Ticker fires on a fixed 20ms wall clock, so per-frame encode time is
	// absorbed instead of added on top — a per-frame time.After drifts slower
	// than real time, backing audio up and growing end-to-end latency. During
	// silence the extra ticks coalesce into the size-1 buffer (no burst on resume).
	ticker := time.NewTicker(frameDur)
	defer ticker.Stop()

	var buf []int16
	var last time.Time // wall clock of the previous frame emission
	for {
		pcm, err := m.Recv()
		if err != nil {
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
				return
			}
			// Record the realized emission cadence (the §3.1 pacing SLO).
			now := time.Now()
			if !last.IsZero() {
				metrics.ObserveFrameInterval(now.Sub(last))
			}
			last = now
			select {
			case <-ticker.C:
			case <-ctx.Done():
				return
			}
		}
	}
}

// transcriber is the subset of models that surface speech-to-text as
// model.Transcript values for the browser. Both Gemini and Doubao implement it.
type transcriber interface {
	RecvTranscript() (model.Transcript, error)
}

func forwardTranscripts(ctx context.Context, dc *webrtc.DataChannel, t transcriber, sess *session) {
	for {
		tr, err := t.RecvTranscript()
		if err != nil {
			return
		}
		ev := sess.appendTranscript(tr.Role, tr.Text)
		for dc.ReadyState() != webrtc.DataChannelStateOpen {
			select {
			case <-ctx.Done():
				return
			case <-time.After(50 * time.Millisecond):
			}
		}
		if err := dc.SendText(marshalTranscript(ev)); err != nil {
			log.Printf("rtc: transcript send: %v", err)
			return
		}
	}
}

func sendReplay(dc *webrtc.DataChannel, replay []TranscriptLine) {
	for _, line := range replay {
		if err := dc.SendText(marshalTranscript(line)); err != nil {
			log.Printf("rtc: transcript replay send: %v", err)
			return
		}
	}
}

func marshalTranscript(line TranscriptLine) string {
	b, _ := json.Marshal(line)
	return string(b)
}

func (s *session) appendTranscript(role, text string) TranscriptLine {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	line := TranscriptLine{Seq: s.seq, Role: role, Text: text}
	s.history = append(s.history, line)
	if extra := len(s.history) - s.histLimit; extra > 0 {
		s.history = append([]TranscriptLine(nil), s.history[extra:]...)
	}
	return line
}

func (s *session) snapshot(afterSeq uint64) []TranscriptLine {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]TranscriptLine, 0, len(s.history))
	for _, line := range s.history {
		if line.Seq > afterSeq {
			out = append(out, line)
		}
	}
	return out
}

func (s *session) maxSeq() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seq
}
