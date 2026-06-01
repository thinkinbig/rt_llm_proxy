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
	"github.com/thinkinbig/rt-llm-proxy/internal/identity"
	"github.com/thinkinbig/rt-llm-proxy/internal/metrics"
	"github.com/thinkinbig/rt-llm-proxy/internal/model"
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

	// earlyFaultWindow is how soon after a session starts a provider stream
	// failure (with no audio produced yet) counts as an upstream fault worth
	// reporting to the circuit breaker — "connected but dead on arrival".
	earlyFaultWindow = 10 * time.Second
)

// Hub holds the shared WebRTC API and the registry of active sessions, keyed by
// server-minted session id. It doubles as the SessionManager.
type Hub struct {
	api   *webrtc.API
	mu    sync.Mutex
	conns map[identity.SessionID]*session
	// archives keeps a bounded transcript history for disconnected sessions so a
	// reconnecting client can resume from session_id + last_seq on this node.
	// Entries expire after archiveTTL and are swept lazily on insert.
	archives map[identity.SessionID]sessionArchive
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

type sessionArchive struct {
	provider string
	userID   identity.UserID
	history  []transcript.Line
	maxSeq   uint64
	expiry   time.Time
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
		conns:    make(map[identity.SessionID]*session),
		archives: make(map[identity.SessionID]sessionArchive),
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
	now := time.Now()
	h.mu.Lock()
	delete(h.conns, s.id)
	// Lazy reaper: drop expired archives whenever a session disconnects, so the
	// map can't grow without bound under session churn.
	for id, arch := range h.archives {
		if now.After(arch.expiry) {
			delete(h.archives, id)
		}
	}
	if len(snapshot) > 0 {
		h.archives[s.id] = sessionArchive{
			provider: s.provider,
			userID:   s.userID,
			history:  snapshot,
			maxSeq:   maxSeq,
			expiry:   now.Add(archiveTTL),
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
	var archived sessionArchive
	h.mu.Lock()
	if s, exists := h.conns[sessionID]; exists {
		if s.provider != provider || s.userID != userID {
			h.mu.Unlock()
			return nil, nil, 0, false
		}
		active = s
		takeover = s.cleanup
		ok = true
	} else if arch, exists := h.archives[sessionID]; exists {
		if arch.provider != provider || arch.userID != userID || time.Now().After(arch.expiry) {
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
		full = active.rec.FullHistory()
		startSeq = active.rec.MaxSeq()
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
	} else if arch, exists := h.archives[sessionID]; exists {
		if arch.userID != userID || time.Now().After(arch.expiry) {
			h.mu.Unlock()
			return "", 0, false
		}
		h.mu.Unlock()
		return arch.provider, arch.maxSeq, true
	}
	h.mu.Unlock()
	if active != nil {
		return active.provider, active.rec.MaxSeq(), true
	}
	return "", 0, false
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
	// OnModelFault, if set, is called when the provider stream fails before
	// producing any audio within earlyFaultWindow — a "connected but dead on
	// arrival" upstream fault the handler feeds into the circuit breaker. nil
	// disables the report (e.g. loopback).
	OnModelFault func(error)
}

// Serve sets up the peer connection for one session and returns the answer SDP.
// Media bridging runs in background goroutines that tear down when the
// connection drops, the model session ends, or the hub is closed.
// Serve takes ownership of m: on every error return it closes m (directly on the
// early setup failures, via sess.cleanup once the session exists), and on success
// the session owns and closes it. Callers must not close m after calling Serve.
func (h *Hub) Serve(ctx context.Context, offerSDP string, m model.Model, info SessionInfo) (string, error) {
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

	sctx, scancel := context.WithCancel(ctx)
	meta := transcript.SessionMeta{
		SessionID: info.ID,
		UserID:    info.UserID,
		Provider:  info.Provider,
	}
	sess := &session{
		id:        info.ID,
		userID:    info.UserID,
		provider:  info.Provider,
		createdAt: time.Now(),
		rec: transcript.NewRecorder(
			info.StartSeq,
			info.InitialHistory,
			256,
			meta,
			info.Transcript,
		),
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

	// Data channel: browser text -> model; model transcripts -> browser.
	var replayOnce sync.Once
	pc.OnDataChannel(func(dc *webrtc.DataChannel) {
		dc.OnMessage(func(msg webrtc.DataChannelMessage) {
			if msg.IsString {
				sess.rec.Record("user", string(msg.Data))
				_ = m.SendText(string(msg.Data))
			}
		})
		if t, ok := m.(model.Transcriber); ok {
			start := func() {
				replayOnce.Do(func() { sendReplay(dc, info.Replay) })
				h.wg.Go(func() { forwardTranscripts(sctx, dc, t, sess) })
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
	h.wg.Go(func() { writeOutbound(sctx, out, m, sess, info.OnModelFault) })

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

func writeOutbound(ctx context.Context, out *webrtc.TrackLocalStaticSample, m model.Model, sess *session, onFault func(error)) {
	enc, err := audio.NewEncoder()
	if err != nil {
		log.Printf("rtc: encoder: %v", err)
		return
	}
	produced := false
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
			// Connected-but-dead-on-arrival: a stream error before any audio was
			// produced, soon after start, is an upstream fault for the breaker.
			if !produced && onFault != nil && time.Since(sess.createdAt) < earlyFaultWindow {
				onFault(err)
			}
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
			produced = true
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

func forwardTranscripts(ctx context.Context, dc *webrtc.DataChannel, t model.Transcriber, sess *session) {
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
		if err := dc.SendText(marshalLine(line)); err != nil {
			log.Printf("rtc: transcript send: %v", err)
			return
		}
	}
}

func sendReplay(dc *webrtc.DataChannel, replay []transcript.Line) {
	for _, line := range replay {
		if err := dc.SendText(marshalLine(line)); err != nil {
			log.Printf("rtc: transcript replay send: %v", err)
			return
		}
	}
}

func marshalLine(line transcript.Line) string {
	b, _ := json.Marshal(line)
	return string(b)
}
