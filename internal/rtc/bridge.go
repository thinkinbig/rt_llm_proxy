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
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"

	"github.com/thinkinbig/rt-llm-proxy/internal/audio"
	"github.com/thinkinbig/rt-llm-proxy/internal/model"
)

const (
	frameSamples = audio.OpusRate * 20 / 1000 // 960 samples = 20ms @ 48kHz
	frameDur     = 20 * time.Millisecond

	// Advertised on the answer so the browser encodes mic audio with FEC + DTX
	// and a narrowband-ish cap, matching the reference's SDP rewrite.
	opusFmtp = "minptime=10;useinbandfec=1;usedtx=1;maxaveragebitrate=16000"
)

// Hub holds the shared WebRTC API and the set of active sessions.
type Hub struct {
	api   *webrtc.API
	mu    sync.Mutex
	conns map[*session]struct{}
}

type session struct{ cleanup func() }

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
	return &Hub{api: api, conns: make(map[*session]struct{})}, nil
}

// CloseAll tears down every active session (used on graceful shutdown).
func (h *Hub) CloseAll() {
	h.mu.Lock()
	snapshot := make([]*session, 0, len(h.conns))
	for s := range h.conns {
		snapshot = append(snapshot, s)
	}
	h.mu.Unlock()
	for _, s := range snapshot {
		s.cleanup()
	}
}

func (h *Hub) add(s *session) {
	h.mu.Lock()
	h.conns[s] = struct{}{}
	h.mu.Unlock()
}

func (h *Hub) remove(s *session) {
	h.mu.Lock()
	delete(h.conns, s)
	h.mu.Unlock()
}

// Serve sets up the peer connection for one session and returns the answer SDP.
// Media bridging runs in background goroutines that tear down when the
// connection drops, the model session ends, or the hub is closed.
func (h *Hub) Serve(ctx context.Context, offerSDP string, m model.Model) (string, error) {
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
	sess := &session{}
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
	pc.OnDataChannel(func(dc *webrtc.DataChannel) {
		dc.OnMessage(func(msg webrtc.DataChannelMessage) {
			if msg.IsString {
				_ = m.SendText(string(msg.Data))
			}
		})
		if t, ok := m.(transcriber); ok {
			start := func() { go forwardTranscripts(sctx, dc, t) }
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
	// Pace at real time so we don't blast the browser, mirroring proxy.py. A
	// single Ticker fires on a fixed 20ms wall clock, so per-frame encode time is
	// absorbed instead of added on top — a per-frame time.After drifts slower
	// than real time, backing audio up and growing end-to-end latency. During
	// silence the extra ticks coalesce into the size-1 buffer (no burst on resume).
	ticker := time.NewTicker(frameDur)
	defer ticker.Stop()

	var buf []int16
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
			select {
			case <-ticker.C:
			case <-ctx.Done():
				return
			}
		}
	}
}

// transcriber is the subset of models that surface speech-to-text as
// "role: text" lines for the browser. Both Gemini and Doubao implement it.
type transcriber interface {
	RecvText() (string, error)
}

func forwardTranscripts(ctx context.Context, dc *webrtc.DataChannel, t transcriber) {
	for {
		line, err := t.RecvText()
		if err != nil {
			return
		}
		for dc.ReadyState() != webrtc.DataChannelStateOpen {
			select {
			case <-ctx.Done():
				return
			case <-time.After(50 * time.Millisecond):
			}
		}
		if err := dc.SendText(line); err != nil {
			log.Printf("rtc: transcript send: %v", err)
			return
		}
	}
}
