// Command loadgen drives N concurrent real WebRTC sessions against the proxy to
// measure its capacity. It is meant to run OFF-BOX (it competes for CPU with the
// proxy otherwise) and against ?model=loopback (no upstream cost). The pacing
// SLO itself is read from the proxy's own /stats (admin endpoint); loadgen only
// produces honest load and reports client-side liveness.
//
// Cost trick: the Opus frames are encoded ONCE and the same bytes are replayed
// across every session, so the generator pays Opus encode once, not per session
// — otherwise it saturates before the proxy does.
package main

import (
	"bytes"
	"context"
	"flag"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"

	"github.com/thinkinbig/rt-llm-proxy/internal/audio"
)

const frameSamples = audio.OpusRate * 20 / 1000 // 960 = 20ms @ 48kHz

type stats struct {
	connected   atomic.Int64 // gauge: live sessions
	failed      atomic.Uint64
	frames      atomic.Uint64
	transcripts atomic.Uint64
}

func main() {
	url := flag.String("url", "http://localhost:8080", "proxy base url")
	n := flag.Int("n", 100, "concurrent sessions")
	ramp := flag.Duration("ramp", 5*time.Second, "spread session starts over this window")
	dur := flag.Duration("duration", 30*time.Second, "hold sessions after ramp completes")
	modelName := flag.String("model", "loopback", "?model= backend (use loopback to isolate the proxy)")
	statsURL := flag.String("stats", "", "proxy admin /stats url to poll (optional)")
	flag.Parse()

	frames := preEncode()
	log.Printf("loadgen: pre-encoded %d Opus frames (%.0fms loop)", len(frames), float64(len(frames))*20)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// Sessions live for the ramp plus the hold duration, then everyone drains.
	runCtx, cancel := context.WithTimeout(ctx, *ramp+*dur)
	defer cancel()

	st := &stats{}
	go report(runCtx, st, *n, *statsURL)

	var wg sync.WaitGroup
	gap := *ramp / time.Duration(max(*n, 1))
	for i := 0; i < *n; i++ {
		select {
		case <-runCtx.Done():
		case <-time.After(gap):
		}
		wg.Go(func() {
			runSession(runCtx, *url, *modelName, frames, st)
		})
	}
	wg.Wait()

	log.Printf("loadgen done: peak_connected~%d failed=%d frames_sent=%d transcripts=%d",
		*n-int(st.failed.Load()), st.failed.Load(), st.frames.Load(), st.transcripts.Load())
}

// preEncode builds one second of 440Hz sine and Opus-encodes it into 20ms
// frames, once, for all sessions to replay.
func preEncode() [][]byte {
	enc, err := audio.NewEncoder()
	if err != nil {
		log.Fatalf("loadgen: encoder: %v", err)
	}
	tone := make([]int16, audio.OpusRate) // 1s
	for i := range tone {
		tone[i] = int16(8000 * math.Sin(2*math.Pi*440*float64(i)/float64(audio.OpusRate)))
	}
	var frames [][]byte
	for i := 0; i+frameSamples <= len(tone); i += frameSamples {
		f, err := enc.Encode(tone[i : i+frameSamples])
		if err != nil {
			continue
		}
		cp := make([]byte, len(f)) // Encode reuses its buffer; copy before retaining
		copy(cp, f)
		frames = append(frames, cp)
	}
	return frames
}

func runSession(ctx context.Context, base, model string, frames [][]byte, st *stats) {
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		st.failed.Add(1)
		return
	}
	defer pc.Close()

	track, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2},
		"audio", "loadgen")
	if err != nil {
		st.failed.Add(1)
		return
	}
	if _, err := pc.AddTrack(track); err != nil {
		st.failed.Add(1)
		return
	}

	// A data channel so the proxy forwards transcripts (exercises the
	// transcript + side-channel path); count what comes back.
	dc, err := pc.CreateDataChannel("data", nil)
	if err != nil {
		st.failed.Add(1)
		return
	}
	dc.OnMessage(func(webrtc.DataChannelMessage) { st.transcripts.Add(1) })

	// Drain inbound model audio so the receive buffer doesn't stall.
	pc.OnTrack(func(t *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		for {
			if _, _, err := t.ReadRTP(); err != nil {
				return
			}
		}
	})

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		st.failed.Add(1)
		return
	}
	gather := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(offer); err != nil {
		st.failed.Add(1)
		return
	}
	<-gather // non-trickle, mirroring the proxy

	answer, err := postSDP(ctx, base+"/?model="+model, pc.LocalDescription().SDP)
	if err != nil {
		st.failed.Add(1)
		return
	}
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: answer}); err != nil {
		st.failed.Add(1)
		return
	}

	st.connected.Add(1)
	defer st.connected.Add(-1)

	// Replay the pre-encoded frames at real time.
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := track.WriteSample(media.Sample{Data: frames[i%len(frames)], Duration: 20 * time.Millisecond}); err != nil {
				return
			}
			st.frames.Add(1)
		}
	}
}

func postSDP(ctx context.Context, url, sdp string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBufferString(sdp))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/sdp")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", &httpError{resp.StatusCode, string(body)}
	}
	return string(body), nil
}

type httpError struct {
	code int
	body string
}

func (e *httpError) Error() string { return e.body }

// report prints a client-side liveness line every 2s, plus the proxy's own
// /stats if a url was given (the pacing-p99 SLO lives there, not here).
func report(ctx context.Context, st *stats, target int, statsURL string) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			log.Printf("connected=%d/%d failed=%d frames=%d transcripts=%d%s",
				st.connected.Load(), target, st.failed.Load(),
				st.frames.Load(), st.transcripts.Load(), pollStats(statsURL))
		}
	}
}

func pollStats(url string) string {
	if url == "" {
		return ""
	}
	resp, err := http.Get(url)
	if err != nil {
		return " proxy_stats=err"
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return " proxy=" + string(bytes.TrimSpace(b))
}
