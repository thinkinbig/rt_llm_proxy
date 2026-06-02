package doubao

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/thinkinbig/rt-llm-proxy/internal/audio"
	"github.com/thinkinbig/rt-llm-proxy/internal/model"
	"github.com/thinkinbig/rt-llm-proxy/internal/model/pcm"
)

// Doubao "端到端实时语音大模型" over Volcengine's binary V3 WebSocket. Voice in,
// voice out, single model — same shape as Gemini Live, so it drops into the
// bridge unchanged. Upstream PCM is 16kHz mono; downstream TTS is 24kHz mono.
const (
	doubaoURL          = "wss://openspeech.bytedance.com/api/v3/realtime/dialogue"
	doubaoPublicAppKey = "PlgvMymc7f3tQnJ6" // fixed public key for this product, not a user secret
	doubaoInRate       = 16000
	// Downstream TTS is 24kHz mono float32 PCM (f32le) — confirmed by dumping and
	// analyzing the raw stream. We resample it up to the 48kHz Opus rate.
	doubaoOutRate = 24000
)

// transcript is one speech-to-text update. Text is cumulative (not a delta).
type transcript struct {
	Role  string // "user" or "model"
	Text  string
	Final bool
}

type Doubao struct {
	ctx          context.Context
	cancel       context.CancelFunc
	conn         *websocket.Conn
	writeM       sync.Mutex
	wg           sync.WaitGroup // readLoop + keepAlive; Close waits on it
	recvCh       chan []int16
	transcriptCh chan transcript
	sid          string
	lastSend     atomic.Int64 // unix-nanos of last upstream audio, for keep-alive

	// Model reply text streams as deltas; accumulate so each transcript carries
	// the full sentence so far. Reset on ChatEnded. Touched only by readLoop.
	modelBuf strings.Builder
}

func New(ctx context.Context) (*Doubao, error) {
	appID := os.Getenv("DOUBAO_APP_ID")
	token := os.Getenv("DOUBAO_ACCESS_TOKEN")
	if appID == "" || token == "" {
		return nil, fmt.Errorf("doubao: set DOUBAO_APP_ID and DOUBAO_ACCESS_TOKEN")
	}
	botName := os.Getenv("DOUBAO_BOT_NAME")
	if botName == "" {
		botName = "豆包"
	}

	cctx, cancel := context.WithCancel(ctx)
	hdr := http.Header{}
	hdr.Set("X-Api-App-ID", appID)
	hdr.Set("X-Api-Access-Key", token)
	hdr.Set("X-Api-Resource-Id", "volc.speech.dialog")
	hdr.Set("X-Api-App-Key", doubaoPublicAppKey)
	hdr.Set("X-Api-Connect-Id", newUUID())

	conn, _, err := websocket.Dial(cctx, doubaoURL, &websocket.DialOptions{HTTPHeader: hdr})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("doubao: dial: %w", err)
	}
	conn.SetReadLimit(16 << 20)

	d := &Doubao{
		ctx: cctx, cancel: cancel, conn: conn,
		recvCh: make(chan []int16, 64), transcriptCh: make(chan transcript, 64), sid: newUUID(),
	}

	if err := d.writeFrame(dbMsgFullClient, dbSerialJSON, dbEvStartConnection, gzipBytes([]byte("{}"))); err != nil {
		d.Close()
		return nil, fmt.Errorf("doubao: start connection: %w", err)
	}
	start := map[string]any{
		"dialog": map[string]any{"bot_name": botName},
		"tts":    map[string]any{"audio_config": map[string]any{"channel": 1, "format": "pcm", "sample_rate": doubaoOutRate}},
	}
	sb, _ := json.Marshal(start)
	if err := d.writeFrame(dbMsgFullClient, dbSerialJSON, dbEvStartSession, gzipBytes(sb)); err != nil {
		d.Close()
		return nil, fmt.Errorf("doubao: start session: %w", err)
	}
	d.lastSend.Store(time.Now().UnixNano())

	d.wg.Add(2)
	go d.readLoop()
	go d.keepAlive()
	return d, nil
}

func (d *Doubao) writeFrame(msgType, serial byte, event int32, payload []byte) error {
	frame := dbBuildFrame(msgType, serial, dbCompressGzip, event, d.sid, payload)
	d.writeM.Lock()
	defer d.writeM.Unlock()
	return d.conn.Write(d.ctx, websocket.MessageBinary, frame)
}

func (d *Doubao) SendAudio(samples []int16) error {
	in := audio.ResampleLinear(samples, audio.OpusRate, doubaoInRate)
	d.lastSend.Store(time.Now().UnixNano())
	return d.writeFrame(dbMsgAudioClient, dbSerialRaw, dbEvTaskRequest, gzipBytes(pcm.ToBytes(in)))
}

// SendText is a no-op: this is a voice-to-voice model with no text input path.
func (d *Doubao) SendText(string) error { return nil }

func (d *Doubao) Recv() ([]int16, error) {
	select {
	case <-d.ctx.Done():
		return nil, io.EOF
	case samples, ok := <-d.recvCh:
		if !ok {
			return nil, io.EOF
		}
		return samples, nil
	}
}

func (d *Doubao) recvTranscript() (transcript, error) {
	select {
	case <-d.ctx.Done():
		return transcript{}, io.EOF
	case t, ok := <-d.transcriptCh:
		if !ok {
			return transcript{}, io.EOF
		}
		return t, nil
	}
}

// RecvTranscript returns the next transcript turn, skipping turns with empty
// text. Returns io.EOF on close.
func (d *Doubao) RecvTranscript() (model.Transcript, error) {
	for {
		t, err := d.recvTranscript()
		if err != nil {
			return model.Transcript{}, err
		}
		if t.Text == "" {
			continue
		}
		return model.Transcript{Role: t.Role, Text: t.Text}, nil
	}
}

func (d *Doubao) RecvInterrupted() (bool, error) {
	// TODO: Implement when Doubao's VAD/barge-in API details are confirmed
	return false, nil
}

func (d *Doubao) SupportsInterruption() bool {
	// TODO: Enable after verifying Doubao's VAD configuration
	return false
}

func (d *Doubao) HandleInterrupted() error {
	// TODO: Implement Doubao-specific interruption handling
	return nil
}

func (d *Doubao) emitTranscript(role, text string, final bool) {
	if text == "" && !final {
		return
	}
	select {
	case d.transcriptCh <- transcript{Role: role, Text: text, Final: final}:
	case <-d.ctx.Done():
	}
}

// ASRResponse (user) and ChatResponse (model) carry the STT/text. ASR chunks
// flip is_interim=false on the final result for an utterance; chat text streams
// in pieces and is closed by a separate ChatEnded event.
type doubaoASR struct {
	Results []struct {
		Text      string `json:"text"`
		IsInterim bool   `json:"is_interim"`
	} `json:"results"`
}

type doubaoChat struct {
	Content string `json:"content"`
}

func (d *Doubao) handleASR(payload []byte) {
	var msg doubaoASR
	if err := json.Unmarshal(payload, &msg); err != nil {
		return
	}
	for _, r := range msg.Results {
		d.emitTranscript("user", r.Text, !r.IsInterim)
	}
}

func (d *Doubao) handleChat(payload []byte) {
	var msg doubaoChat
	if err := json.Unmarshal(payload, &msg); err != nil {
		return
	}
	d.modelBuf.WriteString(msg.Content)
	d.emitTranscript("model", d.modelBuf.String(), false)
}

// ttsToModelPCM turns one TTS payload into mono s16 at 48kHz (Model contract).
// Downstream wire format is f32le at doubaoOutRate; the server sends no per-frame
// rate metadata, so the rate is a const shared with session start.
func ttsToModelPCM(payload []byte) []int16 {
	return audio.ResampleLinear(f32leToPCM(payload), doubaoOutRate, audio.OpusRate)
}

// f32leToPCM parses Doubao TTS float32 little-endian samples into s16.
func f32leToPCM(b []byte) []int16 {
	samples := make([]int16, len(b)/4)
	for i := range samples {
		f := math.Float32frombits(binary.LittleEndian.Uint32(b[i*4:]))
		v := float64(f) * 32767
		switch {
		case v > 32767:
			v = 32767
		case v < -32768:
			v = -32768
		}
		samples[i] = int16(v)
	}
	return samples
}

func (d *Doubao) readLoop() {
	defer d.wg.Done()
	defer close(d.recvCh)
	defer close(d.transcriptCh)
	for {
		_, raw, err := d.conn.Read(d.ctx)
		if err != nil {
			log.Printf("doubao: read loop ended: %v", err)
			return
		}
		f, err := dbParseFrame(raw)
		if err != nil {
			continue
		}
		payload := f.payload
		if f.compress == dbCompressGzip && len(payload) > 0 {
			if dec, err := gunzipBytes(payload); err == nil {
				payload = dec
			}
		}
		switch f.event {
		case dbEvTTSResponse:
			samples := ttsToModelPCM(payload)
			select {
			case d.recvCh <- samples:
			case <-d.ctx.Done():
				return
			}
		case dbEvASRResponse:
			d.handleASR(payload)
		case dbEvChatResponse:
			d.handleChat(payload)
		case dbEvChatEnded:
			d.emitTranscript("model", d.modelBuf.String(), true)
			d.modelBuf.Reset()
		case dbEvSessionFailed, dbEvConnectionFailed:
			log.Printf("doubao: failed (event=%d): %s", f.event, string(payload))
			return
		case dbEvSessionFinished:
			return
		}
		if f.msgType == dbMsgError {
			log.Printf("doubao: error frame code=%d: %s", f.errorCode, string(payload))
		}
	}
}

// keepAlive pushes 100ms of silence whenever the upstream has been idle for 5s,
// keeping the session open (required by the server).
func (d *Doubao) keepAlive() {
	defer d.wg.Done()
	silence := gzipBytes(make([]byte, doubaoInRate/10*2)) // 100ms @ 16kHz s16 mono
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-d.ctx.Done():
			return
		case <-t.C:
			if time.Since(time.Unix(0, d.lastSend.Load())) < 5*time.Second {
				continue
			}
			if err := d.writeFrame(dbMsgAudioClient, dbSerialRaw, dbEvTaskRequest, silence); err != nil {
				return
			}
			d.lastSend.Store(time.Now().UnixNano())
		}
	}
}

func (d *Doubao) Close() error {
	d.cancel()
	err := d.conn.Close(websocket.StatusNormalClosure, "")
	d.wg.Wait() // readLoop + keepAlive have observed cancel/conn close and exited
	return err
}

// --- helpers ---

func gzipBytes(b []byte) []byte {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	w.Write(b)
	w.Close()
	return buf.Bytes()
}

func gunzipBytes(b []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

func newUUID() string {
	var b [16]byte
	rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
