package model

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/thinkinbig/rt-llm-proxy/internal/audio"
)

// Doubao "端到端实时语音大模型" over Volcengine's binary V3 WebSocket. Voice in,
// voice out, single model — same shape as Gemini Live, so it drops into the
// bridge unchanged. Upstream PCM is 16kHz mono; downstream TTS is 24kHz mono.
const (
	doubaoURL          = "wss://openspeech.bytedance.com/api/v3/realtime/dialogue"
	doubaoPublicAppKey = "PlgvMymc7f3tQnJ6" // fixed public key for this product, not a user secret
	doubaoInRate       = 16000
	doubaoOutRate      = 24000
)

type Doubao struct {
	ctx      context.Context
	cancel   context.CancelFunc
	conn     *websocket.Conn
	writeM   sync.Mutex
	recvCh   chan []int16
	sid      string
	lastSend atomic.Int64 // unix-nanos of last upstream audio, for keep-alive
}

func NewDoubao(ctx context.Context) (*Doubao, error) {
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

	d := &Doubao{ctx: cctx, cancel: cancel, conn: conn, recvCh: make(chan []int16, 64), sid: newUUID()}

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

func (d *Doubao) SendAudio(pcm []int16) error {
	in := audio.ResampleLinear(pcm, audio.OpusRate, doubaoInRate)
	d.lastSend.Store(time.Now().UnixNano())
	return d.writeFrame(dbMsgAudioClient, dbSerialRaw, dbEvTaskRequest, gzipBytes(pcmToBytes(in)))
}

// SendText is a no-op: this is a voice-to-voice model with no text input path.
func (d *Doubao) SendText(string) error { return nil }

func (d *Doubao) Recv() ([]int16, error) {
	select {
	case <-d.ctx.Done():
		return nil, io.EOF
	case pcm, ok := <-d.recvCh:
		if !ok {
			return nil, io.EOF
		}
		return pcm, nil
	}
}

func (d *Doubao) readLoop() {
	defer close(d.recvCh)
	first := true
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
		if first {
			log.Printf("doubao: first frame event=%d msgType=%d", f.event, f.msgType)
			first = false
		}
		payload := f.payload
		if f.compress == dbCompressGzip && len(payload) > 0 {
			if dec, err := gunzipBytes(payload); err == nil {
				payload = dec
			}
		}
		switch f.event {
		case dbEvTTSResponse:
			pcm := audio.ResampleLinear(bytesToPCM(payload), doubaoOutRate, audio.OpusRate)
			select {
			case d.recvCh <- pcm:
			case <-d.ctx.Done():
				return
			}
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
	return d.conn.Close(websocket.StatusNormalClosure, "")
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
