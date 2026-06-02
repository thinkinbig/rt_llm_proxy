package cascade

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/coder/websocket"
	"github.com/thinkinbig/rt-llm-proxy/internal/audio"
)

// whisperRate is the sample rate faster-whisper-server expects on the wire.
const whisperRate = 16000

// WhisperASR is a streaming ASR stage backed by faster-whisper-server.
//
// Wire protocol (faster-whisper-server v0.x WebSocket streaming):
//   - Client sends raw 16kHz mono s16le PCM frames as binary messages.
//   - Server sends JSON messages: {"type":"...","text":"..."}
//     type = "partial" | "final" | "speech_start" | "speech_end"
//
// Incoming audio from the cascade is 48kHz; we resample to 16kHz before
// sending.
type WhisperASR struct {
	conn   *websocket.Conn
	events chan ASREvent
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// whisperMsg is the JSON message shape from faster-whisper-server.
type whisperMsg struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// NewWhisperASR dials the faster-whisper-server WebSocket endpoint and starts
// the reader goroutine. url should be the full ws:// address of the streaming
// endpoint, e.g. "ws://localhost:9000/v1/audio/transcriptions/streaming".
func NewWhisperASR(url string) (*WhisperASR, error) {
	ctx, cancel := context.WithCancel(context.Background())
	conn, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		HTTPHeader: http.Header{"Content-Type": []string{"audio/pcm"}},
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("whisper dial %s: %w", url, err)
	}

	a := &WhisperASR{
		conn:   conn,
		events: make(chan ASREvent, 16),
		cancel: cancel,
	}
	a.wg.Add(1)
	go a.readLoop(ctx)
	return a, nil
}

// Write accepts 48kHz mono s16 PCM (the cascade contract), resamples to 16kHz,
// and sends the bytes to faster-whisper-server as a binary WebSocket message.
func (a *WhisperASR) Write(pcm []int16) error {
	down := audio.ResampleLinear(pcm, audio.OpusRate, whisperRate)
	buf := make([]byte, len(down)*2)
	for i, s := range down {
		binary.LittleEndian.PutUint16(buf[i*2:], uint16(s))
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	return a.conn.Write(ctx, websocket.MessageBinary, buf)
}

func (a *WhisperASR) Events() <-chan ASREvent { return a.events }

func (a *WhisperASR) Close() error {
	a.cancel()
	err := a.conn.Close(websocket.StatusNormalClosure, "")
	a.wg.Wait()
	return err
}

// readLoop reads JSON messages from the server and converts them to ASREvents.
func (a *WhisperASR) readLoop(ctx context.Context) {
	defer a.wg.Done()
	defer close(a.events)
	for {
		_, data, err := a.conn.Read(ctx)
		if err != nil {
			return
		}
		var msg whisperMsg
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}
		var ev ASREvent
		switch msg.Type {
		case "speech_start":
			ev = ASREvent{Kind: ASRSpeechStarted}
		case "partial":
			ev = ASREvent{Kind: ASRPartial, Text: msg.Text}
		case "final":
			ev = ASREvent{Kind: ASRFinal, Text: msg.Text}
		default:
			continue
		}
		select {
		case a.events <- ev:
		case <-ctx.Done():
			return
		}
	}
}
