// Package asr provides concrete cascade.ASR stage implementations.
package asr

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/coder/websocket"
	"github.com/thinkinbig/rt-llm-proxy/internal/audio"
	"github.com/thinkinbig/rt-llm-proxy/internal/model/cascade"
)

// whisperRate is the sample rate the ASR server expects on the wire.
const whisperRate = 16000

// Whisper is a streaming ASR stage backed by the RealtimeSTT sidecar
// (realtimestt/server.py — Silero/WebRTC VAD + faster-whisper).
//
// Wire protocol (WebSocket streaming):
//   - Client sends raw 16kHz mono s16le PCM frames as binary messages.
//   - Server sends JSON messages: {"type":"...","text":"..."}
//     type = "partial" | "final" | "speech_start" | "speech_end"
//
// Incoming audio from the cascade is 48kHz; we resample to 16kHz before
// sending.
type Whisper struct {
	conn   *websocket.Conn
	events chan cascade.ASREvent
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// whisperMsg is the JSON message shape from faster-whisper-server.
type whisperMsg struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// NewWhisper dials the faster-whisper-server WebSocket endpoint and starts
// the reader goroutine. url should be the full ws:// address of the streaming
// endpoint, e.g. "ws://localhost:9000/v1/audio/transcriptions/streaming".
func NewWhisper(url string) (*Whisper, error) {
	ctx, cancel := context.WithCancel(context.Background())
	conn, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		HTTPHeader: http.Header{"Content-Type": []string{"audio/pcm"}},
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("whisper dial %s: %w", url, err)
	}

	a := &Whisper{
		conn:   conn,
		events: make(chan cascade.ASREvent, 16),
		cancel: cancel,
	}
	a.wg.Add(1)
	go a.readLoop(ctx)
	return a, nil
}

// Write accepts 48kHz mono s16 PCM (the cascade contract), resamples to 16kHz,
// and sends the bytes to faster-whisper-server as a binary WebSocket message.
func (a *Whisper) Write(pcm []int16) error {
	down := audio.ResampleLinear(pcm, audio.OpusRate, whisperRate)
	buf := make([]byte, len(down)*2)
	for i, s := range down {
		binary.LittleEndian.PutUint16(buf[i*2:], uint16(s))
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	return a.conn.Write(ctx, websocket.MessageBinary, buf)
}

func (a *Whisper) Events() <-chan cascade.ASREvent { return a.events }

func (a *Whisper) Close() error {
	a.cancel()
	err := a.conn.Close(websocket.StatusNormalClosure, "")
	a.wg.Wait()
	return err
}

// readLoop reads JSON messages from the server and converts them to ASREvents.
func (a *Whisper) readLoop(ctx context.Context) {
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
		var ev cascade.ASREvent
		switch msg.Type {
		case "speech_start":
			ev = cascade.ASREvent{Kind: cascade.ASRSpeechStarted}
		case "partial":
			ev = cascade.ASREvent{Kind: cascade.ASRPartial, Text: msg.Text}
		case "final":
			ev = cascade.ASREvent{Kind: cascade.ASRFinal, Text: msg.Text}
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

var _ cascade.ASR = (*Whisper)(nil)
