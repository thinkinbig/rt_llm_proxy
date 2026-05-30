package model

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/coder/websocket"

	"github.com/thinkinbig/rt-llm-proxy/internal/audio"
)

// Gemini Live wants 16kHz PCM in and emits 24kHz PCM out.
const (
	geminiInRate  = 16000
	geminiOutRate = 24000
	geminiWSURL   = "wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent"
)

// --- wire format (BidiGenerateContent JSON over WS) ---
// Field names match the google-genai SDK / v1beta proto JSON. The realtimeInput
// shape is version-sensitive: older servers want "mediaChunks", newer ones also
// accept "audio". We send mediaChunks for broad compatibility.

type geminiSetup struct {
	Setup struct {
		Model            string `json:"model"`
		GenerationConfig struct {
			ResponseModalities []string `json:"responseModalities"`
		} `json:"generationConfig"`
	} `json:"setup"`
}

type geminiBlob struct {
	MimeType string `json:"mimeType"`
	Data     string `json:"data"`
}

type geminiRealtimeInput struct {
	RealtimeInput struct {
		MediaChunks []geminiBlob `json:"mediaChunks"`
	} `json:"realtimeInput"`
}

type geminiClientContent struct {
	ClientContent struct {
		Turns []struct {
			Role  string `json:"role"`
			Parts []struct {
				Text string `json:"text"`
			} `json:"parts"`
		} `json:"turns"`
		TurnComplete bool `json:"turnComplete"`
	} `json:"clientContent"`
}

type geminiServerMsg struct {
	SetupComplete *struct{} `json:"setupComplete"`
	ServerContent *struct {
		ModelTurn *struct {
			Parts []struct {
				InlineData *geminiBlob `json:"inlineData"`
			} `json:"parts"`
		} `json:"modelTurn"`
	} `json:"serverContent"`
}

type Gemini struct {
	ctx    context.Context
	cancel context.CancelFunc
	conn   *websocket.Conn
	writeM sync.Mutex
	recvCh chan []int16
}

func NewGemini(ctx context.Context) (*Gemini, error) {
	key := os.Getenv("GEMINI_API_KEY")
	if key == "" {
		key = os.Getenv("GOOGLE_API_KEY")
	}
	if key == "" {
		return nil, fmt.Errorf("gemini: set GEMINI_API_KEY or GOOGLE_API_KEY")
	}
	model := os.Getenv("GEMINI_MODEL")
	if model == "" {
		// Must support bidiGenerateContent. Verify with:
		//   curl ".../v1beta/models?key=$KEY&pageSize=200" | jq '.models[]
		//     | select(.supportedGenerationMethods[]?=="bidiGenerateContent").name'
		model = "models/gemini-2.5-flash-native-audio-latest"
	}

	cctx, cancel := context.WithCancel(ctx)
	conn, _, err := websocket.Dial(cctx, geminiWSURL+"?key="+key, nil)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("gemini: dial: %w", err)
	}
	// Audio messages from the server are large; lift the default 32KB read cap.
	conn.SetReadLimit(16 << 20)

	g := &Gemini{ctx: cctx, cancel: cancel, conn: conn, recvCh: make(chan []int16, 64)}

	var setup geminiSetup
	setup.Setup.Model = model
	setup.Setup.GenerationConfig.ResponseModalities = []string{"AUDIO"}
	if err := g.writeJSON(setup); err != nil {
		g.Close()
		return nil, fmt.Errorf("gemini: setup: %w", err)
	}

	go g.readLoop()
	return g, nil
}

func (g *Gemini) writeJSON(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	g.writeM.Lock()
	defer g.writeM.Unlock()
	return g.conn.Write(g.ctx, websocket.MessageText, b)
}

func (g *Gemini) SendAudio(pcm []int16) error {
	in := audio.ResampleLinear(pcm, audio.OpusRate, geminiInRate)
	var msg geminiRealtimeInput
	msg.RealtimeInput.MediaChunks = []geminiBlob{{
		MimeType: "audio/pcm;rate=" + strconv.Itoa(geminiInRate),
		Data:     base64.StdEncoding.EncodeToString(pcmToBytes(in)),
	}}
	return g.writeJSON(msg)
}

func (g *Gemini) SendText(text string) error {
	var msg geminiClientContent
	msg.ClientContent.TurnComplete = true
	msg.ClientContent.Turns = make([]struct {
		Role  string `json:"role"`
		Parts []struct {
			Text string `json:"text"`
		} `json:"parts"`
	}, 1)
	msg.ClientContent.Turns[0].Role = "user"
	msg.ClientContent.Turns[0].Parts = []struct {
		Text string `json:"text"`
	}{{Text: text}}
	return g.writeJSON(msg)
}

func (g *Gemini) Recv() ([]int16, error) {
	select {
	case <-g.ctx.Done():
		return nil, io.EOF
	case pcm, ok := <-g.recvCh:
		if !ok {
			return nil, io.EOF
		}
		return pcm, nil
	}
}

func (g *Gemini) readLoop() {
	defer close(g.recvCh)
	first := true
	for {
		_, data, err := g.conn.Read(g.ctx)
		if err != nil {
			log.Printf("gemini: read loop ended: %v", err)
			return
		}
		if first {
			// Reveals setupComplete (good) or an error payload (bad model name /
			// wrong field shape) so verification problems are obvious.
			log.Printf("gemini: first server message: %s", truncate(data, 300))
			first = false
		}
		var msg geminiServerMsg
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}
		if msg.ServerContent == nil || msg.ServerContent.ModelTurn == nil {
			continue
		}
		for _, part := range msg.ServerContent.ModelTurn.Parts {
			if part.InlineData == nil || part.InlineData.Data == "" {
				continue
			}
			raw, err := base64.StdEncoding.DecodeString(part.InlineData.Data)
			if err != nil {
				continue
			}
			rate := rateFromMime(part.InlineData.MimeType, geminiOutRate)
			pcm := audio.ResampleLinear(bytesToPCM(raw), rate, audio.OpusRate)
			select {
			case g.recvCh <- pcm:
			case <-g.ctx.Done():
				return
			}
		}
	}
}

func (g *Gemini) Close() error {
	g.cancel()
	return g.conn.Close(websocket.StatusNormalClosure, "")
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "…"
}

// rateFromMime parses "audio/pcm;rate=24000" -> 24000, falling back to def.
func rateFromMime(mime string, def int) int {
	if i := strings.Index(mime, "rate="); i >= 0 {
		if r, err := strconv.Atoi(strings.TrimSpace(mime[i+5:])); err == nil {
			return r
		}
	}
	return def
}
