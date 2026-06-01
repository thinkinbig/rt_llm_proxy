package gemini

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
	"unicode"
	"unicode/utf8"

	"github.com/coder/websocket"

	"github.com/thinkinbig/rt-llm-proxy/internal/audio"
	"github.com/thinkinbig/rt-llm-proxy/internal/model"
	"github.com/thinkinbig/rt-llm-proxy/internal/model/pcm"
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
		InputAudioTranscription  struct{} `json:"inputAudioTranscription"`
		OutputAudioTranscription struct{} `json:"outputAudioTranscription"`
	} `json:"setup"`
}

type geminiTranscription struct {
	Text     string `json:"text"`
	Finished bool   `json:"finished"`
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
	SetupComplete       *struct{}            `json:"setupComplete"`
	InputTranscription  *geminiTranscription `json:"inputTranscription"`
	OutputTranscription *geminiTranscription `json:"outputTranscription"`
	ServerContent       *struct {
		TurnComplete        bool                 `json:"turnComplete"`
		InputTranscription  *geminiTranscription `json:"inputTranscription"`
		OutputTranscription *geminiTranscription `json:"outputTranscription"`
		ModelTurn           *struct {
			Parts []struct {
				Text       string      `json:"text"`
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
	wg     sync.WaitGroup // readLoop; Close waits on it
	recvCh chan []int16
	textCh chan model.Transcript

	// Transcription arrives as deltas; we accumulate per role so the data
	// channel carries the full sentence so far (the browser replaces the bubble
	// body on each line). Reset at turn boundaries. Touched only by readLoop.
	userBuf  strings.Builder
	modelBuf strings.Builder
}

func New(ctx context.Context) (*Gemini, error) {
	key := os.Getenv("GEMINI_API_KEY")
	if key == "" {
		key = os.Getenv("GOOGLE_API_KEY")
	}
	if key == "" {
		return nil, fmt.Errorf("gemini: set GEMINI_API_KEY or GOOGLE_API_KEY")
	}
	modelName := os.Getenv("GEMINI_MODEL")
	if modelName == "" {
		// Must support bidiGenerateContent. Verify with:
		//   curl ".../v1beta/models?key=$KEY&pageSize=200" | jq '.models[]
		//     | select(.supportedGenerationMethods[]?=="bidiGenerateContent").name'
		modelName = "models/gemini-2.5-flash-native-audio-latest"
	}

	cctx, cancel := context.WithCancel(ctx)
	conn, _, err := websocket.Dial(cctx, geminiWSURL+"?key="+key, nil)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("gemini: dial: %w", err)
	}
	// Audio messages from the server are large; lift the default 32KB read cap.
	conn.SetReadLimit(16 << 20)

	g := &Gemini{
		ctx: cctx, cancel: cancel, conn: conn,
		recvCh: make(chan []int16, 64), textCh: make(chan model.Transcript, 64),
	}

	var setup geminiSetup
	setup.Setup.Model = modelName
	setup.Setup.GenerationConfig.ResponseModalities = []string{"AUDIO"}
	setup.Setup.InputAudioTranscription = struct{}{}
	setup.Setup.OutputAudioTranscription = struct{}{}
	if err := g.writeJSON(setup); err != nil {
		g.Close()
		return nil, fmt.Errorf("gemini: setup: %w", err)
	}

	g.wg.Add(1)
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

func (g *Gemini) SendAudio(pcmSamples []int16) error {
	in := audio.ResampleLinear(pcmSamples, audio.OpusRate, geminiInRate)
	var msg geminiRealtimeInput
	msg.RealtimeInput.MediaChunks = []geminiBlob{{
		MimeType: "audio/pcm;rate=" + strconv.Itoa(geminiInRate),
		Data:     base64.StdEncoding.EncodeToString(pcm.ToBytes(in)),
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
	case samples, ok := <-g.recvCh:
		if !ok {
			return nil, io.EOF
		}
		return samples, nil
	}
}

func (g *Gemini) RecvTranscript() (model.Transcript, error) {
	select {
	case <-g.ctx.Done():
		return model.Transcript{}, io.EOF
	case tr, ok := <-g.textCh:
		if !ok {
			return model.Transcript{}, io.EOF
		}
		return tr, nil
	}
}

func (g *Gemini) emitText(role, text string) {
	if text == "" {
		return
	}
	select {
	case g.textCh <- model.Transcript{Role: role, Text: text}:
	case <-g.ctx.Done():
	}
}

// inlineAudioToModelPCM turns one inline-data audio part into mono s16 at 48kHz
// (Model contract). Wire format is s16le; native rate comes from the part MIME.
func inlineAudioToModelPCM(raw []byte, mime string) []int16 {
	return audio.ResampleLinear(pcm.FromBytes(raw), rateFromMime(mime, geminiOutRate), audio.OpusRate)
}

func (g *Gemini) readLoop() {
	defer g.wg.Done()
	defer close(g.recvCh)
	defer close(g.textCh)
	first := true
	for {
		_, data, err := g.conn.Read(g.ctx)
		if err != nil {
			log.Printf("gemini: read loop ended: %v", err)
			return
		}
		if first {
			log.Printf("gemini: first server message: %s", truncate(data, 300))
			first = false
		}
		var msg geminiServerMsg
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}
		g.handleTranscription("user", msg.InputTranscription)
		g.handleTranscription("model", msg.OutputTranscription)
		if msg.ServerContent == nil {
			continue
		}
		g.handleTranscription("user", msg.ServerContent.InputTranscription)
		g.handleTranscription("model", msg.ServerContent.OutputTranscription)
		if msg.ServerContent.TurnComplete {
			g.userBuf.Reset()
			g.modelBuf.Reset()
		}
		if msg.ServerContent.ModelTurn == nil {
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
			samples := inlineAudioToModelPCM(raw, part.InlineData.MimeType)
			select {
			case g.recvCh <- samples:
			case <-g.ctx.Done():
				return
			}
		}
	}
}

func (g *Gemini) handleTranscription(role string, t *geminiTranscription) {
	if t == nil || t.Text == "" {
		return
	}
	buf := &g.userBuf
	text := t.Text
	if role == "model" {
		buf = &g.modelBuf
		text = normalizeModelTranscriptionDelta(buf.Len(), text)
	}
	buf.WriteString(text)
	g.emitText(role, buf.String())
	if t.Finished {
		buf.Reset()
	}
}

// normalizeModelTranscriptionDelta strips a leading space from output
// transcription deltas when Gemini tokenizes CJK one character at a time.
// English word deltas keep their space (e.g. "Hi" + " there").
func normalizeModelTranscriptionDelta(bufLen int, frag string) string {
	if bufLen == 0 || !strings.HasPrefix(frag, " ") {
		return frag
	}
	rest := frag[1:]
	if rest == "" {
		return frag
	}
	r, _ := utf8.DecodeRuneInString(rest)
	if isCJKTranscriptionRune(r) {
		return rest
	}
	return frag
}

func isCJKTranscriptionRune(r rune) bool {
	if unicode.Is(unicode.Han, r) {
		return true
	}
	switch {
	case r >= 0x3000 && r <= 0x303F: // CJK symbols and punctuation
	case r >= 0xFF00 && r <= 0xFFEF: // fullwidth forms
		return true
	}
	return false
}

func (g *Gemini) Close() error {
	g.cancel()
	err := g.conn.Close(websocket.StatusNormalClosure, "")
	g.wg.Wait() // readLoop has observed cancel/conn close and exited
	return err
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
