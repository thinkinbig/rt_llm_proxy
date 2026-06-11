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

// VADConfig controls voice activity detection settings.
type VADConfig struct {
	Enabled                  bool
	StartOfSpeechSensitivity float64
	EndOfSpeechSensitivity   float64
}

// FunctionDeclaration declares one callable tool to the model. Parameters is a
// JSON Schema object describing the arguments. These come from the proxy config
// file — the proxy stays business-neutral and only forwards calls/results.
type FunctionDeclaration struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	Parameters  map[string]any `json:"parameters,omitempty"`
}

// Config holds session behavior for a Gemini Live connection. The API key and
// model name still come from the environment (GEMINI_API_KEY / GEMINI_MODEL);
// Config carries only the per-deployment behavior set from the proxy config file.
type Config struct {
	// SystemPrompt, when non-empty, is sent as the Live setup systemInstruction
	// so the model adopts a persona without consuming a dialogue turn.
	SystemPrompt string
	VAD          VADConfig
	// Tools, when non-empty, are declared to the model as functionDeclarations.
	Tools []FunctionDeclaration
}

// --- wire format (BidiGenerateContent JSON over WS) ---
// Field names match the google-genai SDK / v1beta proto JSON. Audio goes in
// realtimeInput.audio: the legacy realtimeInput.mediaChunks form is rejected by
// Live 3.1 ("media_chunks is deprecated. Use audio, video, or text instead").

type geminiPart struct {
	Text string `json:"text"`
}

type geminiContent struct {
	Parts []geminiPart `json:"parts"`
}

// geminiTool is one entry of the setup "tools" array. The Live API groups
// function declarations under a single tool object.
type geminiTool struct {
	FunctionDeclarations []FunctionDeclaration `json:"functionDeclarations,omitempty"`
}

type geminiSetup struct {
	Setup struct {
		Model            string `json:"model"`
		GenerationConfig struct {
			ResponseModalities []string `json:"responseModalities"`
		} `json:"generationConfig"`
		RealtimeInputConfig struct {
			AutomaticActivityDetection *struct {
				Disabled bool `json:"disabled,omitempty"`
			} `json:"automaticActivityDetection,omitempty"`
		} `json:"realtimeInputConfig,omitempty"`
		InputAudioTranscription  struct{}       `json:"inputAudioTranscription"`
		OutputAudioTranscription struct{}       `json:"outputAudioTranscription"`
		SystemInstruction        *geminiContent `json:"systemInstruction,omitempty"`
		Tools                    []geminiTool   `json:"tools,omitempty"`
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
		Audio *geminiBlob `json:"audio,omitempty"`
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

type geminiToolCall struct {
	FunctionCalls []struct {
		ID   string          `json:"id"`
		Name string          `json:"name"`
		Args json.RawMessage `json:"args"`
	} `json:"functionCalls"`
}

type geminiServerMsg struct {
	SetupComplete       *struct{}            `json:"setupComplete"`
	InputTranscription  *geminiTranscription `json:"inputTranscription"`
	OutputTranscription *geminiTranscription `json:"outputTranscription"`
	ToolCall            *geminiToolCall      `json:"toolCall"`
	ServerContent       *struct {
		TurnComplete        bool                 `json:"turnComplete"`
		Interrupted         bool                 `json:"interrupted"`
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
	vadCfg VADConfig

	// Transcription arrives as deltas; we accumulate per role so the data
	// channel carries the full sentence so far (the browser replaces the bubble
	// body on each line). Reset at turn boundaries. Touched only by readLoop.
	userBuf       strings.Builder
	modelBuf      strings.Builder
	interruptedCh chan struct{}       // Signals user speech interruption from server
	toolCallCh    chan model.ToolCall // server tool calls; nil when no tools declared
}

var (
	_ model.Model           = (*Gemini)(nil)
	_ model.Transcriber     = (*Gemini)(nil)
	_ model.ContextRestorer = (*Gemini)(nil)
	_ model.ToolDispatcher  = (*Gemini)(nil)
)

// EnvVAD returns the default VAD settings: enabled unless VAD_ENABLED=false,
// with 0.5 sensitivities. The composition root uses it so VAD stays
// env-controlled while the rest of Config comes from the proxy config file.
func EnvVAD() VADConfig {
	return VADConfig{
		Enabled:                  os.Getenv("VAD_ENABLED") != "false",
		StartOfSpeechSensitivity: 0.5,
		EndOfSpeechSensitivity:   0.5,
	}
}

func New(ctx context.Context) (*Gemini, error) {
	return NewWithConfig(ctx, Config{VAD: EnvVAD()})
}

// NewWithVAD dials Gemini with only VAD settings. Retained for callers that do
// not need the wider Config; delegates to NewWithConfig.
func NewWithVAD(ctx context.Context, vadCfg VADConfig) (*Gemini, error) {
	return NewWithConfig(ctx, Config{VAD: vadCfg})
}

// buildSetup assembles the Live setup message. SystemPrompt becomes
// systemInstruction (omitted when empty); manual VAD is requested only when
// disabled, otherwise the field is left off so the server keeps auto VAD.
func buildSetup(modelName string, cfg Config) geminiSetup {
	var setup geminiSetup
	setup.Setup.Model = modelName
	setup.Setup.GenerationConfig.ResponseModalities = []string{"AUDIO"}
	setup.Setup.InputAudioTranscription = struct{}{}
	setup.Setup.OutputAudioTranscription = struct{}{}
	if cfg.SystemPrompt != "" {
		setup.Setup.SystemInstruction = &geminiContent{Parts: []geminiPart{{Text: cfg.SystemPrompt}}}
	}
	if len(cfg.Tools) > 0 {
		setup.Setup.Tools = []geminiTool{{FunctionDeclarations: cfg.Tools}}
	}
	if !cfg.VAD.Enabled {
		setup.Setup.RealtimeInputConfig.AutomaticActivityDetection = &struct {
			Disabled bool `json:"disabled,omitempty"`
		}{Disabled: true}
	}
	return setup
}

func NewWithConfig(ctx context.Context, cfg Config) (*Gemini, error) {
	vadCfg := cfg.VAD
	key := os.Getenv("GEMINI_API_KEY")
	if key == "" {
		key = os.Getenv("GOOGLE_API_KEY")
	}
	if key == "" {
		return nil, fmt.Errorf("gemini: set GEMINI_API_KEY or GOOGLE_API_KEY")
	}
	modelName := os.Getenv("GEMINI_MODEL")
	if modelName == "" {
		modelName = "models/gemini-3.1-flash-live-preview"
	}

	cctx, cancel := context.WithCancel(ctx)
	conn, _, err := websocket.Dial(cctx, geminiWSURL+"?key="+key, nil)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("gemini: dial: %w", err)
	}
	conn.SetReadLimit(16 << 20)

	g := &Gemini{
		ctx: cctx, cancel: cancel, conn: conn, vadCfg: vadCfg,
		recvCh: make(chan []int16, 64), textCh: make(chan model.Transcript, 64),
		interruptedCh: make(chan struct{}, 1),
		toolCallCh:    make(chan model.ToolCall, 8),
	}

	if err := g.writeJSON(buildSetup(modelName, cfg)); err != nil {
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
	msg.RealtimeInput.Audio = &geminiBlob{
		MimeType: "audio/pcm;rate=" + strconv.Itoa(geminiInRate),
		Data:     base64.StdEncoding.EncodeToString(pcm.ToBytes(in)),
	}
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

// RestoreContext implements model.ContextRestorer. On reconnect it replays the
// prior conversation into the Live session as clientContent turns with
// turnComplete=false, so the model regains dialogue context without being
// prompted to generate a reply — the resumed live audio drives the next turn.
func (g *Gemini) RestoreContext(turns []model.RestoredTurn) error {
	if len(turns) == 0 {
		return nil
	}
	return g.writeJSON(restoreClientContent(turns))
}

// restoreClientContent builds the clientContent message that replays prior turns
// into the Live session. TurnComplete is false so the seeded history does not
// trigger a model reply; roles other than "model" map to "user".
func restoreClientContent(turns []model.RestoredTurn) geminiClientContent {
	var msg geminiClientContent
	msg.ClientContent.TurnComplete = false
	for _, t := range turns {
		role := "user"
		if t.Role == "model" {
			role = "model"
		}
		msg.ClientContent.Turns = append(msg.ClientContent.Turns, struct {
			Role  string `json:"role"`
			Parts []struct {
				Text string `json:"text"`
			} `json:"parts"`
		}{
			Role: role,
			Parts: []struct {
				Text string `json:"text"`
			}{{Text: t.Text}},
		})
	}
	return msg
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

func (g *Gemini) RecvInterrupted() (bool, error) {
	select {
	case <-g.ctx.Done():
		return false, io.EOF
	case <-g.interruptedCh:
		return true, nil
	default:
		return false, nil
	}
}

func (g *Gemini) SupportsInterruption() bool {
	return g.vadCfg.Enabled
}

// handleToolCall fans the server's function calls into toolCallCh for the bridge.
func (g *Gemini) handleToolCall(tc *geminiToolCall) {
	for _, fc := range tc.FunctionCalls {
		call := model.ToolCall{ID: fc.ID, Name: fc.Name, Args: fc.Args}
		select {
		case g.toolCallCh <- call:
		case <-g.ctx.Done():
			return
		}
	}
}

// RecvToolCall implements model.ToolDispatcher: blocks for the next tool call.
func (g *Gemini) RecvToolCall() (model.ToolCall, error) {
	select {
	case <-g.ctx.Done():
		return model.ToolCall{}, io.EOF
	case call, ok := <-g.toolCallCh:
		if !ok {
			return model.ToolCall{}, io.EOF
		}
		return call, nil
	}
}

// geminiToolResponse is the client toolResponse message returning function results.
type geminiToolResponse struct {
	ToolResponse struct {
		FunctionResponses []geminiFunctionResponse `json:"functionResponses"`
	} `json:"toolResponse"`
}

type geminiFunctionResponse struct {
	ID       string          `json:"id"`
	Name     string          `json:"name"`
	Response json.RawMessage `json:"response"`
}

// SendToolResult implements model.ToolDispatcher: returns a function result to
// the model so it can continue the turn. A nil Response is sent as {} so the
// wire stays a valid JSON object.
func (g *Gemini) SendToolResult(res model.ToolResult) error {
	resp := res.Response
	if len(resp) == 0 {
		resp = json.RawMessage(`{}`)
	}
	var msg geminiToolResponse
	msg.ToolResponse.FunctionResponses = []geminiFunctionResponse{{
		ID: res.ID, Name: res.Name, Response: resp,
	}}
	return g.writeJSON(msg)
}

func (g *Gemini) HandleInterrupted() error {
	// Gemini sends an interruption signal, but we may still have already-buffered
	// audio chunks queued locally. Drain them so barge-in feels immediate.
	g.drainAudioQueue()
	return nil
}

func (g *Gemini) drainAudioQueue() {
	for {
		select {
		case <-g.recvCh:
		default:
			return
		}
	}
}

func (g *Gemini) signalInterrupted() {
	g.drainAudioQueue()
	select {
	case g.interruptedCh <- struct{}{}:
	default:
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
	defer close(g.interruptedCh)
	defer close(g.toolCallCh)
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
		if msg.ToolCall != nil {
			g.handleToolCall(msg.ToolCall)
		}
		if msg.ServerContent == nil {
			continue
		}
		g.handleTranscription("user", msg.ServerContent.InputTranscription)
		g.handleTranscription("model", msg.ServerContent.OutputTranscription)
		if msg.ServerContent.Interrupted {
			g.signalInterrupted()
		}
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
