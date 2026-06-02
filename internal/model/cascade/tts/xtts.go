// Package tts provides concrete cascade.TTS stage implementations.
package tts

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/thinkinbig/rt-llm-proxy/internal/audio"
	"github.com/thinkinbig/rt-llm-proxy/internal/model/cascade"
)

// xttsRate is the sample rate XTTS streams on the wire (model native rate).
const xttsRate = 24000

// stream_chunk_size knobs (XTTS GPT decoding granularity). The quick answer —
// the first segment of a turn — uses a small chunk for the lowest
// time-to-first-audio; later segments use a larger chunk for steadier
// throughput. Mirrors RealtimeVoiceChat's QUICK/FINAL_ANSWER_STREAM_CHUNK_SIZE.
const (
	xttsQuickChunkSize = "8"
	xttsFinalChunkSize = "30"
)

// prewarmOnce ensures the XTTS server is warmed at most once per process (it is
// shared across sessions), not once per session, so it never adds setup latency.
var prewarmOnce sync.Once

// XTTSStream is a TTS stage backed by coqui-ai/xtts-streaming-server.
//
// Unlike Coqui (which fetches a complete WAV then chunks it), this stage
// uses the /tts_stream endpoint: XTTS emits int16 PCM incrementally as it
// synthesises, so the first audio reaches the caller within ~100ms instead of
// after the whole utterance is rendered. This is what makes the cascade's
// quick-answer / per-sentence pipeline pay off.
//
// Wire protocol (xtts-streaming-server):
//   - GET  /studio_speakers      -> {name: {speaker_embedding, gpt_cond_latent}}
//   - POST /tts_stream {speaker_embedding, gpt_cond_latent, text, language,
//          add_wav_header:false, stream_chunk_size} -> streaming raw int16 LE
//          PCM at 24kHz (no WAV header when add_wav_header=false).
//
// Speaker conditioning is fetched once at construction and reused for every
// request (the server is stateless per call).
type XTTSStream struct {
	baseURL    string
	language   string
	embedding  []float64
	latent     [][]float64
	httpClient *http.Client
}

// studioSpeaker is one entry of the /studio_speakers response.
type studioSpeaker struct {
	SpeakerEmbedding []float64   `json:"speaker_embedding"`
	GPTCondLatent    [][]float64 `json:"gpt_cond_latent"`
}

// xttsStreamRequest is the /tts_stream body. stream_chunk_size is a string on
// the wire (matching the server's pydantic model); smaller = faster first
// chunk, lower quality.
type xttsStreamRequest struct {
	SpeakerEmbedding []float64   `json:"speaker_embedding"`
	GPTCondLatent    [][]float64 `json:"gpt_cond_latent"`
	Text             string      `json:"text"`
	Language         string      `json:"language"`
	AddWavHeader     bool        `json:"add_wav_header"`
	StreamChunkSize  string      `json:"stream_chunk_size"`
}

// NewXTTSStream dials the xtts-streaming-server at baseURL (e.g.
// "http://localhost:8020"), fetches the studio speakers and caches the
// conditioning for `speaker`. If speaker is "", the first speaker (sorted by
// name for determinism) is used. language is the XTTS language code (e.g.
// "en", "zh-cn").
func NewXTTSStream(baseURL, speaker, language string) (*XTTSStream, error) {
	baseURL = strings.TrimRight(baseURL, "/")
	if language == "" {
		language = "en"
	}
	client := &http.Client{}

	resp, err := client.Get(baseURL + "/studio_speakers")
	if err != nil {
		return nil, fmt.Errorf("xtts studio_speakers: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("xtts studio_speakers status %d", resp.StatusCode)
	}
	var speakers map[string]studioSpeaker
	if err := json.NewDecoder(resp.Body).Decode(&speakers); err != nil {
		return nil, fmt.Errorf("xtts studio_speakers decode: %w", err)
	}
	if len(speakers) == 0 {
		return nil, fmt.Errorf("xtts: server reports no studio speakers")
	}

	if speaker == "" {
		names := make([]string, 0, len(speakers))
		for name := range speakers {
			names = append(names, name)
		}
		sort.Strings(names)
		speaker = names[0]
	}
	sp, ok := speakers[speaker]
	if !ok {
		return nil, fmt.Errorf("xtts: speaker %q not found", speaker)
	}

	x := &XTTSStream{
		baseURL:    baseURL,
		language:   language,
		embedding:  sp.SpeakerEmbedding,
		latent:     sp.GPTCondLatent,
		httpClient: client,
	}
	x.prewarm()
	return x, nil
}

// prewarm fires a single throwaway synthesis (once per process, in the
// background) so the XTTS server's first real utterance doesn't pay CUDA
// cold-start cost, and logs the measured time-to-first-audio. Best-effort:
// failures are logged and ignored. It never blocks session setup.
func (x *XTTSStream) prewarm() {
	prewarmOnce.Do(func() {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			start := time.Now()
			ch, err := x.SynthesizeQuick(ctx, "Warming up the text to speech engine.")
			if err != nil {
				log.Printf("xtts: prewarm failed: %v", err)
				return
			}
			var ttfa time.Duration
			gotFirst := false
			for range ch {
				if !gotFirst {
					ttfa = time.Since(start)
					gotFirst = true
				}
			}
			if gotFirst {
				log.Printf("xtts: prewarm complete, time-to-first-audio=%dms", ttfa.Milliseconds())
			}
		}()
	})
}

// Synthesize streams PCM (mono s16, 48kHz) for text as XTTS renders it, using
// the throughput-tuned chunk size (later segments of a turn).
func (x *XTTSStream) Synthesize(ctx context.Context, text string) (<-chan []int16, error) {
	return x.stream(ctx, text, xttsFinalChunkSize)
}

// SynthesizeQuick streams text using the small chunk size for the lowest
// time-to-first-audio (the quick answer — first segment of a turn).
func (x *XTTSStream) SynthesizeQuick(ctx context.Context, text string) (<-chan []int16, error) {
	return x.stream(ctx, text, xttsQuickChunkSize)
}

// stream issues the /tts_stream request and forwards PCM as XTTS renders it.
// The 24kHz wire stream is resampled to 48kHz and re-framed into ~20ms chunks
// (matching the bridge cadence) so audio flows to the caller before synthesis
// finishes. Cancelling ctx aborts the request (barge-in).
func (x *XTTSStream) stream(ctx context.Context, text, chunkSize string) (<-chan []int16, error) {
	body, err := json.Marshal(xttsStreamRequest{
		SpeakerEmbedding: x.embedding,
		GPTCondLatent:    x.latent,
		Text:             text,
		Language:         x.language,
		AddWavHeader:     false,
		StreamChunkSize:  chunkSize,
	})
	if err != nil {
		return nil, fmt.Errorf("xtts marshal: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, x.baseURL+"/tts_stream", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("xtts request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := x.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("xtts http: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("xtts status %d", resp.StatusCode)
	}

	ch := make(chan []int16, 4)
	go func() {
		defer close(ch)
		defer resp.Body.Close()

		readBuf := make([]byte, 8192)
		var oddByte []byte  // 0 or 1 byte carried across reads (split int16)
		var pending []int16 // resampled 48k samples awaiting 20ms framing

		send := func(frame []int16) bool {
			select {
			case ch <- frame:
				return true
			case <-ctx.Done():
				return false
			}
		}

		for {
			n, rerr := resp.Body.Read(readBuf)
			if n > 0 {
				raw := readBuf[:n]
				if len(oddByte) == 1 {
					raw = append(oddByte, raw...)
					oddByte = oddByte[:0]
				}
				if len(raw)%2 == 1 {
					oddByte = append(oddByte, raw[len(raw)-1])
					raw = raw[:len(raw)-1]
				}
				if len(raw) > 0 {
					in := make([]int16, len(raw)/2)
					for i := range in {
						in[i] = int16(binary.LittleEndian.Uint16(raw[i*2:]))
					}
					pending = append(pending, audio.ResampleLinear(in, xttsRate, audio.OpusRate)...)
					for len(pending) >= chunkSamples {
						if !send(pending[:chunkSamples]) {
							return
						}
						pending = pending[chunkSamples:]
					}
				}
			}
			if rerr != nil {
				if len(pending) > 0 {
					send(pending) // best-effort tail (<20ms)
				}
				return
			}
		}
	}()

	return ch, nil
}

func (x *XTTSStream) Close() error { return nil }

var (
	_ cascade.TTS              = (*XTTSStream)(nil)
	_ cascade.QuickSynthesizer = (*XTTSStream)(nil)
)
