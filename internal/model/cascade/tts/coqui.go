package tts

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/thinkinbig/rt-llm-proxy/internal/audio"
	"github.com/thinkinbig/rt-llm-proxy/internal/model/cascade"
)

// chunkSamples is the PCM chunk size pushed into the channel per iteration
// (~20ms at 48kHz, matching the RTC bridge frame cadence).
const chunkSamples = audio.OpusRate * 20 / 1000

// Coqui is a TTS stage backed by a Coqui TTS server.
//
// Coqui TTS server does not stream; it returns a complete WAV file per request.
// We parse the WAV header to determine the native sample rate, resample to
// 48kHz (the cascade contract), then push chunks into the channel so the
// caller can start playing while we are still pushing — and so barge-in via
// ctx cancellation drops remaining chunks immediately.
//
// Endpoint: GET/POST <baseURL>/api/tts?text=<encoded>
// Response: audio/wav
type Coqui struct {
	baseURL    string
	httpClient *http.Client
}

// NewCoqui creates a Coqui TTS that calls the server at baseURL
// (e.g. "http://localhost:5002").
func NewCoqui(baseURL string) *Coqui {
	return &Coqui{
		baseURL:    strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{},
	}
}

// Synthesize requests synthesis of text, then streams the resulting PCM
// (mono s16, 48kHz) in ~20ms chunks. The channel closes when all chunks
// have been sent or ctx is cancelled.
func (c *Coqui) Synthesize(ctx context.Context, text string) (<-chan []int16, error) {
	endpoint := c.baseURL + "/api/tts?text=" + url.QueryEscape(text)
	// Retry once on transient failure before giving up.
	var wavData []byte
	for attempt := range 2 {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, fmt.Errorf("coqui request: %w", err)
		}
		resp, err := c.httpClient.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			wavData, err = io.ReadAll(resp.Body)
			resp.Body.Close()
			if err == nil {
				break
			}
		} else if resp != nil {
			resp.Body.Close()
		}
		if attempt == 1 {
			if err != nil {
				return nil, fmt.Errorf("coqui http (retry exhausted): %w", err)
			}
			return nil, fmt.Errorf("coqui status %d (retry exhausted)", resp.StatusCode)
		}
	}

	pcm, sampleRate, err := parseWAV(wavData)
	if err != nil {
		return nil, fmt.Errorf("coqui parse wav: %w", err)
	}

	// Resample from the server's native rate to 48kHz.
	if sampleRate != audio.OpusRate {
		pcm = audio.ResampleLinear(pcm, sampleRate, audio.OpusRate)
	}

	ch := make(chan []int16, 4)
	go func() {
		defer close(ch)
		for len(pcm) > 0 {
			n := min(chunkSamples, len(pcm))
			chunk := pcm[:n]
			pcm = pcm[n:]
			select {
			case ch <- chunk:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch, nil
}

func (c *Coqui) Close() error { return nil }

// parseWAV reads a minimal RIFF/WAV header and returns the PCM samples and
// sample rate. Only handles PCM format (format tag 1), mono or stereo (we
// downmix stereo to mono). Returns an error for unsupported formats.
func parseWAV(data []byte) ([]int16, int, error) {
	r := bytes.NewReader(data)

	// RIFF header: "RIFF" + fileSize(4) + "WAVE"
	var riff [4]byte
	if _, err := io.ReadFull(r, riff[:]); err != nil || string(riff[:]) != "RIFF" {
		return nil, 0, fmt.Errorf("not a RIFF file")
	}
	var fileSize uint32
	if err := binary.Read(r, binary.LittleEndian, &fileSize); err != nil {
		return nil, 0, err
	}
	var wave [4]byte
	if _, err := io.ReadFull(r, wave[:]); err != nil || string(wave[:]) != "WAVE" {
		return nil, 0, fmt.Errorf("not a WAVE file")
	}

	var (
		sampleRate uint32
		channels   uint16
	)

	// Walk chunks until we find fmt and data.
	for {
		var chunkID [4]byte
		if _, err := io.ReadFull(r, chunkID[:]); err != nil {
			return nil, 0, fmt.Errorf("unexpected end of WAV")
		}
		var chunkSize uint32
		if err := binary.Read(r, binary.LittleEndian, &chunkSize); err != nil {
			return nil, 0, err
		}

		switch string(chunkID[:]) {
		case "fmt ":
			var audioFmt uint16
			if err := binary.Read(r, binary.LittleEndian, &audioFmt); err != nil {
				return nil, 0, err
			}
			if audioFmt != 1 {
				return nil, 0, fmt.Errorf("unsupported WAV format %d (only PCM=1 supported)", audioFmt)
			}
			if err := binary.Read(r, binary.LittleEndian, &channels); err != nil {
				return nil, 0, err
			}
			if err := binary.Read(r, binary.LittleEndian, &sampleRate); err != nil {
				return nil, 0, err
			}
			// Skip byteRate(4) + blockAlign(2) + bitsPerSample(2) + any extra fmt bytes.
			skip := int(chunkSize) - 2 - 2 - 4
			if _, err := io.ReadFull(r, make([]byte, skip)); err != nil {
				return nil, 0, err
			}

		case "data":
			raw := make([]byte, chunkSize)
			if _, err := io.ReadFull(r, raw); err != nil {
				return nil, 0, err
			}
			samples := make([]int16, len(raw)/2)
			if err := binary.Read(bytes.NewReader(raw), binary.LittleEndian, &samples); err != nil {
				return nil, 0, err
			}
			// Downmix stereo to mono by averaging channel pairs.
			if channels == 2 {
				mono := make([]int16, len(samples)/2)
				for i := range mono {
					mono[i] = int16((int32(samples[i*2]) + int32(samples[i*2+1])) / 2)
				}
				samples = mono
			}
			return samples, int(sampleRate), nil

		default:
			// Unknown chunk — skip it.
			if _, err := io.ReadFull(r, make([]byte, chunkSize)); err != nil {
				return nil, 0, err
			}
		}
	}
}

var _ cascade.TTS = (*Coqui)(nil)
