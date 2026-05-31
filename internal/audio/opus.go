package audio

import (
	"sync/atomic"

	"github.com/hraban/opus"
)

// WebRTC always negotiates Opus at 48kHz. We work in mono throughout the proxy;
// a mono decoder still correctly down-mixes a stereo-configured stream.
const (
	OpusRate     = 48000
	OpusChannels = 1

	// 60ms is the largest Opus frame; size the decode buffer for it.
	maxFrameSamples = OpusRate * 60 / 1000
	// 1275 bytes is the max size of a single Opus packet.
	maxPacketBytes = 1275
)

type Decoder struct {
	d   *opus.Decoder
	pcm []int16 // reused across calls; see Decode
}

func NewDecoder() (*Decoder, error) {
	d, err := opus.NewDecoder(OpusRate, OpusChannels)
	if err != nil {
		return nil, err
	}
	return &Decoder{d: d, pcm: make([]int16, maxFrameSamples)}, nil
}

// Decode turns one Opus packet into mono s16 PCM at 48kHz. The returned slice
// aliases an internal buffer reused on every call (cf. bufio.Scanner.Bytes), so
// it is valid only until the next Decode on this Decoder — copy it to retain it.
// A Decoder is single-goroutine; this allocates zero per frame on the hot path.
func (d *Decoder) Decode(payload []byte) ([]int16, error) {
	n, err := d.d.Decode(payload, d.pcm)
	if err != nil {
		return nil, err
	}
	return d.pcm[:n], nil
}

type Encoder struct {
	e       *opus.Encoder
	buf     []byte // reused across calls; see Encode
	applied int32  // last complexity applied to e (-1 = none yet)
}

// encoderComplexity is the live Opus encoder complexity (0–10), -1 = libopus
// default. It is read atomically by every Encoder each frame, so an adaptive
// controller can lower it under load and raise it back — Encoders pick up the
// change within one frame (see Encode / applyComplexity).
var encoderComplexity atomic.Int32

func init() { encoderComplexity.Store(-1) }

// SetEncoderComplexity sets the live complexity for all Encoders. c outside
// 0–10 (e.g. -1) leaves the libopus default. Safe to call concurrently.
func SetEncoderComplexity(c int) { encoderComplexity.Store(int32(c)) }

// EncoderComplexity returns the current live complexity (-1 = libopus default).
func EncoderComplexity() int { return int(encoderComplexity.Load()) }

func NewEncoder() (*Encoder, error) {
	e, err := opus.NewEncoder(OpusRate, OpusChannels, opus.AppVoIP)
	if err != nil {
		return nil, err
	}
	// Opus tuning: in-band FEC + DTX for resilience on
	// lossy/quiet links. PacketLossPerc>0 is what actually activates FEC.
	_ = e.SetInBandFEC(true)
	_ = e.SetPacketLossPerc(10)
	_ = e.SetDTX(true)
	enc := &Encoder{e: e, buf: make([]byte, maxPacketBytes), applied: -1}
	enc.applyComplexity()
	return enc, nil
}

// applyComplexity re-applies the live complexity to this encoder if it changed.
// A plain atomic load per frame; the cgo SetComplexity only runs on a change.
func (e *Encoder) applyComplexity() {
	c := encoderComplexity.Load()
	if c != e.applied && c >= 0 {
		_ = e.e.SetComplexity(int(c))
		e.applied = c
	}
}

// Encode turns one frame of mono s16 PCM at 48kHz into an Opus packet. pcm must
// be a valid Opus frame size (e.g. 960 samples = 20ms). The returned slice
// aliases an internal buffer reused on every call (cf. bufio.Scanner.Bytes),
// valid only until the next Encode on this Encoder. pion's Opus payloader copies
// it synchronously inside WriteSample, so the bridge passes it straight through;
// any caller that retains it must copy. Single-goroutine; zero alloc per frame.
func (e *Encoder) Encode(pcm []int16) ([]byte, error) {
	e.applyComplexity()
	n, err := e.e.Encode(pcm, e.buf)
	if err != nil {
		return nil, err
	}
	return e.buf[:n], nil
}
