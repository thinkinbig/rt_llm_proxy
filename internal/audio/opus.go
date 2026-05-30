package audio

import "github.com/hraban/opus"

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

type Decoder struct{ d *opus.Decoder }

func NewDecoder() (*Decoder, error) {
	d, err := opus.NewDecoder(OpusRate, OpusChannels)
	if err != nil {
		return nil, err
	}
	return &Decoder{d}, nil
}

// Decode turns one Opus packet into mono s16 PCM at 48kHz.
func (d *Decoder) Decode(payload []byte) ([]int16, error) {
	pcm := make([]int16, maxFrameSamples)
	n, err := d.d.Decode(payload, pcm)
	if err != nil {
		return nil, err
	}
	return pcm[:n], nil
}

type Encoder struct{ e *opus.Encoder }

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
	return &Encoder{e}, nil
}

// Encode turns one frame of mono s16 PCM at 48kHz into an Opus packet.
// pcm must be a valid Opus frame size (e.g. 960 samples = 20ms).
func (e *Encoder) Encode(pcm []int16) ([]byte, error) {
	buf := make([]byte, maxPacketBytes)
	n, err := e.e.Encode(pcm, buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}
