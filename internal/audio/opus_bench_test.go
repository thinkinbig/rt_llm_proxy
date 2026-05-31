package audio

import (
	"fmt"
	"math"
	"testing"
)

// sineFrame is one 20ms frame (960 samples) of 440Hz tone — a non-trivial
// signal so the encoder does real work (silence would be near-free under DTX).
func sineFrame() []int16 {
	f := make([]int16, OpusRate*20/1000)
	for i := range f {
		f[i] = int16(8000 * math.Sin(2*math.Pi*440*float64(i)/float64(OpusRate)))
	}
	return f
}

// BenchmarkEncode measures the cgo Opus encode cost of one outbound frame — the
// per-session, 50-times-per-second hot path that dominates proxy CPU. ns/op
// here, times 50 frames/s, gives CPU-fraction per session; its reciprocal is
// sessions-per-core for the encode direction.
func BenchmarkEncode(b *testing.B) {
	enc, err := NewEncoder()
	if err != nil {
		b.Fatal(err)
	}
	frame := sineFrame()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := enc.Encode(frame); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDecode measures the cgo Opus decode cost of one inbound frame.
func BenchmarkDecode(b *testing.B) {
	enc, err := NewEncoder()
	if err != nil {
		b.Fatal(err)
	}
	dec, err := NewDecoder()
	if err != nil {
		b.Fatal(err)
	}
	pkt, err := enc.Encode(sineFrame())
	if err != nil {
		b.Fatal(err)
	}
	p := make([]byte, len(pkt)) // Encode reuses its buffer; copy before retaining
	copy(p, pkt)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := dec.Decode(p); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRoundtrip is the full per-session per-frame audio CPU: decode inbound
// + encode outbound, which is what one bridged session costs 50 times a second.
func BenchmarkRoundtrip(b *testing.B) {
	enc, err := NewEncoder()
	if err != nil {
		b.Fatal(err)
	}
	dec, err := NewDecoder()
	if err != nil {
		b.Fatal(err)
	}
	frame := sineFrame()
	pkt, err := enc.Encode(frame)
	if err != nil {
		b.Fatal(err)
	}
	in := make([]byte, len(pkt))
	copy(in, pkt)
	b.ReportAllocs()
	for b.Loop() {
		pcm, err := dec.Decode(in)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := enc.Encode(pcm); err != nil {
			b.Fatal(err)
		}
	}
}

// richFrame is a 20ms frame with several tones plus a little deterministic
// noise, so the encoder does complexity-dependent work (a pure tone can
// short-circuit and hide the complexity/CPU tradeoff).
func richFrame() []int16 {
	f := make([]int16, OpusRate*20/1000)
	seed := uint32(1)
	for i := range f {
		t := float64(i) / float64(OpusRate)
		v := 4000*math.Sin(2*math.Pi*300*t) +
			2500*math.Sin(2*math.Pi*900*t) +
			1500*math.Sin(2*math.Pi*1800*t)
		seed = seed*1664525 + 1013904223 // LCG noise
		v += float64(int32(seed>>16)%800 - 400)
		f[i] = int16(v)
	}
	return f
}

// BenchmarkEncodeComplexity sweeps Opus complexity to quantify the encode-CPU
// vs quality tradeoff — the only lever on the proxy's dominant per-session cost.
func BenchmarkEncodeComplexity(b *testing.B) {
	frame := richFrame()
	for _, c := range []int{0, 3, 5, 8, 10} {
		b.Run(fmt.Sprintf("c=%d", c), func(b *testing.B) {
			enc, err := NewEncoder()
			if err != nil {
				b.Fatal(err)
			}
			if err := enc.e.SetComplexity(c); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := enc.Encode(frame); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
