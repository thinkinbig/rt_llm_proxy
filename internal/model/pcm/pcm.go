// Package pcm serializes mono s16 PCM for provider wire formats that use
// little-endian s16 bytes. Only shared because both adapters uplink s16 at 16kHz.
package pcm

import "encoding/binary"

// ToBytes serializes mono s16 samples as little-endian bytes.
func ToBytes(samples []int16) []byte {
	b := make([]byte, len(samples)*2)
	for i, s := range samples {
		binary.LittleEndian.PutUint16(b[i*2:], uint16(s))
	}
	return b
}

// FromBytes parses little-endian s16 bytes into samples.
func FromBytes(b []byte) []int16 {
	samples := make([]int16, len(b)/2)
	for i := range samples {
		samples[i] = int16(binary.LittleEndian.Uint16(b[i*2:]))
	}
	return samples
}
