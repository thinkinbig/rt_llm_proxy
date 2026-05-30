package model

import "encoding/binary"

// pcmToBytes serializes mono s16 samples as little-endian bytes.
func pcmToBytes(pcm []int16) []byte {
	b := make([]byte, len(pcm)*2)
	for i, s := range pcm {
		binary.LittleEndian.PutUint16(b[i*2:], uint16(s))
	}
	return b
}

// bytesToPCM parses little-endian s16 bytes into samples.
func bytesToPCM(b []byte) []int16 {
	pcm := make([]int16, len(b)/2)
	for i := range pcm {
		pcm[i] = int16(binary.LittleEndian.Uint16(b[i*2:]))
	}
	return pcm
}
