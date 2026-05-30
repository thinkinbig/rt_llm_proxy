package audio

// ResampleLinear converts mono int16 PCM from inRate to outRate using linear
// interpolation. For the integer ratios we use (48k<->16k, 24k->48k) the output
// length is exact, so per-chunk boundaries line up and artifacts are minimal.
// Good enough for speech; swap for a polyphase filter if quality matters.
func ResampleLinear(in []int16, inRate, outRate int) []int16 {
	if inRate == outRate || len(in) == 0 {
		return in
	}
	outLen := len(in) * outRate / inRate
	if outLen == 0 {
		return nil
	}
	out := make([]int16, outLen)
	ratio := float64(inRate) / float64(outRate)
	for i := range out {
		pos := float64(i) * ratio
		idx := int(pos)
		frac := pos - float64(idx)
		s0 := float64(in[idx])
		s1 := s0
		if idx+1 < len(in) {
			s1 = float64(in[idx+1])
		}
		out[i] = int16(s0 + (s1-s0)*frac)
	}
	return out
}
