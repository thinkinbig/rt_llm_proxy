package cascade

import (
	"strings"
	"unicode"
)

// sentenceEnders are runes that mark the end of a speakable sentence and
// trigger the quick TTS phase. Chosen to balance latency vs. fragment risk.
const sentenceEnders = ".?!\n"

// containsSentenceEnd reports whether s contains any sentence-ending rune.
func containsSentenceEnd(s string) bool {
	return strings.ContainsAny(s, sentenceEnders) ||
		strings.IndexFunc(s, func(r rune) bool {
			return unicode.Is(unicode.Po, r) // other punctuation (CJK 。？！)
		}) >= 0
}

// jaccard returns the token-level Jaccard similarity between two strings.
// Tokens are whitespace-split words, lowercased. Returns 1.0 for equal
// strings and 0.0 when both are empty.
func jaccard(a, b string) float64 {
	sa := tokenSet(a)
	sb := tokenSet(b)
	if len(sa) == 0 && len(sb) == 0 {
		return 1.0
	}
	var inter int
	for t := range sa {
		if sb[t] {
			inter++
		}
	}
	union := len(sa) + len(sb) - inter
	if union == 0 {
		return 1.0
	}
	return float64(inter) / float64(union)
}

func tokenSet(s string) map[string]bool {
	m := make(map[string]bool)
	for w := range strings.FieldsSeq(strings.ToLower(s)) {
		m[w] = true
	}
	return m
}
