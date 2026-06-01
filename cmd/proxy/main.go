// Command proxy is a real-time LLM proxy: browsers connect over WebRTC, the
// proxy terminates the peer connection and bridges audio to a streaming LLM
// provider's WebSocket API. Pick a provider with ?model=gemini|doubao.
package main

import (
	"bufio"
	"log"
	"os"
	"strings"
)

func main() {
	loadDotenv(".env")
	if err := runProxy(parseFlags()); err != nil {
		log.Fatal(err)
	}
}

func loadDotenv(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		k = strings.TrimSpace(k)
		v = strings.Trim(strings.TrimSpace(v), `"'`)
		if _, exists := os.LookupEnv(k); !exists {
			os.Setenv(k, v)
		}
	}
}
