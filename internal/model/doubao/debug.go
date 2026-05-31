package doubao

import (
	"log"
	"os"
)

// Protocol tracing for the Doubao session, kept out of the bridging hot path.
// Enable with DOUBAO_DEBUG=1; off by default so production logs stay quiet.
var doubaoDebug = os.Getenv("DOUBAO_DEBUG") != ""

// dbTraceEvent logs one server frame. Control events carry JSON (e.g.
// SessionStarted, ASR results, usage); TTS frames are summarized by size so the
// log isn't flooded with raw audio. No-op unless DOUBAO_DEBUG is set.
func dbTraceEvent(f *dbFrame, payload []byte) {
	if !doubaoDebug {
		return
	}
	if f.event == dbEvTTSResponse {
		log.Printf("doubao: TTS frame %d bytes (%d f32 samples @ %dHz)", len(payload), len(payload)/4, doubaoOutRate)
		return
	}
	log.Printf("doubao: event=%d payload=%s", f.event, string(payload))
}
