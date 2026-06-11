package model

// Capabilities is the resolved set of optional capabilities a concrete Model
// supports. Each field is non-nil only when the model both implements the
// capability and (for Interrupter) reports it active. It exists so callers learn
// what a provider can do in ONE place instead of scattering type assertions:
// Resolve is the single seam, and adding a capability means editing only this
// file.
type Capabilities struct {
	Restorer    ContextRestorer // non-nil: can be seeded with prior turns on reconnect
	Transcriber Transcriber     // non-nil: surfaces STT to forward to the data channel
	Tools       ToolDispatcher  // non-nil: supports function calling
	Interrupter Interrupter     // non-nil: supports AND has enabled VAD barge-in
}

// Resolve discovers a model's optional capabilities. Interrupter is resolved
// only when the model both implements it and reports SupportsInterruption — the
// runtime gate (e.g. gemini with VAD disabled implements Interrupter but is not
// interruptible, so it is left nil).
func Resolve(m Model) Capabilities {
	var c Capabilities
	c.Restorer, _ = m.(ContextRestorer)
	c.Transcriber, _ = m.(Transcriber)
	c.Tools, _ = m.(ToolDispatcher)
	if it, ok := m.(Interrupter); ok && it.SupportsInterruption() {
		c.Interrupter = it
	}
	return c
}
