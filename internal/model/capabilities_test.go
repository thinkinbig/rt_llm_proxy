package model

import "testing"

// bareModel implements only the required Model interface — no optional capabilities.
type bareModel struct{}

func (bareModel) SendAudio([]int16) error { return nil }
func (bareModel) SendText(string) error   { return nil }
func (bareModel) Recv() ([]int16, error)  { return nil, nil }
func (bareModel) Close() error            { return nil }

// fullModel implements every optional capability. supportsInt controls the
// Interrupter runtime gate (mirrors gemini's vadCfg.Enabled).
type fullModel struct {
	bareModel
	supportsInt bool
}

func (fullModel) RestoreContext([]RestoredTurn) error { return nil }
func (fullModel) RecvTranscript() (Transcript, error) { return Transcript{}, nil }
func (fullModel) RecvToolCall() (ToolCall, error)     { return ToolCall{}, nil }
func (fullModel) SendToolResult(ToolResult) error     { return nil }
func (m fullModel) SupportsInterruption() bool        { return m.supportsInt }
func (fullModel) RecvInterrupted() (bool, error)      { return false, nil }
func (fullModel) HandleInterrupted() error            { return nil }

func TestResolveBareModel(t *testing.T) {
	c := Resolve(bareModel{})
	if c.Restorer != nil || c.Transcriber != nil || c.Tools != nil || c.Interrupter != nil {
		t.Fatalf("bare model resolved a capability: %+v", c)
	}
}

func TestResolveFullModel(t *testing.T) {
	c := Resolve(fullModel{supportsInt: true})
	if c.Restorer == nil || c.Transcriber == nil || c.Tools == nil || c.Interrupter == nil {
		t.Fatalf("full model missing a capability: %+v", c)
	}
}

func TestResolveInterrupterRuntimeGate(t *testing.T) {
	// A model that IMPLEMENTS Interrupter but reports it inactive (gemini with VAD
	// disabled) must NOT be resolved as interruptible.
	c := Resolve(fullModel{supportsInt: false})
	if c.Interrupter != nil {
		t.Fatal("interrupter resolved despite SupportsInterruption()==false")
	}
	// The other capabilities are unaffected by the interruption gate.
	if c.Transcriber == nil || c.Tools == nil {
		t.Fatalf("non-interrupt capabilities wrongly gated: %+v", c)
	}
}
