package transcript

import "sync"

// Recorder assigns monotonic seq numbers, retains bounded history, and notifies
// listeners. The Bridge owns one Recorder per session.
type Recorder struct {
	mu        sync.Mutex
	seq       uint64
	history   []Line
	histLimit int
	meta      SessionMeta
	listener  Listener
}

// NewRecorder builds a recorder starting at startSeq with optional initial
// history (for reconnect). listener may be nil.
func NewRecorder(startSeq uint64, initial []Line, histLimit int, meta SessionMeta, listener Listener) *Recorder {
	if listener == nil {
		listener = NopListener{}
	}
	return &Recorder{
		seq:       startSeq,
		history:   append([]Line(nil), initial...),
		histLimit: histLimit,
		meta:      meta,
		listener:  listener,
	}
}

// Record appends one line, notifies the listener, and returns the line.
func (r *Recorder) Record(role, text string) Line {
	r.mu.Lock()
	r.seq++
	line := Line{Seq: r.seq, Role: role, Text: text}
	r.history = append(r.history, line)
	if extra := len(r.history) - r.histLimit; extra > 0 {
		r.history = append([]Line(nil), r.history[extra:]...)
	}
	meta := r.meta
	listener := r.listener
	r.mu.Unlock()

	listener.OnLine(meta, line)
	return line
}

// Snapshot returns lines with seq > afterSeq.
func (r *Recorder) Snapshot(afterSeq uint64) []Line {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Line, 0, len(r.history))
	for _, line := range r.history {
		if line.Seq > afterSeq {
			out = append(out, line)
		}
	}
	return out
}

// FullHistory returns a copy of all retained lines.
func (r *Recorder) FullHistory() []Line {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Line(nil), r.history...)
}

// MaxSeq returns the highest seq assigned so far.
func (r *Recorder) MaxSeq() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.seq
}
