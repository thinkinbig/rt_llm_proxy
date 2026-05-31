# Domain glossary — rt-llm-proxy

## Transcript line

One speech-to-text turn within a **Session**, identified by `(seq, role, text)`.
`seq` is monotonic per session and is the single authority for reconnect
(`X-Last-Seq`), data-channel JSON, and side-channel Kafka events.

## Recording point

The Bridge **Recorder** (`internal/transcript.Recorder`) is the only place that
assigns `seq` and retains history. All transcript sources (data-channel typed
text, provider STT via `RecvTranscript`) call `Record(role, text)`.

## Provider transcript

A provider adapter emits `model.Transcript{Role, Text}` without session context.
The Bridge converts it to a **transcript line** at the recording point.

## Side-channel tap

`sidechannel.Tap` implements `transcript.Listener` and publishes
`TranscriptEvent` protobuf messages using the seq from the recording point —
it never assigns its own seq.
