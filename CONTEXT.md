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

## Data-channel protocol

`rtc/dcproto` owns the browser-facing data-channel wire contract: it encodes
outbound **transcript lines** (bare `{seq,role,text}`, no type tag) and model
**tool calls** (`{"type":"tool_call",...}`), and classifies inbound strings via
`Decode` — a `{"type":"tool_result",...}` envelope yields a `model.ToolResult`,
anything else (plain text or any other JSON) is user text. The format is
asymmetric by design (tools tagged, transcript untagged) to match the existing
client. `Decode` only classifies; the **Bridge** keeps the capability gate
(routing a tool_result requires a `model.ToolDispatcher`).

## Model capabilities

A provider's optional capabilities — `ContextRestorer`, `Transcriber`,
`ToolDispatcher`, `Interrupter` — are resolved in one place by `model.Resolve`,
which returns a `model.Capabilities` value (each field nil when absent). The
**Bridge** calls it once and reads the resolved set instead of scattering type
assertions across `Serve`. `Interrupter` unifies the three barge-in methods that
were once split across `Model` and `Transcriber`; it is resolved only when the
model both implements it **and** reports `SupportsInterruption()` (a runtime gate
— e.g. gemini, only when VAD is enabled). Adding a capability touches only
`model.Resolve`.

## Side-channel tap

`sidechannel.Tap` implements `transcript.Listener` and publishes
`TranscriptEvent` protobuf messages using the seq from the recording point —
it never assigns its own seq.

## Session offer intake

`offer.Intake` is the control-plane module for a new **Session**: rate limit,
**Provider guard**, reconnect replay (`ResolveReplay`), then `MediaHub.Serve`
to start the **Bridge**. `offer.Handler` is the thin HTTP adapter.

## Provider guard

`modelcb.Manager` is the **Provider guard**: per-provider circuit state,
`AllowDial` / `RecordDial` on the offer path, and `RecordStreamFault` for early
stream failures (within `modelcb.EarlyFaultWindow`) before the **Bridge** has
sent audio. Dial and stream failures are tracked as separate streaks; a
successful dial resets both. Loopback and a nil manager skip all guard logic.

## Replay source wiring

`offer.Intake` uses an explicit `Replayer` adapter (`Intake.ReplayIndex`) that
calls the replay-index HTTP service. It does not infer replay capability from
`Publisher`.

## Session archive

`rtc` keeps reconnect history in a dedicated `sessionArchiveStore` module (TTL +
ownership checks), separate from the live Bridge session registry.

## Composition root

`cmd/proxy` keeps startup composition in `runProxy`: it wires adapters from
flags/config, starts admin endpoints, and enforces shutdown order
(`http.Server.Shutdown` -> `Hub.CloseAll` -> `Publisher.Close`).

## Session scope

`rtc.sessionScope` owns one bridge’s media context, peer connection, and model.
`Hub.Serve` commits the scope only after SDP succeeds; otherwise `defer` aborts
without registering the session. Media lifetime is not tied to the offer HTTP
request context.
