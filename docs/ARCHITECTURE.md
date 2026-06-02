# Architecture & Engineering Notes

How rt-llm-proxy is put together, and **why** each non-obvious engineering
decision is the way it is. Kept deliberately small — this is a small project.

| § | Topic |
|---|---|
| **1** | Proxy core — WebRTC bridge, control plane, fault tolerance |
| **2** | **Cascade pipeline** — orchestrator (in-process) + sidecars (ASR/LLM/TTS/turn-detect) |
| **3** | Modules & seams |
| **4** | Engineering optimizations (pacing, Opus, replay, …) |
| **5** | Tests |

## 1. Architecture

**Invariant (load-bearing):** the control / personalization plane must never gate
the real-time media plane (§4.3). Fault tolerance below follows that rule —
degrade the feature, not the call, unless the failure is an explicit hard guard
(rate limit at capacity, circuit open).

### 1.1 System overview

```mermaid
flowchart TB
  subgraph browser["Browser"]
    WEB["WebRTC<br/>Opus audio + DataChannel JSON"]
  end

  subgraph proxy["rt-llm-proxy"]
    direction TB
    subgraph control["Control plane — POST /?model="]
      OFFER["offer.Handler"]
      RL["ratelimit"]
      AUTH["auth"]
      CB["modelcb"]
      REPLAY["ResolveReplay"]
      OFFER --> RL
      OFFER --> AUTH
      OFFER --> CB
      OFFER --> REPLAY
    end
    subgraph data["Data plane — per session"]
      HUB["rtc.Hub / SessionManager"]
      BRIDGE["rtc.Bridge"]
      REC["transcript.Recorder"]
      ADAPT["adaptive Opus complexity<br/>(optional)"]
      HUB --> BRIDGE
      BRIDGE --> REC
      ADAPT -.-> BRIDGE
    end
    OFFER -->|"mint session_id · replay history"| HUB
    TAP["sidechannel.Tap"]
    REC --> TAP
  end

  subgraph optional["Optional backends"]
    REDIS[("Redis")]
    KAFKA[("Kafka")]
  end

  subgraph upstream["Providers"]
    STS["gemini · doubao · loopback<br/>(WebSocket PCM)"]
    CASCADE["cascade<br/>(HTTP stages)"]
  end

  WEB <-->|"Opus ↔ JSON {seq,role,text}"| BRIDGE
  BRIDGE <-->|"mono s16 PCM @ 48 kHz"| STS
  BRIDGE <-->|"same Model seam"| CASCADE
  RL <-->|"INCR+EXPIRE (Lua)"| REDIS
  TAP -->|"Publish (non-blocking)"| KAFKA
  REPLAY -.->|"memory archive · optional replay-index"| HUB
  REPLAY -.-> KAFKA
  CB -.->|"connect + early stream fault"| STS
```

Solid arrows are on the hot path; dashed arrows are best-effort or optional.
Redis and Kafka are **never** on the 20ms audio loop.

### 1.2 Media data path

```
browser ──WebRTC(Opus audio + datachannel)──▶ rtc.Bridge ──▶ provider adapter
        ◀──────────── Opus audio ────────────              ◀──── PCM ──────
```

STS providers (gemini, doubao) speak WebSocket with provider-native PCM;
`?model=cascade` uses the **same** `Model` seam but chains HTTP stages inside
`internal/model/cascade` — see [#cascade](#cascade).

- **Inbound (mic → model):** `track.ReadRTP` → Opus decode → mono s16 PCM @48kHz
  → `Model.SendAudio`. The provider adapter resamples to its own wire rate.
- **Outbound (model → speaker):** `Model.Recv` → accumulate into a buffer →
  Opus-encode each 20ms / 960-sample frame → `WriteSample`, **paced at real time**
  (session `time.Ticker`, §4.1). Optional `-adaptive` lowers encoder complexity
  under load (§4.11).
- **Data channel:** browser typed text → `Recorder.Record("user")` + `Model.SendText`;
  provider STT (`RecvTranscript`) → `Recorder.Record` → browser as JSON
  `{seq,role,text}` so reconnect can resume from `last_seq`.

### 1.3 Control & reconnect path

```mermaid
sequenceDiagram
  participant B as Browser
  participant O as offer.Handler
  participant R as ResolveReplay
  participant H as rtc.Hub
  participant M as Provider

  B->>O: POST SDP offer<br/>?model= · Bearer · X-Session-ID / X-Last-Seq
  O->>O: ratelimit (429 if full)
  O->>O: auth → user_id (or "")
  O->>O: modelcb.Allow (503 if open)
  O->>M: Models.New (502 on dial error)
  O->>R: memory archive → optional replay-index
  Note over R: timeout / miss → status miss,<br/>media still starts
  O->>H: Serve(SDP, model, replay history)
  O-->>B: answer SDP + X-Session-ID + X-Replay-Status
  H-->>B: WebRTC media (background)
```

Reconnect is **best-effort**: malformed replay headers → `400`; incomplete
headers → fresh session; replay-index over budget → `index_timeout` / `index_error` but
the call proceeds.

### 1.4 Fault tolerance & degradation

| Layer | Component | Trigger | Policy | Blocks media? |
|---|---|---|---|---|
| Control | `ratelimit` | Redis error | **Fail open** (allow + log) | No |
| Control | `ratelimit` | window full | `429` | Yes (offer only) |
| Control | `auth` | missing / invalid token | **Anonymous** `user_id=""` | No |
| Control | `modelcb` | circuit open / half-open gated | `503` + `Retry-After` | Yes (offer only) |
| Control | `modelcb` | N connect failures / auth error | Open per provider | Yes (offer only) |
| Control | `modelcb` | stream error before first audio (within 10s) | `StreamFaultAt` → `RecordStreamFault` | No (existing sessions continue) |
| Control | `ResolveReplay` | timeout / miss / disabled | `X-Replay-Status` degrade | No |
| Side | `sidechannel` / Kafka | buffer full / closed | **Drop** + `dropped_total` | No |
| Data | `rtc.Bridge` | provider silence | Ticker coalesces ticks (no burst) | No |
| Data | Opus | packet loss | In-band FEC + DTX (uplink fmtp, downlink encoder) | No |
| Data | `adaptive` | high session count or frame drift | Lower Opus complexity | No (quality tradeoff) |
| Data | lifecycle | disconnect / SIGTERM | `sync.Once` cleanup · `CloseAll` | N/A |

```mermaid
flowchart LR
  subgraph hard["Hard guards — offer only"]
    RL429["ratelimit full → 429"]
    CB503["circuit open → 503"]
  end
  subgraph soft["Soft degradation — call continues"]
    RLO["Redis blip → allow"]
    ANON["auth fail → anonymous"]
    REP["replay miss → fresh / partial history"]
    DROP["Kafka full → drop event"]
    ADP["load → lower Opus complexity"]
  end
  subgraph media["Media path — no external RTT"]
    BR["Bridge ↔ provider PCM loop"]
  end
  hard --> OFFER["offer.Handler"]
  soft --> OFFER
  OFFER --> media
```

Failover levels (L1–L4) and production scaling notes live in
[README § Scaling & failover](../README.md#scaling-failover).

<a id="cascade"></a>

## 2. Cascade pipeline

`?model=cascade` selects a **self-hosted ASR → LLM → TTS** stack wired as a
fourth provider adapter. It implements `model.Model` and `model.Transcriber` —
the Bridge, transcript recorder, side-channel, and reconnect machinery are
**unchanged**. Only the object behind the Model seam differs.

This chapter is the deep dive; operational flags and Docker Compose live in
[README § Cascade](../README.md#cascade).

<a id="orchestrator-vs-sidecars"></a>

### 2.1 Orchestrator vs sidecars

Deployment splits into **one in-process orchestrator** and **four external
sidecars**. Everything except turn orchestration runs out-of-process; the proxy
process only hosts the coordinator and thin HTTP/WebSocket clients.

| Layer | Where it runs | What it does |
|---|---|---|
| **Orchestrator** | `internal/model/cascade` inside the proxy | `run()` turn loop, `respond()` LLM→TTS pipeline, barge-in, history, business seams (`OnLLMToken`, `SetAudioSource`). Implements `model.Model` — the Bridge treats it like gemini/doubao. |
| **Stage clients** | `internal/model/cascade/{asr,llm,tts,turndetect}/` — same process as orchestrator | Thin adapters that speak each sidecar's wire protocol. Not separate services; wired via `cascade.Config`. |
| **Sidecars** | Separate containers on the internal Docker network | ASR (`realtimestt/`), LLM (vLLM), TTS (xtts-streaming-server), turn-detect (`turndetect/`, optional). Only the proxy port is exposed publicly. |

`rtc.Bridge` and the control plane (rate limit, replay, Kafka side-channel) are
**shared proxy infrastructure** — not part of the cascade orchestrator or its
sidecars.

### 2.2 End-to-end data flow

```
browser ──WebRTC(Opus)──▶ rtc.Bridge ──PCM 48k──▶ RealtimeSTT (ASR)
                                                      │ partials · finals · speech_start
                                                      ▼
                                            vLLM / Qwen (OpenAI API)
                                                      │ token stream
                                                      ▼
                                            XTTS streaming TTS
                                                      │ PCM chunks
        ◀──Opus────────── rtc.Bridge ◀───────────────┘
```

```mermaid
flowchart LR
  subgraph bridge["rtc.Bridge — unchanged"]
    M["Model seam<br/>SendAudio · Recv · SendText"]
  end

  subgraph orchestrator["Orchestrator — in-process (internal/model/cascade)"]
    RUN["run() — history owner"]
    ASR["ASR client"]
    TD["TurnDetector client<br/>(optional)"]
    RESP["respond() — LLM + TTS"]
    RUN --> ASR
    ASR -->|"partials · finals · speech_start"| RUN
    RUN --> TD
    TD -->|"SuggestedPause"| RUN
    RUN --> RESP
    RESP -->|"PCM chunks"| M
  end

  subgraph sidecars["Sidecars — HTTP / WS"]
    STT["realtimestt/"]
    VLLM["vLLM"]
    XTTS["xtts-streaming-server"]
    TURN["turndetect/"]
  end

  M --> ASR
  ASR --- STT
  RESP --- VLLM
  RESP --- XTTS
  TD --- TURN
```

All sidecar hops are LAN-local on a single GPU host (~1–5 ms). Resampling and
wire formats stay **inside each stage client** — the Bridge only ever sees
mono s16 PCM @ 48 kHz (same contract as gemini/doubao).

### 2.3 Stages & injection

`cascade.Config` takes injectable stage interfaces — same pattern as
`ratelimit.New(addr, …)` with production defaults wired in
`offer.ProdModelFactory`:

| Interface | Stage client (orchestrator) | Sidecar |
|---|---|---|
| `ASR` | `asr.NewWhisper(url)` | `realtimestt/` — Silero VAD + faster-whisper; partials, finals, `speech_start` |
| `LLM` | `llm.New(url, model)` | vLLM — OpenAI-compatible API (default Qwen3.5-9B) |
| `TTS` | `tts.NewXTTSStream(url, …)` | xtts-streaming-server — incremental PCM via `/tts_stream` |
| `TurnDetector` | `turndetect.NewHTTP(url)` or `NopTurnDetector{}` | `turndetect/` — sentence-finished classifier (optional) |

Tests swap in `fakestage/` stubs (no sidecars). Docker stack:
`docker-compose.cascade.yml` — orchestrator in the proxy container, four
sidecars on the internal network (see README).

### 2.4 Turn orchestration

One `run()` goroutine owns `history` and all turn state — no lock on
conversation data. It reads ASR events, typed `SendText`, turn-detect timers,
and completed model replies from `modelTurnCh`.

```mermaid
sequenceDiagram
  participant ASR as RealtimeSTT
  participant RUN as cascade.run()
  participant TD as TurnDetector
  participant RESP as respond()
  participant LLM as vLLM
  participant TTS as XTTS

  ASR->>RUN: ASRPartial (stable sentence?)
  opt speculative
    RUN->>RESP: start respond() snapshot
    RESP->>LLM: Generate(history)
  end
  ASR->>RUN: ASRFinal
  RUN->>TD: SuggestedPause(text)
  TD-->>RUN: pause (or 0)
  RUN->>RESP: start respond() snapshot
  RESP->>LLM: Generate(history)
  loop per sentence segment
    LLM-->>RESP: token deltas
    RESP->>TTS: Synthesize(segment)
    TTS-->>RESP: PCM chunks → recvCh
  end
  RESP->>RUN: modelTurnCh ← full reply
  RUN->>RUN: append model turn to history

  Note over ASR,RUN: ASRSpeechStarted → bargeIn()<br/>cancels RESP, drains recvCh
```

| Event | Action |
|---|---|
| `ASRPartial` | Live caption via `RecvTranscript`; cancel pending turn timer; optionally **speculative** LLM start when partial is stable (~200 ms, sentence-ending punctuation) |
| `ASRSpeechStarted` | **Barge-in**: cancel in-flight `respond()`, drain `recvCh` |
| `ASRFinal` | Dedup (Jaccard ≥ 0.9); confirm or discard speculation; schedule LLM after turn-detect pause (or immediately) |
| `SendText` | Same path as ASR final (typed data-channel input) |

`respond()` receives a **snapshot** of history and never mutates it. The
completed reply is handed back over `modelTurnCh` so `run()` remains the sole
history writer.

### 2.5 Low-latency design

Four mechanisms stack to cut time-to-first-audio (TTFA):

1. **Speculative LLM start** — stable ASR partials that look like complete
   sentences commit a tentative user turn and fire the LLM before `ASRFinal`.
   Matching final patches the text and keeps in-flight generation; mismatch
   discards speculation and starts fresh.

2. **Sentence-segmented streaming TTS** — `respond()` splits LLM tokens on
   sentence boundaries (`. ? !` newline / CJK punctuation). A segmenter pushes
   completed sentences to a concurrent TTS worker while the LLM keeps generating.
   The LLM is never blocked waiting for synthesis.

3. **XTTS streaming** — each sentence streams PCM incrementally as it is
   synthesised, so playback begins before the sentence is fully rendered.
   Optional `QuickSynthesizer` path for the first segment when implemented.

4. **Turn detection (optional)** — `-cascade-turndetect` adds a bounded pause
   after ASR final before committing the turn. `NopTurnDetector` (default when
   unset) fires immediately.

### 2.6 Barge-in

Triggered by `ASRSpeechStarted` from RealtimeSTT (user began speaking while
the bot is talking). Cancel sequence mirrors RealtimeVoiceChat
`process_abort_generation()`:

1. Cancel `genCtx` → stops LLM HTTP stream and TTS synthesis.
2. Wait on `genDone` → `respond()` goroutine has exited.
3. Drain `recvCh` → discard queued audio before the next turn starts.

Step 2 is load-bearing: without it, an old `respond()` could write to `recvCh`
after a new turn begins. Both the segmenter and TTS worker in `respond()` run
under `genCtx` so barge-in cancels them together.

Duplicate utterances (Jaccard token similarity ≥ 0.9) are ignored — the bot will
not restart a reply it is already giving for essentially the same input.

### 2.7 Business seams

The cascade exposes two hooks that keep the core proxy business-agnostic while
enabling downstream use cases (e.g. a personalized real-time DJ). Code examples
in [README § Cascade](../README.md#cascade).

**LLM intercept — `Config.OnLLMToken`**

Called per token before TTS. Return `("", false)` to pass through,
`(replacement, true)` to substitute, `("", true)` to drop silently. History
always receives the original token.

**Output-mix — `Cascade.SetAudioSource`**

Injects any `AudioSource` (mono s16, 48 kHz) into outbound audio. `Recv()` reads
from it until `io.EOF`, then falls back to TTS seamlessly. Replacing or clearing
a source closes the previous one.

### 2.8 Fault tolerance & reconnect

| Failure | Policy | Blocks session? |
|---|---|---|
| LLM/TTS transient HTTP error | Retry once, then skip segment | No |
| LLM/TTS hard error mid-turn | Skip segment, turn may be partial | No |
| GPU host crash | All cascade sessions end | Yes (SPOF) |
| Reconnect with replay headers | Transcript lines restored via `seq` | No |
| Reconnect LLM context | **Not restored** — history lived on GPU host | No (degraded) |

Cascade reconnect uses the same `X-Session-ID` / `X-Last-Seq` path as other
providers (provider-scoped). Transcript text replays; the LLM starts from
`-cascade-system` plus any replayed lines the Bridge injects, not from the
in-memory `history` slice that died with the old session.

For multi-host resilience the three stages would need independent reconnect and
state — out of scope for the current single-GPU deployment.

## 3. Modules & seams

| Module | Package | Role |
|---|---|---|
| **Bridge** | `internal/rtc` | Terminates one browser WebRTC peer connection; pumps audio + data-channel text both ways. Talks **only** to the Model seam. Owns the transcript **Recorder** (single recording point). |
| **Session archive** | `internal/rtc` (`sessionArchiveStore`) | In-memory reconnect archive for disconnected sessions with TTL + ownership checks; used by `Resume`/`SessionState`. |
| **Transcript** | `internal/transcript` | Session-scoped `Line{seq,role,text}` and `Recorder` — the single seq authority shared by data channel, reconnect history, and side-channel. |
| **Session offer intake** | `internal/offer` (`Intake`) | Control-plane chain: rate limit, provider guard, reconnect replay, then `Hub.Serve`. |
| **Offer HTTP adapter** | `internal/offer` (`Handler`) | Maps POST / to `Intake.ServeOffer`. |
| **Provider guard** | `internal/modelcb` | Per-provider circuit: `AllowDial` / `RecordDial` on offer; early stream faults via `StreamFaultAt` on the Bridge. |
| **Auth** | `internal/auth` | Bearer → `user_id` on the offer path; fail-open anonymous. |
| **Adaptive** | `internal/adaptive` | Optional Opus encode-complexity controller under load (`-adaptive`). |
| **Model seam** | `internal/model` | The provider-agnostic `Model` interface (`SendAudio`/`SendText`/`Recv`/`Close`). Optional `Transcriber` (`RecvTranscript`) for STT. |
| **Providers / adapters** | `internal/model/gemini`, `internal/model/doubao` | One concrete `Model` per streaming STS API. Each owns its WebSocket protocol and native audio format. |
| **Cascade** | `internal/model/cascade` | In-process **orchestrator** — turn loop, barge-in, business seams. See [#cascade](#cascade). |
| **Cascade stages** | `internal/model/cascade/asr`, `llm`, `tts`, `turndetect` | In-process stage clients; sidecars in [§2.1](#orchestrator-vs-sidecars). |
| **Side-channel** | `internal/sidechannel` | `Tap` implements `transcript.Listener`; publishes `TranscriptEvent` to Kafka/stdout using the Bridge-assigned seq. |
| **Replay index** | `cmd/replay`, `internal/replayindex` | Kafka consumer + HTTP store; serves `GET /v1/replay` for cross-node reconnect. |
| **PCM helpers** | `internal/model/pcm` | `ToBytes` / `FromBytes` — s16le serialize for the uplink. Shared only because both adapters serialize contract-side s16; **not** a unified decode layer. |
| **Audio** | `internal/audio` | Opus encode/decode (libopus via cgo) + linear resampler. |
| **Rate limit** | `internal/ratelimit` | Redis fixed-window limiter for the SDP offer endpoint. **Control plane only.** |
| **Composition root** | `cmd/proxy` (`runProxy`) | Wires runtime adapters from config and owns process shutdown ordering. |

### The audio contract (load-bearing)

**Every audio chunk crossing the Model seam is mono signed-16 PCM at 48kHz**
(WebRTC's native Opus rate). Providers convert to/from their own format
*internally*, so the Bridge never knows a provider's wire format. This single
canonical format is what keeps the Bridge fully provider-agnostic.

### Provider asymmetry is intentional, not duplication

| Provider | downstream → contract | rate source |
|---|---|---|
| Gemini | s16le | read off the wire per chunk, from the MIME type (`inlineAudioToModelPCM`) |
| Doubao | f32le | fixed `24000` const (`ttsToModelPCM`, `f32leToPCM`) |

Gemini reading the rate off the wire is the safer pattern; Doubao's protocol
*can't* carry it, so its rate is an unverifiable const (confirmed once by
dumping + analyzing the raw stream). **Don't flatten Gemini's per-chunk rate
into a static const to look symmetric** — that deletes the safer behavior.

## 4. Engineering optimization points

Each entry: what we do, and the failure mode it avoids.

### 4.1 Real-time outbound pacing without clock drift  *(`rtc/bridge.go`, `writeOutbound`)*

We pace outbound frames with a **single session-level `time.Ticker`**, not a
per-frame `time.After(frameDur)`.

- **Why pace at all:** dumping the whole response to the browser at once would
  overrun its jitter buffer. We feed audio at real time (mirrors the reference
  `proxy.py`).
- **Why a Ticker, not `time.After`:** `time.After` starts its 20ms *after* the
  encode + `WriteSample` work, so each frame's real period is `20ms + encode`.
  That is slower than real time, so the buffer backs up and **end-to-end latency
  grows monotonically with response length**. A Ticker fires on a fixed wall
  clock; encode time is absorbed into the 20ms instead of added on top → zero
  drift.
- **Silence handling:** while `Recv` blocks on provider silence the Ticker keeps
  firing, but its size-1 channel coalesces the extra ticks — so resuming speech
  does **not** burst out a backlog of frames.

### 4.2 Atomic rate limiting + fail-open  *(`internal/ratelimit`)*

- **Atomic INCR+EXPIRE via a Lua script.** A separate `INCR` then `EXPIRE` has a
  crash window: dying between the two leaves the key with **no TTL**, so the
  counter never resets and that IP is **locked out permanently**. The Lua script
  makes both one atomic step.
- **Fail open on Redis errors.** Rate limiting is a soft guard on the control
  plane; a Redis blip should not take down the real-time service. On error
  `Allow` returns `true` and surfaces the error for logging only.

### 4.3 Redis stays strictly on the control plane

Redis touches **only** the SDP offer endpoint (session-creation rate). The media
path (Opus ↔ PCM ↔ provider) never makes a network round-trip to Redis —
routing 20ms audio frames through Redis would add latency and defeat the point
of a real-time proxy. This is an invariant, not an accident.

### 4.4 Shared pion API / MediaEngine  *(`rtc.Hub`)*

The `Hub` builds the pion `API` (with an Opus-tuned `MediaEngine` + default
interceptors) **once** and reuses it for every peer connection, rather than
rebuilding codec/interceptor state per session.

### 4.5 Opus tuning for lossy/quiet links  *(`audio/opus.go`, `rtc/bridge.go`)*

Opus is tuned twice — once per direction — for real-time speech over lossy
links. Both sides trade a little fidelity for resilience and bandwidth.

**Browser → proxy (mic uplink).** The answer SDP advertises this fmtp on the
registered Opus codec (`rtc/bridge.go` → `MediaEngine`):

`minptime=10;useinbandfec=1;usedtx=1;maxaveragebitrate=16000`

| fmtp field | Effect |
|---|---|
| `minptime=10` | Allow 10ms frames — lower first-packet / short-utterance latency. |
| `useinbandfec=1` | In-band FEC: recover partial audio from later packets after loss. |
| `usedtx=1` | DTX: suppress full frames during silence — saves bandwidth and jitter-buffer pressure. |
| `maxaveragebitrate=16000` | Cap average bitrate ~16 kbps — narrowband speech is enough for LLM dialogue. |

The proxy decodes with a **mono** decoder (`audio/opus.go`); stereo in SDP is
normal WebRTC negotiation and is down-mixed automatically.

**Proxy → browser (model downlink).** `writeOutbound` encodes via
`audio.NewEncoder`: `AppVoIP`, in-band FEC + DTX, and `PacketLossPerc=10`
(what actually activates FEC on the encoder side — fmtp alone is not enough).
Frames are 20ms / 960 samples @ 48kHz, paced by the §4.1 Ticker.

### 4.6 Non-trickle ICE, host candidates only  *(`rtc/bridge.go`, `Serve`)*

`Serve` waits on `GatheringCompletePromise` and returns the **full** answer SDP
with candidates (non-trickle). No STUN/TURN/SFU (`iceServers=[]`). The proxy is
intentionally **not** NAT-traversal infrastructure — simpler signaling, fewer
moving parts. Tradeoff: media won't traverse cluster NAT; horizontal scale and
failover are partial by design (L1–L4 — see README's "Scaling & failover").
Run the container on a host the browser can reach directly.

### 4.7 Linear resampling at integer ratios  *(`audio/resample.go`)*

We use linear interpolation. At our integer ratios (48k↔16k, 24k→48k) output
length is exact and per-chunk boundaries line up, so artifacts are minimal —
good enough for speech. Swap for a polyphase filter if quality ever matters.

### 4.8 Lifecycle & backpressure  *(`rtc/bridge.go`, `cmd/proxy/main.go`)*

- **Idempotent teardown:** `session.cleanup` runs under a `sync.Once`; connection
  state changes, model EOF, and hub shutdown all funnel through it safely.
- **Graceful shutdown:** the `Hub` tracks live sessions; SIGINT/SIGTERM calls
  `CloseAll` before the HTTP server shuts down.
- **RTCP drain:** a goroutine reads the sender's RTCP so the send buffer doesn't
  fill and stall the outbound track.
- **Session outlives the request:** model connect + `Serve` use a background
  context, so the media session isn't bound to the SDP HTTP request's lifetime.

### 4.9 Reconnect replay policy (best effort, bounded)  *(`internal/offer`, `cmd/replay`, `internal/sidechannel`)*

- **Protocol:** reconnect uses `X-Replay-Version: 1`, `X-Session-ID`,
  `X-Last-Seq`; server replies with `X-Replay-Status`.
- **Resolution:** `offer.ResolveReplay` validates headers and tries memory
  archive first, then optional replay-index (`-replay-url`).
- **Strict but non-blocking:** malformed `X-Last-Seq` / unsupported replay
  version returns `400`; missing id/seq simply falls back to a new session.
- **Provider scoped:** replay only when reconnect provider matches the original
  session/provider to avoid cross-model transcript contamination.
- **Order of sources:** memory archive first (same node), replay-index HTTP
  second; hard budget (`-replay-timeout`, default `300ms`) and bounded lines
  (`-replay-limit`, default `100`).
- **Seq invariant:** `transcript.Recorder` assigns seq once; side-channel `Tap`
  and data-channel JSON both reuse that seq (no independent counters).
- **Invariant preserved:** replay is control-plane best effort; timeout/error
  never blocks media startup, and cross-node replay is disabled when
  `-replay-url` is empty.

### 4.10 Provider guard  *(`internal/modelcb`, `internal/offer/intake.go`, `rtc/bridge.go`)*

- **Scope:** gates **new** dials on the offer path (`AllowDial` before
  `Models.New`). Established media sessions are unaffected once connected.
- **Policy:** fail with `503` when circuit is open/half-open gated, with
  `Retry-After`, `X-Model-CB-State`, `X-Model-CB-Reason`.
- **State machine:** `closed -> open -> half_open -> closed`; half-open allows
  a single probe request at a time per provider.
- **Error sensitivity:** auth-class dial failures (`401/403`,
  unauthorized/forbidden) open immediately with a longer hold
  (`-model-cb-auth-open-for`, default 5m). Non-auth dial failures open after
  `-model-cb-open-after` consecutive dial misses.
- **Recovery:** a successful dial (`RecordDial(nil)`) resets both dial and early
  stream failure streaks for that provider.
- **Early stream fault:** if the provider WebSocket connects but `Recv` errors
  before any audio within `modelcb.EarlyFaultWindow` (10s), `writeOutbound`
  reports via `StreamFaultAt` → `RecordStreamFault` — catches "connected but dead
  on arrival". Early stream failures are counted separately from dial failures.
- **Isolation:** breakers are per provider, with optional per-provider overrides.
  Loopback and a nil manager skip all guard logic.

### 4.11 Adaptive Opus complexity  *(`internal/adaptive`, `internal/audio/opus.go`)*

Encode CPU dominates per-session cost (~161µs/frame at default complexity). An
atomic complexity value is re-read each encode; controllers run off the media
path and can only mis-pick quality, never stall a session.

- **`sessions` (recommended):** proactive step function of active session count
  with hysteresis — sheds CPU before pacing slips, no feedback loop.
- **`drift` (experimental):** reactive on the fraction of frames ≥30ms late;
  tracks the real SLO but can oscillate under sustained load (same hazard as
  the reverted shared timing wheel).

## 5. Tests

- `internal/model/gemini`, `internal/model/doubao` — audio + transcript decode.
- `internal/model/cascade` — turn orchestration, barge-in, speculative partials,
  LLM intercept seam, output-mix seam (uses `fakestage/` stubs).
- `internal/offer` — session offer intake (rate limit, guard, model lifecycle) and
  reconnect replay resolution (table-driven).
- `internal/modelcb` — provider guard dial + early stream fault policy.
- `internal/transcript`, `internal/rtc` — recorder seq + listener notification.
- `internal/ratelimit` — at-max rejection, window reset (TTL was set),
  fail-open on unreachable Redis, disabled-limiter passthrough (uses miniredis).
- `internal/replayindex`, `internal/sidechannel` — replay-index client + store.
- `docs/bench/` — Opus micro-benchmark baselines and capacity sweeps (see
  [`docs/bench/README.md`](bench/README.md)).
