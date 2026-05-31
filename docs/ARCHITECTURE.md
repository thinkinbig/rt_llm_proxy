# Architecture & Engineering Notes

How rt-llm-proxy is put together, and **why** each non-obvious engineering
decision is the way it is. Kept deliberately small — this is a small project.

## 1. Data flow

```
browser ──WebRTC(Opus audio + datachannel)──▶ proxy ──WebSocket(PCM)──▶ Gemini / Doubao
        ◀──────────── Opus audio ────────────         ◀──── PCM ──────
```

- **Inbound (mic → model):** `track.ReadRTP` → Opus decode → mono s16 PCM @48kHz
  → `Model.SendAudio`. The provider adapter resamples to its own wire rate.
- **Outbound (model → speaker):** `Model.Recv` → accumulate into a buffer →
  Opus-encode each 20ms / 960-sample frame → `WriteSample`, **paced at real time**.
- **Data channel:** browser text → `Model.SendText`; model transcripts
  (`RecvText`) → browser, as `role: text` lines.

## 2. Modules & seams

| Module | Package | Role |
|---|---|---|
| **Bridge** | `internal/rtc` | Terminates one browser WebRTC peer connection; pumps audio + data-channel text both ways. Talks **only** to the Model seam. |
| **Model seam** | `internal/model` | The provider-agnostic `Model` interface (`SendAudio`/`SendText`/`Recv`/`Close`). The Bridge depends on this and nothing provider-specific. |
| **Providers / adapters** | `internal/model/gemini`, `internal/model/doubao` | One concrete `Model` per streaming LLM. Each owns its WebSocket protocol and native audio format. Optional `transcriber` (`RecvText`) surfaces STT. |
| **PCM helpers** | `internal/model/pcm` | `ToBytes` / `FromBytes` — s16le serialize for the uplink. Shared only because both adapters serialize contract-side s16; **not** a unified decode layer. |
| **Audio** | `internal/audio` | Opus encode/decode (libopus via cgo) + linear resampler. |
| **Rate limit** | `internal/ratelimit` | Redis fixed-window limiter for the SDP offer endpoint. **Control plane only.** |

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

## 3. Engineering optimization points

Each entry: what we do, and the failure mode it avoids.

### 3.1 Real-time outbound pacing without clock drift  *(`rtc/bridge.go`, `writeOutbound`)*

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

### 3.2 Atomic rate limiting + fail-open  *(`internal/ratelimit`)*

- **Atomic INCR+EXPIRE via a Lua script.** A separate `INCR` then `EXPIRE` has a
  crash window: dying between the two leaves the key with **no TTL**, so the
  counter never resets and that IP is **locked out permanently**. The Lua script
  makes both one atomic step.
- **Fail open on Redis errors.** Rate limiting is a soft guard on the control
  plane; a Redis blip should not take down the real-time service. On error
  `Allow` returns `true` and surfaces the error for logging only.

### 3.3 Redis stays strictly on the control plane

Redis touches **only** the SDP offer endpoint (session-creation rate). The media
path (Opus ↔ PCM ↔ provider) never makes a network round-trip to Redis —
routing 20ms audio frames through Redis would add latency and defeat the point
of a real-time proxy. This is an invariant, not an accident.

### 3.4 Shared pion API / MediaEngine  *(`rtc.Hub`)*

The `Hub` builds the pion `API` (with an Opus-tuned `MediaEngine` + default
interceptors) **once** and reuses it for every peer connection, rather than
rebuilding codec/interceptor state per session.

### 3.5 Opus tuning for lossy/quiet links  *(`audio/opus.go`, `rtc/bridge.go`)*

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
Frames are 20ms / 960 samples @ 48kHz, paced by the §3.1 Ticker.

### 3.6 Non-trickle ICE, host candidates only  *(`rtc/bridge.go`, `Serve`)*

`Serve` waits on `GatheringCompletePromise` and returns the **full** answer SDP
with candidates (non-trickle). No STUN/TURN/SFU (`iceServers=[]`). The proxy is
intentionally **not** NAT-traversal infrastructure — simpler signaling, fewer
moving parts. Tradeoff: media won't traverse cluster NAT; horizontal scale and
failover are partial by design (L1–L4 — see README's "Scaling & failover").
Run the container on a host the browser can reach directly.

### 3.7 Linear resampling at integer ratios  *(`audio/resample.go`)*

We use linear interpolation. At our integer ratios (48k↔16k, 24k→48k) output
length is exact and per-chunk boundaries line up, so artifacts are minimal —
good enough for speech. Swap for a polyphase filter if quality ever matters.

### 3.8 Lifecycle & backpressure  *(`rtc/bridge.go`, `cmd/proxy/main.go`)*

- **Idempotent teardown:** `session.cleanup` runs under a `sync.Once`; connection
  state changes, model EOF, and hub shutdown all funnel through it safely.
- **Graceful shutdown:** the `Hub` tracks live sessions; SIGINT/SIGTERM calls
  `CloseAll` before the HTTP server shuts down.
- **RTCP drain:** a goroutine reads the sender's RTCP so the send buffer doesn't
  fill and stall the outbound track.
- **Session outlives the request:** model connect + `Serve` use a background
  context, so the media session isn't bound to the SDP HTTP request's lifetime.

## 4. Tests

- `internal/model/gemini`, `internal/model/doubao` — audio + transcript decode.
- `internal/ratelimit` — at-max rejection, window reset (TTL was set),
  fail-open on unreachable Redis, disabled-limiter passthrough (uses miniredis).
