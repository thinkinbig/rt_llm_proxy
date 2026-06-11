# rt-llm-proxy

Real-time LLM proxy in Go. Browsers connect over **WebRTC**; the proxy
terminates the peer connection, decodes Opus audio, and bridges it to a
streaming model backend — managed STS APIs (Gemini, Doubao) or a self-hosted
cascade (ASR → LLM → TTS).
```
browser ──WebRTC(Opus + datachannel)──▶ proxy ──▶ gemini / doubao (WebSocket PCM)
                                              └──▶ cascade (HTTP stages)
        ◀──────────── Opus audio ────────────
```

No STUN/TURN/SFU is configured (`iceServers=[]`, host candidates only) — the
proxy is **not** NAT-traversal infrastructure. Rate limiting is optional and
lives purely on the control plane (the SDP offer endpoint).

## Quick start

| Goal | Command |
|---|---|
| Gemini Live (local) | `export GEMINI_API_KEY=...` → `go run ./cmd/proxy` → `http://localhost:8080/demo/` |
| Gemini Live (Docker) | `cp .env.example .env` → `docker compose up --build` |
| Self-hosted cascade | GPU host + `PUBLIC_IP` → see [Docker Compose § cascade](#docker-compose-cascade) → `?model=cascade` |
| Load test (no upstream) | `go run ./cmd/proxy` → `http://localhost:8080/demo/?model=loopback` |

Domain terms and module seams: [`CONTEXT.md`](CONTEXT.md), [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md#cascade) (cascade: §2).

## Providers

| `?model=` | Provider | Status |
|---|---|---|
| `gemini` (default) | Gemini Live (`BidiGenerateContent`) | working |
| `doubao` | Doubao 端到端实时语音 (Volcengine binary V3 WS) | working |
| `cascade` | Self-hosted ASR → LLM → TTS pipeline (see below) | working |
| `loopback` | fake provider (sine tone, no upstream) — load testing only | working |

## Prerequisites

- Go 1.25+
- libopus dev libraries (WebRTC Opus encode/decode via cgo):
  ```
  sudo apt-get install -y libopus-dev libopusfile-dev pkg-config
  ```
- (optional) Redis, for rate limiting

**Go module proxy:** default is `https://proxy.golang.org,direct` (abroad). If
`proxy.golang.org` is blocked or slow, use the China mirror:
`go env -w GOPROXY=https://goproxy.cn,direct`.

## Run

```
export GEMINI_API_KEY=...            # or GOOGLE_API_KEY
go run ./cmd/proxy -addr :8080
# open http://localhost:8080/demo/
```

Flags:

| flag | default | meaning |
|---|---|---|
| `-addr` | `:8080` | listen address |
| `-redis` | `` (off) | redis address for rate limiting |
| `-rl-max` | `10` | max sessions per client IP per window |
| `-rl-window` | `1m` | rate-limit window |
| `-sidechannel` | `off` | transcript side-channel: `off` \| `stdout` \| `kafka` |
| `-kafka` | `` | kafka seed brokers (csv) for `-sidechannel=kafka` |
| `-kafka-topic` | `transcripts` | kafka topic for transcript events |
| `-replay-url` | `` | replay-index service base URL (enables cross-node reconnect replay when set) |
| `-replay-timeout` | `300ms` | hard replay timeout budget (keep reconnect latency bounded) |
| `-replay-limit` | `100` | max replayed transcript lines per reconnect |
| `-model-cb` | `true` | circuit-break model connect attempts (`gemini` / `doubao`) |
| `-model-cb-open-after` | `5` | transient failures before opening breaker |
| `-model-cb-open-for` | `30s` | open duration for transient failures |
| `-model-cb-half-open-success` | `3` | successful half-open probes needed to close |
| `-model-cb-auth-open-for` | `5m` | open duration for auth failures (`401/403`) |
| `-admin` | `` (off) | admin listener for `/stats` (JSON) + `/debug/pprof` |
| `-opus-complexity` | `-1` | Opus encoder complexity 0–10 (-1 = libopus default; lower = less CPU) |
| `-adaptive` | `off` | adaptive complexity under load: `off` \| `sessions` (recommended) \| `drift` (reactive, can oscillate) |
| `-trust-proxy` | `false` | trust `X-Forwarded-For` for rate-limit client IP (only behind a reverse proxy that sets it) |
| `-cascade-whisper` | `ws://localhost:9000/...` | RealtimeSTT / faster-whisper WebSocket URL (`?model=cascade`) |
| `-cascade-llm` | `http://localhost:8000` | OpenAI-compatible LLM base URL (vLLM) |
| `-cascade-llm-model` | `Qwen3.5-9B` | model name served by vLLM |
| `-cascade-tts` | `http://localhost:8020` | XTTS streaming server base URL |
| `-cascade-tts-speaker` | `` | XTTS studio speaker (empty = first available) |
| `-cascade-tts-lang` | `en` | XTTS language code (`en`, `zh-cn`, …) |
| `-cascade-turndetect` | `` (off) | turn-detect sidecar URL (empty = fire LLM right after ASR final) |
| `-cascade-system` | `You are a helpful voice assistant.` | system prompt for cascade LLM |

Env:

- **Gemini** — `GEMINI_API_KEY` / `GOOGLE_API_KEY`, optional `GEMINI_MODEL`
  (default `models/gemini-3.1-flash-live-preview`). The model must support
  `bidiGenerateContent`; list the ones your key can use with:
  ```
  curl "https://generativelanguage.googleapis.com/v1beta/models?key=$GEMINI_API_KEY&pageSize=200" \
    | jq -r '.models[] | select(.supportedGenerationMethods[]?=="bidiGenerateContent").name'
  ```
- **Doubao** — `DOUBAO_APP_ID`, `DOUBAO_ACCESS_TOKEN` (开通豆包端到端实时语音大模型后获取),
  optional `DOUBAO_BOT_NAME`.

### Provider behavior via config file

Provider *behavior* (persona, voice, ASR tuning) is set in an optional YAML file,
loaded from `proxy.yaml` by default (override with `-config path`; a missing file
is skipped). Credentials and infrastructure flags are **not** in this file — keep
secrets in `.env` and tune infra with CLI flags. Copy the template to start:

```
cp proxy.yaml.example proxy.yaml
```

Precedence: an explicitly-set CLI flag wins, then the config file, then built-in
defaults. The provider-behavior fields below have no flags, so the file is their
only source:

| Section | Key | Effect |
|---|---|---|
| `gemini` | `system_prompt` | Live `systemInstruction` (persona without a dialogue turn) |
| `gemini` | `tools` | function-calling declarations (name/description/JSON-Schema parameters) |
| `doubao` | `model` | end-to-end version, required by the API (`1.2.1.1` O2.0 / `2.2.0.0` SC2.0; default `1.2.1.1`) |
| `doubao` | `system_role`, `speaking_style` | persona / tone (O-series) |
| `doubao` | `voice` | `tts.speaker` voice id |
| `doubao` | `asr.twopass`, `asr.end_smooth_ms`, `asr.hotwords` | ASR accuracy tuning (hotwords need `twopass: true`) |
| `cascade` | `system_prompt`, `tts_speaker`, `tts_lang`, `llm_model` | overrides matching `-cascade-*` flags when those flags are unset |

**Tool calling (Gemini Live 3.1).** Declare tools under `gemini.tools`; the proxy
declares them to the model and stays business-neutral. When the model calls a
tool, the proxy forwards `{"type":"tool_call","id","name","args"}` to the browser
over the data channel; the browser runs the function and replies
`{"type":"tool_result","id","name","response"}`, which the proxy returns to the
model. The demo ships a `get_weather` stub. (Doubao's direct protocol has no
native function calling — only the RTC-room "混合编排" path does, which this proxy
does not use.)

**Per-session listener brief.** The offer request may carry an `X-Listener-Brief`
header (base64 of UTF-8 text). It is appended to the provider's system prompt for
that session only — injected as **system instruction**, never as a dialogue turn,
so it cannot loop back into the transcript. Intended for an upstream orchestrator
to inject per-user memory (e.g. "this listener likes Jay Chou, is studying"); the
global persona stays in `proxy.yaml`, the per-user brief rides the header. Decoded
best-effort (bad/oversize header → ignored), capped at 8 KiB. Note: the brief is
trusted from the caller — in production the offer endpoint must be reachable only
by the orchestrator, not browsers (currently it is not locked down).

<a id="cascade"></a>

## Cascade pipeline (`?model=cascade`)

A self-hosted ASR → LLM → TTS pipeline that runs alongside the STS providers
and is selected per-session via `?model=cascade`. The bridge sees a normal
`Model` — same WebRTC path, transcript recorder, side-channel, and reconnect
semantics as `gemini` / `doubao`.

**Orchestrator vs sidecars:** turn orchestration lives **in-process** in
`internal/model/cascade` (history, barge-in, LLM→TTS pipeline, business seams).
Everything else is an **external sidecar** reached over HTTP/WebSocket — ASR,
LLM, TTS, and optional turn-detect. The proxy container hosts only the
orchestrator plus thin stage clients; see [ARCHITECTURE §2.1](docs/ARCHITECTURE.md#orchestrator-vs-sidecars).

```
browser ──WebRTC(Opus)──▶ proxy ──┬── orchestrator (in-process)
                                  ├──▶ realtimestt/     (ASR sidecar)
                                  ├──▶ vLLM             (LLM sidecar)
                                  ├──▶ xtts-streaming   (TTS sidecar)
                                  └──▶ turndetect/      (optional)
        ◀──Opus────────── proxy ◀── PCM from orchestrator
                            ▲
                     output-mix seam (inject real tracks here)
```

All model stages are **co-located on a single GPU host** (e.g. Volcano Engine
L20, 24 GB VRAM). The proxy ↔ stage hop is LAN-local (~1–5 ms), keeping the
intercept seams cheap enough for real-time use.

### Two business seams

The cascade exposes two seams that keep the core proxy business-agnostic while
enabling downstream use cases (e.g. a personalized real-time DJ):

**1. LLM intercept seam — `Config.OnLLMToken`**

Called per token before it reaches TTS. Return `("", false)` to pass through,
`(replacement, true)` to substitute, `("", true)` to drop silently.

```go
cascade.New(ctx, cascade.Config{
    OnLLMToken: func(token, accumulated string) (string, bool) {
        if id := detectSongIntent(accumulated + token); id != "" {
            go playTrack(id)       // trigger output-mix seam
            return "", true        // drop sentinel from TTS
        }
        return "", false
    },
})
```

**2. Output-mix seam — `Cascade.SetAudioSource`**

Injects any `AudioSource` into the outbound stream. `Recv()` reads from it
until `io.EOF`, then falls back to TTS audio automatically.

```go
type AudioSource interface {
    Read() ([]int16, error) // mono s16, 48kHz; return io.EOF when done
    Close() error
}

// Switch to a real track mid-session:
c.SetAudioSource(NewMP3Source("track-42.mp3"))
```

### Low-latency design

Three mechanisms stack to cut time-to-first-audio (TTFA):

1. **Speculative LLM start** — RealtimeSTT emits high-frequency partials. When
   a partial looks like a complete sentence (stable for ~200 ms, ends with
   punctuation), the cascade commits a tentative user turn and starts the LLM
   before the ASR final arrives. If the final matches, the in-flight generation
   continues; if not, speculation is discarded and a fresh turn starts.

2. **Sentence-segmented streaming TTS** — `respond()` splits the LLM token
   stream on sentence boundaries and synthesizes each segment while the LLM
   keeps generating the next. With XTTS streaming, playback begins before the
   sentence is fully rendered.

3. **Turn detection (optional)** — when `-cascade-turndetect` is set, the
   sidecar suggests a pause after each ASR final before firing the LLM, reducing
   premature replies on trailing-off speech. When unset, the LLM starts
   immediately after the final.

### Barge-in

When RealtimeSTT signals `speech_start` (user began speaking), the in-flight
LLM HTTP stream and TTS synthesis are cancelled via context cancellation.
`bargeIn()` waits for `respond()` to exit, then drains buffered audio — no race
between old and new turns.

Duplicate utterances (Jaccard token similarity ≥ 0.9) are ignored.

### Self-hosted sidecars

| Sidecar | Default in compose | Flag |
|---|---|---|
| ASR | `realtimestt/` — Silero VAD + faster-whisper (partials, finals, barge-in) | `-cascade-whisper` |
| LLM | [vLLM](https://github.com/vllm-project/vllm) OpenAI API, Qwen3.5-9B | `-cascade-llm`, `-cascade-llm-model` |
| TTS | [xtts-streaming-server](https://github.com/coqui-ai/xtts-streaming-server) | `-cascade-tts`, `-cascade-tts-speaker`, `-cascade-tts-lang` |
| Turn detect | `turndetect/` — sentence-finished classifier (optional) | `-cascade-turndetect` |

> On a single L20 (24 GB): RealtimeSTT (base + tiny) + Qwen3.5-9B + XTTS v2 +
> turndetect (CPU) fits with headroom when weights are mounted locally (no
> HuggingFace download at runtime for the LLM).

### Fault tolerance

- LLM and TTS HTTP calls retry **once** on transient failure, then fast-fail.
- Session reconnect is handled by the existing Kafka `session_id`+`seq` path
  (same as all other providers).
- **Single-point failure**: a GPU host crash ends all active cascade sessions.
  This is accepted for the current single-host deployment. For multi-host
  resilience, the three stages would need to be promoted to independent services
  with their own reconnect logic — out of scope here.

## Layout

```
cmd/proxy/          HTTP entrypoint, provider routing, rate-limit check, admin
cmd/loadgen/        pion load generator (pre-encoded Opus replay) for capacity tests
internal/rtc/       pion WebRTC bridge + session registry; SDP, Opus<->PCM, audio pump
internal/model/     Model seam (interface only)
internal/model/gemini/   Gemini Live adapter
internal/model/doubao/    Doubao realtime dialogue adapter
internal/model/cascade/   ASR→LLM→TTS cascade pipeline (two seams, barge-in)
internal/model/cascade/asr/   streaming Whisper ASR adapter
internal/model/cascade/llm/   OpenAI-compatible LLM adapter (vLLM)
internal/model/cascade/tts/   XTTS streaming TTS adapter
internal/model/cascade/turndetect/  optional turn-end sidecar
internal/model/cascade/fakestage/  network-free stubs for tests
internal/model/loopback/  fake provider (sine tone) for load testing
internal/model/pcm/      shared s16le serialize (uplink bytes)
internal/replayindex/     replay-index HTTP client + store (used by cmd/replay)
realtimestt/              RealtimeSTT sidecar (Docker)
turndetect/               turn-detect sidecar (Docker)
internal/audio/     Opus encode/decode (libopus) + linear resampler
internal/auth/      Authenticator seam (bearer -> user id, fail-open anonymous)
internal/sidechannel/    transcript tap -> Kafka (protobuf, off the media path)
cmd/replay/              replay-index: Kafka consumer + reconnect query API
internal/metrics/   lock-free frame-interval histogram (the pacing SLO)
internal/ratelimit/ Redis fixed-window limiter (atomic, fail-open)
demo/               minimal browser client
docs/               architecture & engineering notes
```

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md#cascade) for module seams, the
48kHz mono PCM audio contract, cascade orchestration (§2), and the rationale
behind each optimization.

<a id="docker-compose"></a>

## Docker Compose

Minimal stack: **proxy only** (no Redis, no Kafka — mirrors `go run` defaults).
Copy env and start:

```bash
cp .env.example .env   # set GEMINI_API_KEY
docker compose up --build          # Docker: goproxy.io (default)
# http://localhost:8080/demo/
```

Optional overlays (same `-f` pattern for each):

| Overlay | What it adds |
|---|---|
| `docker-compose.redis.yml` | Redis + SDP offer rate limiting (`-redis`) |
| `docker-compose.kafka.yml` | Kafka + transcript side-channel (`-sidechannel=kafka`) |
| `docker-compose.redis-kafka.yml` | Both (use this instead of stacking redis + kafka) |
| `docker-compose.cascade.yml` | Full cascade stack: RealtimeSTT + vLLM/Qwen + XTTS + turndetect + proxy |
| `docker-compose.cn.yml` | `goproxy.cn` for the image build |

<a id="docker-compose-cascade"></a>

```bash
docker compose -f docker-compose.yml -f docker-compose.redis.yml up --build
docker compose -f docker-compose.yml -f docker-compose.kafka.yml up --build

# Cascade (?model=cascade) — needs NVIDIA GPU + PUBLIC_IP for WebRTC
export PUBLIC_IP=<host reachable from browser>
export QWEN_MODEL_PATH=/path/to/Qwen3.5-9B   # optional; default mounts WebHarness cache
docker compose -f docker-compose.yml -f docker-compose.cascade.yml up --build
# open http://<PUBLIC_IP>:8080/demo/?model=cascade
```

Open **TCP 8080** and **UDP 10000–60000** on the host security group. Only the
proxy is exposed; ASR/LLM/TTS/turndetect stay on the internal Docker network.

**China** — `proxy.golang.org` / `goproxy.io` slow in Docker; add the CN overlay
or set `GOPROXY` in `.env`:

```bash
docker compose -f docker-compose.yml -f docker-compose.cn.yml up --build
# or: GOPROXY=https://goproxy.cn,direct docker compose up --build
```

<a id="scaling-failover"></a>

## Scaling & failover

This proxy is **single reachable host / vertical scaling** — it is not built for
Kubernetes horizontal scaling, and failover is partial by nature. The honest
picture, and what we already have toward it:

**Why it's hard (shared-nothing ≠ disaster recovery).** Shared-nothing buys
horizontal scale and fault isolation, not state recovery. A realtime session has
strong *state affinity*: the WebRTC/WebSocket connection lives on one pod's
kernel fds and its state (pion peer connection, provider WebSocket, conversation
context, encoder, jitter buffers) lives in that pod's memory. Another pod cannot
inherit the fd, so **seamless connection migration (L4) is impractical.** Media
is also UDP straight to the pod, and managed-cluster node IPs are private/NAT'd —
so `hostNetwork`/`replicas: N` don't make media work; that needs a TURN relay.

**Failover levels** (most realtime systems aim for L2–L3, not L4):

| Level | Goal | Status here |
|---|---|---|
| L1 | server dies → client reconnects, service stays up | reachable once replicated behind TURN |
| L2 | reconnect restores the *session* | **basic implementation** — demo client sends `X-Session-ID` + `X-Last-Seq`; proxy resumes same-node session and reuses the session id |
| L3 | reconnect resumes *progress* | **implemented** — same-node replay is in-memory, cross-node replay uses the replay-index service (`-replay-url`), and the model is re-seeded with restored context: cascade + gemini via the post-hoc `ContextRestorer` seam, doubao via `dialog.dialog_context` threaded into session construction |
| L4 | near-seamless connection migration | out of scope — fd/media affinity makes it impractical; see [ADR 0001](docs/adr/0001-l4-connection-migration-impractical.md) |

So it's **not "can't be done"** — the design already leans the right way: the
proxy is thin (state behind the `Model` seam), key events are externalized to a
replayable Kafka log keyed by `user_id` with a monotonic `seq`, and `session_id`
is server-minted. With `-replay-url` set, the proxy queries the replay-index
service for events after `last_seq`.
On reconnect the proxy re-seeds the freshly-dialed model with the restored
transcript, so the model resumes *informed* of the prior conversation rather
than amnesiac. Two injection points by provider lifecycle: cascade and gemini
restore mid-session via the post-hoc `model.ContextRestorer` seam (cascade owns
its history; gemini replays multi-turn `clientContent`); doubao takes context
only at session start, so its history is threaded into construction as
`dialog.dialog_context` (this is why `ResolveReplay` runs before the model is
dialed). The hard caveat:
this restores *transcript text*, not the provider's internal generation state,
so we cannot guarantee the model resumes mid-thought.

Reconnect protocol notes:

- Replay protocol version is `X-Replay-Version: 1`.
- Server returns `X-Replay-Status` as one of:
  `memory_hit`, `index_hit`, `index_timeout`, `index_error`, `miss`,
  `disabled`, `protocol_invalid`.
- Replay requires both `X-Session-ID` and `X-Last-Seq`; malformed
  `X-Last-Seq` or unsupported replay version returns `400`.
- Reconnect replay is provider-scoped (`gemini` sessions do not replay into
  `doubao`, and vice versa).
- Model circuit-breaker rejects open circuits with `503` plus
  `Retry-After`, `X-Model-CB-State`, and `X-Model-CB-Reason`.
- Per-provider overrides are available for both `gemini` and `doubao`:
  `-model-cb-*-gemini` / `-model-cb-*-doubao` (`0` means "use global default").

**For production scale today, front it with a mature media layer** — coturn
(TURN) for reachability and an SFU / realtime-agent framework (LiveKit, Pipecat)
for horizontal scale, session routing, and reconnect. Note it's 1:1
(browser↔LLM), so a pure SFU's selective forwarding isn't the need —
reachability + scale/routing is. Building an SFU/TURN into this repo was
deliberately **not** done. What scales unchanged today: Redis rate limiting
(shared across replicas) and the Kafka side-channel (off the media path).

## Notes / known limitations

- **Resampling is linear interpolation.** Fine for speech at our integer ratios
  (48k↔16k, 24k→48k); swap for a polyphase filter if quality matters.
- **Gemini WS field names are version-sensitive.** We send `realtimeInput.mediaChunks`;
  newer servers also accept `realtimeInput.audio`.
- **Doubao** uses Volcengine's binary V3 framing (`wss://openspeech.bytedance.com/api/v3/realtime/dialogue`);
  payloads are gzip'd, upstream PCM is 16kHz, downstream TTS is 24kHz.
- **Rate limiting fails open.** If Redis is unreachable the limiter allows the
  session (and logs the error) rather than rejecting it — a Redis blip won't take
  down the service. The counter's INCR+EXPIRE is one atomic Lua step, so a crash
  can't strand a key without a TTL.
