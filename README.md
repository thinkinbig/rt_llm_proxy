# rt-llm-proxy

Real-time LLM proxy in Go. Browsers connect over **WebRTC**; the proxy
terminates the peer connection, decodes the Opus audio, and bridges it to a
streaming LLM provider's WebSocket API (and back).
```
browser ──WebRTC(Opus audio + datachannel)──▶ proxy ──WebSocket(PCM)──▶ Gemini / Doubao
        ◀──────────── Opus audio ────────────         ◀──── PCM ──────
```

No STUN/TURN/SFU is configured (`iceServers=[]`, host candidates only) — the
proxy is **not** NAT-traversal infrastructure. Rate limiting is optional and
lives purely on the control plane (the SDP offer endpoint).

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

Env:

- **Gemini** — `GEMINI_API_KEY` / `GOOGLE_API_KEY`, optional `GEMINI_MODEL`
  (default `models/gemini-2.5-flash-native-audio-latest`). The model must support
  `bidiGenerateContent`; list the ones your key can use with:
  ```
  curl "https://generativelanguage.googleapis.com/v1beta/models?key=$GEMINI_API_KEY&pageSize=200" \
    | jq -r '.models[] | select(.supportedGenerationMethods[]?=="bidiGenerateContent").name'
  ```
- **Doubao** — `DOUBAO_APP_ID`, `DOUBAO_ACCESS_TOKEN` (开通豆包端到端实时语音大模型后获取),
  optional `DOUBAO_BOT_NAME`.

## Cascade pipeline (`?model=cascade`)

A self-hosted, three-stage ASR → LLM → TTS pipeline that runs alongside the
STS providers and is selected per-session via `?model=cascade`.

```
browser ──WebRTC(Opus)──▶ proxy ──PCM──▶ Whisper-Base (ASR)
                                              │ transcript
                                              ▼
                                    vLLM / DeepSeek-R1-7B  ← LLM intercept seam
                                              │ token stream
                                              ▼
                                      Coqui TTS FastPitch
                                              │ PCM
        ◀──Opus────────── proxy ◀─────────────┘
                            ▲
                     output-mix seam (inject real tracks here)
```

All three services are **co-located on a single GPU host** (Volcano Engine L20,
24 GB VRAM). The proxy ↔ model hop is LAN-local (~1–5 ms), making the two
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

### Low-latency design: quick/final two-phase TTS

Rather than waiting for the full LLM reply before starting TTS, the cascade
synthesizes the **first sentence** (`quick`) as soon as the LLM produces it,
then synthesizes the **remainder** (`final`) while the quick audio is playing.
This cuts time-to-first-audio (TTFA) to roughly one sentence worth of LLM
latency instead of a full reply.

### Barge-in

When the ASR detects the user starting to speak (`ASRSpeechStarted`), the
in-flight LLM HTTP stream and TTS synthesis are cancelled immediately via
context cancellation. The cancel sequence waits for `respond()` to exit before
starting the new turn, preventing any race between old and new audio.

Duplicate utterances (Jaccard token similarity ≥ 0.9) are ignored — the
bot will not restart a reply it is already giving for essentially the same input.

### Self-hosted services required

| Service | Recommended | Env / flag |
|---|---|---|
| ASR | [faster-whisper-server](https://github.com/fedirz/faster-whisper-server) (Whisper Base) | `-whisper-url ws://localhost:9000/...` |
| LLM | [vLLM](https://github.com/vllm-project/vllm) serving DeepSeek-R1-7B INT8 | `-llm-url http://localhost:8000` |
| TTS | [Coqui TTS](https://github.com/coqui-ai/TTS) server (FastPitch) | `-tts-url http://localhost:5002` |

> All three fit on a single L20 (24 GB): Whisper-Base ~1.5 GB + DeepSeek-R1-7B
> INT8 ~7 GB + Coqui TTS ~2 GB + overhead ≈ 13 GB total.

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
internal/model/cascade/fakestage/  network-free stubs for tests
internal/model/loopback/  fake provider (sine tone) for load testing
internal/model/pcm/      shared s16le serialize (uplink bytes)
internal/audio/     Opus encode/decode (libopus) + linear resampler
internal/auth/      Authenticator seam (bearer -> user id, fail-open anonymous)
internal/sidechannel/    transcript tap -> Kafka (protobuf, off the media path)
cmd/replay/              replay-index: Kafka consumer + reconnect query API
internal/metrics/   lock-free frame-interval histogram (the pacing SLO)
internal/ratelimit/ Redis fixed-window limiter (atomic, fail-open)
demo/               minimal browser client
docs/               architecture & engineering notes
```

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the module seams, the
48kHz mono PCM audio contract, and the rationale behind each optimization.

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
| `docker-compose.cn.yml` | `goproxy.cn` for the image build |

```bash
docker compose -f docker-compose.yml -f docker-compose.redis.yml up --build
docker compose -f docker-compose.yml -f docker-compose.kafka.yml up --build
```

**China** — `proxy.golang.org` / `goproxy.io` slow in Docker; add the CN overlay
or set `GOPROXY` in `.env`:

```bash
docker compose -f docker-compose.yml -f docker-compose.cn.yml up --build
# or: GOPROXY=https://goproxy.cn,direct docker compose up --build
```

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
| L3 | reconnect resumes *progress* | **partial** — same-node replay is in-memory; cross-node replay uses the replay-index service (`-replay-url`) |
| L4 | near-seamless connection migration | impractical (fd/media affinity) — don't chase it |

So it's **not "can't be done"** — the design already leans the right way: the
proxy is thin (state behind the `Model` seam), key events are externalized to a
replayable Kafka log keyed by `user_id` with a monotonic `seq`, and `session_id`
is server-minted. With `-replay-url` set, the proxy queries the replay-index
service for events after `last_seq`.
The hard caveat:
the *provider's* dialogue context (Gemini/Doubao) lives in their server-side
socket, so we can restore our session metadata and transcribed text, but cannot
guarantee the upstream model resumes mid-thought.

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
