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

## Layout

```
cmd/proxy/          HTTP entrypoint, provider routing, rate-limit check, admin
cmd/loadgen/        pion load generator (pre-encoded Opus replay) for capacity tests
internal/rtc/       pion WebRTC bridge + session registry; SDP, Opus<->PCM, audio pump
internal/model/     Model seam (interface only)
internal/model/gemini/   Gemini Live adapter
internal/model/doubao/    Doubao realtime dialogue adapter
internal/model/loopback/  fake provider (sine tone) for load testing
internal/model/pcm/      shared s16le serialize (uplink bytes)
internal/audio/     Opus encode/decode (libopus) + linear resampler
internal/auth/      Authenticator seam (bearer -> user id, fail-open anonymous)
internal/sidechannel/    transcript tap -> Kafka (protobuf, off the media path)
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
| L2 | reconnect restores the *session* | **half-built** — server-minted `session_id` exists; needs a client reconnect protocol + server-side session lookup |
| L3 | reconnect resumes *progress* | **half-built** — the Kafka side-channel is a replayable event log with a per-session `seq`; a new pod could resume from `session_id`+`last_seq` |
| L4 | near-seamless connection migration | impractical (fd/media affinity) — don't chase it |

So it's **not "can't be done"** — the design already leans the right way: the
proxy is thin (state behind the `Model` seam), key events are externalized to a
replayable Kafka log keyed by `user_id` with a monotonic `seq`, and `session_id`
is server-minted. What's missing for L2/L3 is a **client reconnect protocol** and
**server-side context restore from `session_id`+`last_seq`**. The hard caveat:
the *provider's* dialogue context (Gemini/Doubao) lives in their server-side
socket, so we can restore our session metadata and transcribed text, but cannot
guarantee the upstream model resumes mid-thought.

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
