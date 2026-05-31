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

Proxy + Redis (SDP rate limiting). Copy env and start:

```bash
cp .env.example .env   # set GEMINI_API_KEY
docker compose up --build          # Docker: goproxy.io (default)
# http://localhost:8080/demo/
```

**China** — `proxy.golang.org` / `goproxy.io` slow in Docker; use the CN overlay
or set `GOPROXY` in `.env`:

```bash
docker compose -f docker-compose.yml -f docker-compose.cn.yml up --build
# or: GOPROXY=https://goproxy.cn,direct docker compose up --build
```

Without Redis:

```bash
docker compose -f docker-compose.yml -f docker-compose.no-redis.yml up --build
```

## Kubernetes (Helm)

Chart at `deploy/helm/rt-llm-proxy` — deploys the proxy plus an in-cluster **Redis**
for SDP offer rate limiting (`redis.enabled`, on by default).

> ⚠️ **WebRTC media will not traverse cluster NAT.** The proxy gathers host ICE
> candidates only (pod IP), and the Service exposes just the TCP signaling port —
> so the SDP exchange and demo page work, but audio never connects. This chart is
> intentionally kept free of `hostNetwork`/TURN workarounds; for working media run
> the container on a host the browser can reach directly (see Docker Compose).

```bash
# build image and load into Minikube
docker build -t rt-llm-proxy:latest .
minikube image load rt-llm-proxy:latest

helm upgrade --install rt-llm-proxy deploy/helm/rt-llm-proxy \
  --set gemini.apiKey="$GEMINI_API_KEY"

# demo UI (NodePort 30080 by default)
echo "http://$(minikube ip):30080/demo/"
```

Disable Redis: `--set redis.enabled=false` (proxy runs without `-redis`, same as local).

Use an existing Secret: `--set gemini.existingSecret=my-secret` (key `GEMINI_API_KEY`).

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
