# Deployment Guide — rt-llm-proxy

Complete guide to deploying rt-llm-proxy in production and at scale.

## Local Development

### Prerequisites

```bash
# Go 1.25+
go version

# libopus development libraries
sudo apt-get install -y libopus-dev libopusfile-dev pkg-config
# or on macOS:
brew install opus libopusfile pkg-config
```

### Configure Go Proxy (China)

```bash
go env -w GOPROXY=https://goproxy.cn,direct
```

### Run Locally

```bash
export GEMINI_API_KEY=your_key_here
go run ./cmd/proxy -addr :8080

# In another terminal:
curl http://localhost:8080/stats
```

### Debug Mode

```bash
go run ./cmd/proxy \
  -addr :8080 \
  -admin :6060 \
  -sidechannel stdout

# View stats
curl http://localhost:6060/stats | jq

# CPU profile
go tool pprof http://localhost:6060/debug/pprof/profile?seconds=30
```

---

## Docker Compose

### Base Stack (Proxy Only)

```bash
cp .env.example .env
# Edit: GEMINI_API_KEY=...
docker compose up --build
# http://localhost:8080/demo/
```

### With Redis (Rate Limiting)

```bash
docker compose -f docker-compose.yml \
               -f docker-compose.redis.yml \
               up --build
```

Enables per-IP rate limiting:
- `-rl-max 10` — max 10 sessions per IP per window
- `-rl-window 1m` — window duration

### With Kafka (Transcript Archival)

```bash
docker compose -f docker-compose.yml \
               -f docker-compose.kafka.yml \
               up --build
```

Transcripts are published to `transcripts` topic (protobuf format).

Consume:
```bash
docker compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic transcripts
```

### With Both Redis + Kafka

```bash
docker compose -f docker-compose.yml \
               -f docker-compose.redis-kafka.yml \
               up --build
```

### Cascade (Self-Hosted ASR/LLM/TTS)

**Prerequisites:**
- NVIDIA GPU (recommend L20 24GB)
- Public IP (for WebRTC)
- ~100GB disk (for models)

**Deploy:**

```bash
export PUBLIC_IP=your.public.ip

# Optional: pre-download model
export QWEN_MODEL_PATH=/path/to/Qwen3.5-9B

docker compose -f docker-compose.yml \
               -f docker-compose.cascade.yml \
               up --build
```

**Access:**
```
http://<PUBLIC_IP>:8080/demo/?model=cascade
```

**Open ports:**
- TCP 8080 (HTTP proxy)
- UDP 10000-60000 (WebRTC media)

**Sidecar containers:**
- `realtimestt` — ASR (port 9000)
- `vllm` — LLM (port 8000)
- `xtts` — TTS (port 8020)
- `turndetect` — Turn detection (port 5002, optional)

All sidecars stay on internal Docker network; only proxy port 8080 exposed.

---

## China-Specific Setup

### Go Proxy

Set in `.env`:
```bash
GOPROXY=https://goproxy.cn,direct
GOSUMDB=off
```

Or use the CN overlay:
```bash
docker compose -f docker-compose.yml -f docker-compose.cn.yml up --build
```

### Doubao (豆包) Provider

Volcengine's native realtime API — no VPN needed, hosted in China.

```bash
export DOUBAO_APP_ID=your_app_id
export DOUBAO_ACCESS_TOKEN=your_token

go run ./cmd/proxy
# http://localhost:8080/demo/?model=doubao
```

### Model Pre-caching

Avoid HuggingFace downloads at runtime:

```bash
# Pre-download Qwen locally
git clone https://huggingface.co/Qwen/Qwen3.5-9B /path/to/model

export QWEN_MODEL_PATH=/path/to/model
docker compose -f docker-compose.yml -f docker-compose.cascade.yml up --build
```

---

## Configuration Flags

### Essential Flags

```
-addr           :8080              Listen address
-model          gemini             Model: gemini | doubao | cascade | loopback
-redis          ""                 Redis address (enables rate limit)
-rl-max         10                 Sessions per IP per window
-rl-window      1m                 Rate limit window
-sidechannel    off                Transcript output: off | stdout | kafka
-kafka          localhost:9092     Kafka brokers (csv)
-admin          ""                 Admin listener for /stats and pprof
```

### Cascade-Specific Flags

```
-cascade-whisper          ws://localhost:9000/... ASR WebSocket URL
-cascade-llm              http://localhost:8000   LLM base URL
-cascade-llm-model        Qwen3.5-9B              Model name
-cascade-tts              http://localhost:8020   TTS URL
-cascade-tts-speaker      ""                      XTTS speaker
-cascade-tts-lang         en                      Language (en, zh-cn, etc)
-cascade-turndetect       ""                      Turn detection URL (optional)
-cascade-system           <prompt>                System prompt for LLM
```

### Opus Tuning

```
-opus-complexity         -1                Complexity 0-10 (-1 = default)
-adaptive                off               Adaptive: off | sessions | drift
-trust-proxy             false             Trust X-Forwarded-For header
```

### Model Circuit Breaker

```
-model-cb               true              Enable circuit breaker
-model-cb-open-after    5                 Failures before opening
-model-cb-open-for      30s               Open duration
-model-cb-auth-open-for 5m                Auth failure duration
```

### Reconnect / Replay

```
-replay-url             ""                Replay-index service URL
-replay-timeout         300ms             Replay lookup timeout
-replay-limit           100               Max replayed transcript lines
```

---

## Monitoring & Operations

### Admin Endpoint

Enable with `-admin :6060`:

```bash
# Stats (JSON)
curl http://localhost:6060/stats | jq

# Goroutines
curl http://localhost:6060/debug/pprof/goroutine?debug=1 | head -20

# Heap dump
curl http://localhost:6060/debug/pprof/heap > heap.prof
go tool pprof heap.prof

# CPU profile
go tool pprof http://localhost:6060/debug/pprof/profile?seconds=30
```

### Key Metrics

| Metric | Alert if > |
|---|---|
| `frames_late_30ms` | 5% of total |
| `sessions` | Capacity limit |
| `memory_bytes` | Baseline × 2 |
| `goroutines` | Baseline × 3 |

### Logs

```bash
# Stdout logging
docker compose logs -f rt-llm-proxy

# Search for errors
docker compose logs rt-llm-proxy | grep -i error

# Specific provider
docker compose logs rt-llm-proxy | grep gemini
```

### Kafka Monitoring

```bash
# Consumer lag
docker compose exec kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --list

# Topic size
docker compose exec kafka du -sh /var/lib/kafka/data/
```

---

## Troubleshooting

### WebRTC Connection Failed

**Check:**
1. Firewall opens UDP 10000-60000?
   ```bash
   sudo ufw allow 10000:60000/udp
   ```
2. Is proxy running?
   ```bash
   curl http://localhost:6060/stats
   ```
3. Are logs clean?
   ```bash
   docker compose logs rt-llm-proxy | tail -20
   ```

### High Latency / Frame Drops

1. Check SLO metric:
   ```bash
   curl http://localhost:6060/stats | jq '.frames_late_30ms'
   ```

2. Enable adaptive complexity:
   ```bash
   go run ./cmd/proxy -adaptive sessions
   ```

3. Lower Opus quality:
   ```bash
   go run ./cmd/proxy -opus-complexity 5
   ```

### Cascade Sidecars Not Responding

```bash
# Check all containers running
docker compose ps

# Test connectivity
docker compose exec rt-llm-proxy \
  curl http://realtimestt:9000/health

# Check sidecar logs
docker compose logs realtimestt -n 50
docker compose logs vllm -n 50
docker compose logs xtts -n 50
```

### Memory Leak

1. Check goroutine count:
   ```bash
   curl http://localhost:6060/debug/pprof/goroutine?debug=1 | wc -l
   ```
   Should stay constant after sessions close.

2. Check heap profile:
   ```bash
   go tool pprof http://localhost:6060/debug/pprof/heap
   > top10
   ```

3. Check for Kafka backlog (if sidechannel enabled):
   ```bash
   docker compose exec kafka kafka-consumer-groups.sh \
     --bootstrap-server localhost:9092 \
     --group transcripts --describe
   ```

### Capacity / Scaling Issues

**Single-host ceiling ~600–1000 concurrent sessions:**
- Limited by Opus encode CPU
- Check baseline: `docs/bench/README.md`
- Scale up: use `-adaptive sessions` to shed load gracefully

**For horizontal scale (Kubernetes, multi-host):**
- This proxy is **not** designed for K8s without external SFU
- Recommend: front with LiveKit / Pipecat SFU for media routing
- Proxy state is thin (sessions are ephemeral) so stateless is OK

---

## Production Checklist

- [ ] **Secrets**: API keys in secure vault, not `.env`
- [ ] **TLS**: Reverse proxy terminates HTTPS
- [ ] **Rate limiting**: Redis configured, `-rl-max` set
- [ ] **Transcripts**: Kafka enabled for audit trail
- [ ] **Monitoring**: Admin endpoint secured, metrics scraped
- [ ] **Logs**: Centralized (ELK, Loki, CloudWatch)
- [ ] **Alerts**: Set on frame latency, error rates, goroutine leaks
- [ ] **Backups**: Kafka retention configured
- [ ] **Failover**: Multi-node with TURN/SFU for media routing
- [ ] **Security**: Auth tokens validated at reverse proxy
- [ ] **Capacity**: Load tested; scaling strategy documented

---

## Performance Tuning

### Opus Complexity vs CPU

| Complexity | CPU / frame | ~sessions/core |
|---|---|---|
| 10 (default) | ~166µs | 107 |
| 5 | ~79µs | 200 |
| 3 | ~60µs | 270 |
| 0 | ~35µs | 330 |

**Recommendation**: Use `-adaptive sessions` — automatically steps down under load.

### Memory & GC

Default Go GC tuning:
```bash
# Reduce GC frequency for lower latency variance
export GOGC=200
go run ./cmd/proxy
```

### Network Optimization

For cascade on GPU host:
- **RTT to ASR/LLM/TTS sidecars**: ~1–5ms (LAN)
- **RTT to browser**: ~10–50ms (depends on internet)
- Keep sidecars co-located to minimize cascade RTT

---

## Example: Complete Production Stack

```bash
# 1. Start all services
docker compose -f docker-compose.yml \
               -f docker-compose.redis-kafka.yml \
               up -d

# 2. Verify health
curl http://localhost:6060/stats | jq '.sessions'

# 3. Set up monitoring
prometheus scrape_interval: 10s
  - targets: ['localhost:6060/stats']

# 4. Set up alerts
- alert: HighFrameLatency
  expr: frames_late_30ms > 5%
  for: 5m
  action: page oncall

# 5. Logs to centralized system
docker compose logs -f | ship-to-loki.sh

# 6. Cascade model auto-update (optional)
# Cron job to pull latest Qwen weekly
```

---

## Glossary

| Term | Meaning |
|---|---|
| **Bridge** | WebRTC endpoint, connects browser to model |
| **Model** | Provider adapter (Gemini, Doubao, Cascade) |
| **Cascade** | Self-hosted ASR→LLM→TTS pipeline |
| **Sidecar** | External service (ASR, LLM, TTS) for cascade |
| **Replay** | Transcript restoration on reconnect |
| **SLO** | Service level objective (e.g., <5% frames ≥30ms late) |

---

## Related Documentation

- [Quick Start](QUICK_START.md) — 5-minute setup
- [Architecture](ARCHITECTURE.md) — Deep design rationale
- [Benchmarks](bench/README.md) — Performance data
- [FAQ](FAQ.md) — Common questions
- [Chinese Guide](中文指南.md) — 中文版本

