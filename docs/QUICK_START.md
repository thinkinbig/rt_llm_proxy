# Quick Start Guide — rt-llm-proxy

Get rt-llm-proxy running in minutes.

## 5-Minute Setup (Gemini)

### Prerequisites
- Go 1.25+
- libopus dev libraries
- Gemini API key

### Installation

**1. Install dependencies**

```bash
# Ubuntu/Debian
sudo apt-get install -y libopus-dev libopusfile-dev pkg-config git

# macOS
brew install opus libopusfile pkg-config go
```

**2. Clone and configure**

```bash
git clone <repo>
cd rt_llm_proxy
export GEMINI_API_KEY=your_key_here
```

Get your key at https://aistudio.google.com/app/apikeys

**3. Run**

```bash
go run ./cmd/proxy -addr :8080
```

**4. Open browser**

http://localhost:8080/demo/

✅ Done! You're running a real-time voice AI.

---

## 10-Minute Setup (Docker)

**1. Copy environment**

```bash
cp .env.example .env
```

**2. Edit `.env`**

```bash
GEMINI_API_KEY=your_key_here
```

**3. Start**

```bash
docker compose up --build
```

**4. Open**

http://localhost:8080/demo/

---

## What Just Happened?

You deployed a **real-time voice LLM proxy**:

```
🌐 Browser (your voice via WebRTC)
    ↓
🖥️  Proxy (runs locally or in Docker)
    ↓
🤖 Gemini / Doubao / Self-hosted LLM
    ↓
🔊 Audio response back to you
```

The proxy handles:
- ✅ WebRTC audio encoding/decoding
- ✅ Real-time streaming to LLM
- ✅ Voice response generation
- ✅ Session management + reconnect

---

## Try Different Providers

### Doubao (豆包) — Chinese LLM

```bash
export DOUBAO_APP_ID=your_app_id
export DOUBAO_ACCESS_TOKEN=your_token
go run ./cmd/proxy
# Visit http://localhost:8080/demo/?model=doubao
```

### Self-Hosted Cascade (Requires GPU)

```bash
export PUBLIC_IP=your.public.ip
docker compose -f docker-compose.yml -f docker-compose.cascade.yml up --build
# Visit http://<PUBLIC_IP>:8080/demo/?model=cascade
```

This runs your own:
- **ASR** (speech-to-text) — Whisper
- **LLM** (language model) — Qwen
- **TTS** (text-to-speech) — XTTS

### Loopback (Testing, No API Key)

```bash
go run ./cmd/proxy -addr :8080
# Visit http://localhost:8080/demo/?model=loopback
# No internet required, generates fake audio
```

---

## Enable Rate Limiting (with Redis)

Limit sessions per IP:

```bash
docker compose -f docker-compose.yml -f docker-compose.redis.yml up --build
```

Then configure:
```bash
go run ./cmd/proxy \
  -redis localhost:6379 \
  -rl-max 10 \
  -rl-window 1m
```

---

## Enable Transcript Logging (with Kafka)

Save all transcripts to Kafka:

```bash
docker compose -f docker-compose.yml -f docker-compose.kafka.yml up --build
```

Then consume:
```bash
docker compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic transcripts
```

---

## Monitor Performance

```bash
# View stats
curl http://localhost:6060/stats | jq

# Expected output
{
  "sessions": 5,
  "frames_total": 125000,
  "frames_late_30ms": 150,
  "bytes_in": 1024000,
  "bytes_out": 2048000
}
```

---

## Troubleshooting

### "Failed to connect" in browser

```bash
# Is proxy running?
curl http://localhost:8080/stats

# Is API key set?
echo $GEMINI_API_KEY

# Are WebRTC ports open?
sudo ufw allow 10000:60000/udp
```

### High latency or frame drops

```bash
# Enable adaptive Opus complexity
go run ./cmd/proxy -adaptive sessions
```

### Docker build slow (China)

```bash
docker compose -f docker-compose.yml -f docker-compose.cn.yml up --build
```

---

## Next Steps

- 📖 [Full Guide](中文指南.md) — Architecture, configuration, features
- 🚀 [Deployment Guide](DEPLOYMENT.md) — Production setup, scaling
- ⚡ [Performance](bench/README.md) — Benchmarks and optimization
- ❓ [FAQ](FAQ.md) — Common questions and solutions

---

## Command Reference

| What | Command |
|---|---|
| Basic | `go run ./cmd/proxy` |
| With admin panel | `go run ./cmd/proxy -admin :6060` |
| With Redis rate limit | `go run ./cmd/proxy -redis localhost:6379` |
| Adaptive complexity | `go run ./cmd/proxy -adaptive sessions` |
| Custom port | `go run ./cmd/proxy -addr :9000` |

---

**Ready?** Start the demo and speak to your AI! 🎙️

