# FAQ — rt-llm-proxy

Frequently asked questions and answers.

## What is rt-llm-proxy?

A **real-time LLM proxy** written in Go. Browsers connect via WebRTC, the proxy bridges to a streaming LLM (Gemini, Doubao, or self-hosted Qwen/LLMs), and streams back voice responses in real time.

```
🌐 Browser (WebRTC voice)
    ↓
🖥️  Proxy (this project)
    ↓
🤖 LLM (Gemini / Doubao / Cascade)
    ↓
🔊 Voice response
```

---

## Which LLMs does it support?

| Provider | Type | Setup | Cost |
|---|---|---|---|
| **Gemini** | Cloud API | API key | Per API call |
| **Doubao** | Cloud API | App ID + token | Per API call |
| **Cascade** | Self-hosted | GPU + Docker | Hardware only |
| **Loopback** | Mock | None | Free (testing) |

**Pick Gemini** if you want: simplicity, quality, managed by Google.
**Pick Doubao** if you're in China or need native Chinese.
**Pick Cascade** if you want: full control, custom models, no cloud costs.

---

## What is Cascade?

A self-hosted **ASR → LLM → TTS** pipeline:

- **ASR** (speech-to-text): RealtimeSTT (Silero VAD + Whisper)
- **LLM** (language model): vLLM running Qwen or other open models
- **TTS** (text-to-speech): XTTS streaming synthesizer

**Why?**
- Complete control over the pipeline
- No cloud API bills (just hardware)
- Can inject custom outputs (e.g., play real music)
- Lowest latency (all on LAN)

**Downsides:**
- Requires NVIDIA GPU (L20 or better, 24GB VRAM)
- Takes ~30min to deploy (models + containers)
- Slower responses (LLMs are slow without optimization)

---

## Can I run it on CPU?

**Yes, but not Cascade:**
- ✅ Gemini / Doubao — works on any machine (no GPU needed)
- ❌ Cascade — requires GPU; LLM inference on CPU is not practical

For development, use **Loopback** (no internet, fake responses, instant).

---

## What's the latency?

End-to-end **200–400ms** typical:

| Component | Latency |
|---|---|
| WebRTC setup | ~100ms |
| ASR (Whisper) | ~100–200ms |
| LLM first token (Qwen) | ~200–500ms |
| TTS synthesis (XTTS) | ~100–200ms |
| Network roundtrips | ~10–50ms |

**Cascade is fastest** (local LAN + self-hosted = no cloud RTT).

---

## How many concurrent users?

**Single 16-core box:**
- Gemini / Doubao: ~600–1000 sessions
- Cascade: ~50–100 sessions (limited by GPU VRAM)

See benchmarks: `docs/bench/README.md`

**To scale beyond:**
- Vertical: bigger machine (more CPU / GPU)
- Horizontal: multiple proxies + TURN/SFU frontend (LiveKit / Pipecat)

This proxy is **single-host only** by design — shared-nothing media routing is hard.

---

## How do I get started?

**Fastest way (5 min):**

```bash
export GEMINI_API_KEY=your_key
go run ./cmd/proxy -addr :8080
# http://localhost:8080/demo/
```

See [Quick Start](QUICK_START.md).

**With Docker:**

```bash
cp .env.example .env  # Edit: GEMINI_API_KEY=...
docker compose up --build
# http://localhost:8080/demo/
```

---

## How do I use it in production?

1. **Front with a reverse proxy** (Nginx) — TLS termination, load balancing
2. **Enable Redis** — rate limiting, shared state across replicas
3. **Enable Kafka** — transcript archival, audit log
4. **Monitor** — CPU, latency, error rates
5. **Add TURN/SFU** (LiveKit / Pipecat) — NAT traversal, horizontal scaling

See [Deployment Guide](DEPLOYMENT.md#production-checklist).

---

## How do I limit requests per user?

Use Redis rate limiting:

```bash
go run ./cmd/proxy \
  -redis localhost:6379 \
  -rl-max 10 \
  -rl-window 1m
```

This allows max 10 **new sessions** per IP per minute. Existing sessions are unlimited.

**Note:** If Redis is down, the proxy allows all requests (fail-open design).

---

## Can I customize the LLM prompt?

**Cascade only** (self-hosted):

```bash
go run ./cmd/proxy \
  -cascade-system "You are a helpful dance instructor."
```

Gemini / Doubao prompts are set by the cloud provider.

---

## Can I inject my own audio (e.g., music)?

**Cascade only** — use the **output-mix seam**:

```go
cascade.New(ctx, cascade.Config{
    OnLLMToken: func(token, accumulated string) (string, bool) {
        // Detect if user asked for a song
        if songID := detectSongIntent(accumulated + token); songID != "" {
            go playTrack(songID)  // Start external playback
            return "", true       // Drop token from TTS
        }
        return "", false  // Pass through normally
    },
})

// Later, switch the audio source
c.SetAudioSource(NewMP3Source("my-track.mp3"))
```

Great for a **real-time DJ** use case.

---

## How do I save transcripts?

Three ways:

1. **Stdout (dev)**:
   ```bash
   go run ./cmd/proxy -sidechannel stdout
   ```

2. **Kafka (production)**:
   ```bash
   docker compose -f docker-compose.yml -f docker-compose.kafka.yml up
   # Transcripts → Kafka topic "transcripts"
   ```

3. **Data channel (browser)**:
   Demo page shows live transcripts in UI.

---

## Why is performance degrading?

Check **frame latency SLO** (target: <5% of frames ≥30ms late):

```bash
curl http://localhost:6060/stats | jq '.frames_late_30ms'
```

If high, enable **adaptive Opus complexity**:

```bash
go run ./cmd/proxy -adaptive sessions
```

This automatically drops encoding quality under load to preserve real-time delivery.

---

## How do I debug issues?

**Enable admin panel:**

```bash
go run ./cmd/proxy -admin :6060
```

Then:

```bash
# Live stats
curl http://localhost:6060/stats | jq

# Goroutines
curl http://localhost:6060/debug/pprof/goroutine?debug=1

# Heap analysis
go tool pprof http://localhost:6060/debug/pprof/heap
```

**Check logs:**

```bash
docker compose logs -f rt-llm-proxy | grep -i error
```

---

## WebRTC connection fails. What now?

Checklist:

1. Is the proxy running?
   ```bash
   curl http://localhost:8080/stats
   ```

2. Are WebRTC ports open?
   ```bash
   sudo ufw allow 10000:60000/udp
   ```

3. Is your firewall / NAT blocking UDP?
   - The proxy is **not** NAT-traversal infra (no STUN/TURN)
   - It needs a direct path or public IP
   - For production, add TURN (coturn) or SFU (LiveKit)

4. Check the logs:
   ```bash
   docker compose logs rt-llm-proxy | tail -20
   ```

---

## I'm in China. How do I use this?

**Go proxy acceleration (required):**

```bash
go env -w GOPROXY=https://goproxy.cn,direct
```

Or in Docker:

```bash
docker compose -f docker-compose.yml -f docker-compose.cn.yml up --build
```

**Provider choices:**
- ✅ **Doubao** (豆包) — native Chinese API, no VPN needed
- ⚠️ **Gemini** — may need VPN (Google API reachability)

**Model pre-caching:**

```bash
export QWEN_MODEL_PATH=/local/model
docker compose -f docker-compose.yml -f docker-compose.cascade.yml up --build
```

Avoids HuggingFace downloads at runtime.

---

## Can it run in Kubernetes?

**Partially:**
- ✅ HTTP control plane (offer endpoint, admin API)
- ❌ WebRTC media (UDP audio has affinity, needs `hostNetwork` or TURN)

**Better approach:**
- Deploy proxy on plain VMs (no K8s)
- Front with TURN + SFU in K8s
- SFU routes media; proxy is just the LLM bridge

---

## How do I monitor in production?

**Key metrics:**

```
frames_late_30ms   → SLO (alert if >5%)
sessions           → capacity tracking
memory_bytes       → leak detection
goroutines         → resource leak
```

**Setup:**

```bash
# Scrape stats endpoint
prometheus:
  - targets: ['localhost:6060/stats']
    
# Alert on high latency
- alert: HighFrameLatency
  expr: frames_late_30ms > 0.05
  for: 5m
```

See [Deployment Guide — Monitoring](DEPLOYMENT.md#monitoring--operations).

---

## Can I use it as a library in my app?

**Yes!** The core is in `internal/model` and `internal/rtc`:

```go
import "rt-llm-proxy/internal/model/cascade"

c, err := cascade.New(ctx, cascade.Config{
    Whisper: asr.NewWhisper(whisperURL),
    LLM:     llm.New(llmURL, "Qwen3.5-9B"),
    TTS:     tts.NewXTTSStream(ttsURL, ...),
    OnLLMToken: func(token, accumulated string) (string, bool) {
        // Custom logic here
        return "", false
    },
})
```

But the main proxy (`cmd/proxy`) is the typical entry point.

---

## I found a bug. What do I do?

1. **Check existing issues** on GitHub
2. **Reproduce with logs enabled:**
   ```bash
   go run ./cmd/proxy -admin :6060 2>&1 | tee debug.log
   ```
3. **Post on GitHub** with:
   - Error message
   - Steps to reproduce
   - `curl http://localhost:6060/stats` output
   - Log excerpt

---

## License?

Check `LICENSE` file in the repo. Usually MIT or Apache 2.0.

---

## Contributing?

Pull requests welcome! See `CONTRIBUTING.md` (if it exists).

---

## Where's the rest of the documentation?

| Doc | Purpose |
|---|---|
| [Quick Start](QUICK_START.md) | 5-min setup |
| [Deployment Guide](DEPLOYMENT.md) | Production deploy |
| [Architecture](ARCHITECTURE.md) | Design deep-dive |
| [Benchmarks](bench/README.md) | Performance data |
| [Chinese Guide](中文指南.md) | 中文版本 |
| [README](../README.md) | Feature overview |

---

## Still stuck?

- 📖 Read [Architecture](ARCHITECTURE.md) for deeper understanding
- 💬 Check GitHub Discussions
- 🐛 File an issue with details
- 📚 Review `CONTEXT.md` for domain terms

