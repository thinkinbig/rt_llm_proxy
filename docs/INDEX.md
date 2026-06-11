# Documentation Index — rt-llm-proxy

Complete guide to finding the right documentation.

## 🚀 For First-Time Users

Start here if you're new to rt-llm-proxy:

1. **[Quick Start (5 min)](QUICK_START.md)** — Get it running in 5 minutes
   - Install prerequisites
   - Run locally or Docker
   - Open demo page

2. **[FAQ — Functional Questions](FAQ.md#what-is-rt-llm-proxy)** — Understand what this is
   - What's the difference between providers?
   - How much latency is expected?
   - Can I run it on CPU?

3. **[Deployment Guide — Local Development](DEPLOYMENT.md#local-development)** — Set up your dev environment
   - Install Go 1.25+
   - Configure dependencies
   - Run in debug mode

---

## 💻 For Deployment & Operations

Planning to deploy or already running in production?

1. **[Deployment Guide (30 min)](DEPLOYMENT.md)** — Complete deployment manual
   - Local development setup
   - Docker Compose stacks (5 variants)
   - Cascade (self-hosted) deployment
   - China-specific setup
   - Production checklist
   - Monitoring & troubleshooting

2. **[FAQ — Deployment & Operations](FAQ.md#how-do-i-get-started)** — Common deployment questions
   - "How do I limit requests per user?"
   - "Can it run in Kubernetes?"
   - "How do I monitor in production?"

3. **[ARCHITECTURE.md](ARCHITECTURE.md)** — Deep design rationale
   - System overview
   - Control vs data plane
   - Cascade orchestration
   - Engineering optimizations
   - Fault tolerance strategy

---

## ⚡ For Performance Optimization

Trying to improve latency or throughput?

1. **[Benchmarks (bench/README.md)](bench/README.md)** — Performance baselines
   - Opus micro-benchmark (161µs encode, 18µs decode)
   - Capacity sweeps (16-core box ~600–1000 sessions)
   - Adaptive complexity tuning (complexity 10 → 5 halves CPU)

2. **[FAQ — Performance & Capacity](FAQ.md#whats-the-latency)** — Quick answers
   - "What's the latency?"
   - "How many concurrent users?"
   - "Why is performance degrading?"

3. **[Deployment Guide — Performance Tuning](DEPLOYMENT.md#performance-tuning)** — Optimization techniques
   - Opus complexity vs CPU tradeoff
   - Memory & GC tuning
   - Network optimization for Cascade

---

## 🏗️ For Understanding the Architecture

Want to understand system design and key decisions?

1. **[ARCHITECTURE.md](ARCHITECTURE.md)** — Full engineering documentation
   - §1: Proxy core — WebRTC bridge, control plane, fault tolerance
   - §2: Cascade pipeline — orchestrator + sidecars
   - §3: Modules & seams — key abstractions
   - §4: Optimizations — pacing, Opus, replay, adaptive complexity
   - §5: Tests — coverage overview

2. **[CONTEXT.md](../CONTEXT.md)** — Domain glossary
   - Session, transcript, provider, bridge, model seam, etc.

3. **[README.md](../README.md)** — High-level overview
   - Feature matrix
   - Quick start
   - Docker Compose variants

---

## ❓ For Troubleshooting

Something broken? Use these resources:

| Problem | Go to |
|---|---|
| WebRTC connection fails | [FAQ — WebRTC](FAQ.md#webrtc-connection-fails-what-now) |
| High latency / frame drops | [FAQ — Performance](FAQ.md#why-is-performance-degrading) |
| Cascade sidecars unreachable | [Deployment — Troubleshooting](DEPLOYMENT.md#cascade-sidecars-not-responding) |
| Memory leak | [Deployment — Memory Leak](DEPLOYMENT.md#memory-leak) |
| API rate limiting (429) | [FAQ — Rate Limiting](FAQ.md#how-do-i-limit-requests-per-user) |

**General workflow:**
1. Check logs: `docker compose logs rt-llm-proxy | tail -20`
2. View stats: `curl http://localhost:6060/stats | jq`
3. Search FAQ for the symptom
4. If needed, enable pprof for detailed analysis

---

## 📚 Full Documentation Map

### Quick References

| Doc | Read time | Audience |
|---|---|---|
| [Quick Start](QUICK_START.md) | 5 min | Everyone |
| [FAQ](FAQ.md) | 15 min | Decision-makers, operators |
| [Deployment Guide](DEPLOYMENT.md) | 30 min | Operators, SREs |
| [ARCHITECTURE.md](ARCHITECTURE.md) | 45 min | Engineers, contributors |
| [Benchmarks](bench/README.md) | 20 min | Performance engineers |

### By Topic

**Getting Started:**
- [Quick Start](QUICK_START.md) — 5-minute setup
- [README.md](../README.md) — Feature overview

**Deployment:**
- [Deployment Guide](DEPLOYMENT.md) — Complete deployment manual
- [Docker Compose Variants](../README.md#docker-compose) — Config examples

**Architecture & Design:**
- [ARCHITECTURE.md](ARCHITECTURE.md) — System design & rationale
- [CONTEXT.md](../CONTEXT.md) — Domain glossary

**Operations & Monitoring:**
- [Deployment — Monitoring](DEPLOYMENT.md#monitoring--operations) — Observability setup
- [FAQ — Production](FAQ.md#how-do-i-monitor-in-production) — Monitoring best practices

**Performance:**
- [Benchmarks](bench/README.md) — Baseline numbers
- [FAQ — Performance](FAQ.md#whats-the-latency) — Performance Q&A
- [Deployment — Tuning](DEPLOYMENT.md#performance-tuning) — Optimization techniques

**Troubleshooting:**
- [Deployment — Troubleshooting](DEPLOYMENT.md#troubleshooting) — Common issues
- [FAQ — General](FAQ.md) — Q&A index

---

## 🎯 Use Case Scenarios

### Scenario: Rapid Prototyping (< 10 min)

Want to test if this works for your idea?

1. [Quick Start](QUICK_START.md) — run locally
2. Open http://localhost:8080/demo/
3. Click to speak, listen to LLM

**Total: 10 minutes**

### Scenario: Building a Voice AI App

Integrating rt-llm-proxy into your product?

1. [Quick Start](QUICK_START.md) — understand basics
2. [Integration Guide](INTEGRATION.md) — the three seams to close (identity, memory, endpoint trust) when embedding in a downstream service
3. [FAQ — Library Use](FAQ.md#can-i-use-it-as-a-library-in-my-app) — embed in your app
4. [ARCHITECTURE.md §3](ARCHITECTURE.md#3-modules--seams) — module seams and interfaces
5. Check `internal/model` for provider adapters

### Scenario: Setting Up Production

Deploying to production servers?

1. [Deployment Guide](DEPLOYMENT.md) — step-by-step
2. [Deployment — Configuration Flags](DEPLOYMENT.md#configuration-flags) — all options
3. [Deployment — Production Checklist](DEPLOYMENT.md#production-checklist) — pre-launch
4. [Deployment — Monitoring](DEPLOYMENT.md#monitoring--operations) — observability

### Scenario: Self-Hosted Everything (Cascade)

Running your own ASR/LLM/TTS?

1. [Quick Start — Cascade](QUICK_START.md#self-hosted-cascade-requires-gpu) — 5-min cascade setup
2. [Deployment — Cascade](DEPLOYMENT.md#cascade-self-hosted-asrllmtts) — detailed cascade deployment
3. [ARCHITECTURE.md §2](ARCHITECTURE.md#2-cascade-pipeline) — how cascade works
4. [FAQ — Cascade](FAQ.md#what-is-cascade) — cascade Q&A

### Scenario: Optimizing for Scale

Improving performance to handle more users?

1. [Benchmarks](bench/README.md) — current capacity limits
2. [FAQ — Capacity](FAQ.md#how-many-concurrent-users) — scaling options
3. [Deployment — Performance Tuning](DEPLOYMENT.md#performance-tuning) — optimization levers
4. [ARCHITECTURE.md §4](ARCHITECTURE.md#4-engineering-optimization-points) — design choices

### Scenario: Using in China

Deploying in China with local constraints?

1. [Deployment — China Setup](DEPLOYMENT.md#china-specific-setup) — Go proxy, Docker, models
2. [FAQ — China](FAQ.md#im-in-china-how-do-i-use-this) — provider choices (Doubao recommended)
3. [Quick Start](QUICK_START.md) — basic setup

---

## 🌍 Chinese Documentation (中文文档)

For Chinese-speaking users:

| 文档 | 说明 |
|---|---|
| [中文指南](中文指南.md) | 完整项目指南 |
| [中文部署指南](中文部署指南.md) | 部署和故障排查 |
| [中文常见问题](中文常见问题.md) | 常见问题解答 |
| [README_中文](README_中文.md) | 中文文档导航 |

---

## 📖 Reading Paths

### Path 1: "I want to understand this system"

```
README.md
  ↓
Quick Start (QUICK_START.md)
  ↓
CONTEXT.md (domain terms)
  ↓
ARCHITECTURE.md (full design)
  ↓
Benchmarks (bench/README.md)
```

**Duration:** ~2 hours

### Path 2: "I need to deploy this"

```
Quick Start (QUICK_START.md)
  ↓
Deployment Guide (DEPLOYMENT.md)
  ↓
FAQ — Operations (FAQ.md)
  ↓
Production Checklist (DEPLOYMENT.md#production-checklist)
```

**Duration:** ~1 hour

### Path 3: "I need to optimize performance"

```
Benchmarks (bench/README.md)
  ↓
FAQ — Performance (FAQ.md#whats-the-latency)
  ↓
Deployment — Tuning (DEPLOYMENT.md#performance-tuning)
  ↓
ARCHITECTURE.md §4 (optimization rationale)
```

**Duration:** ~45 min

### Path 4: "I'm debugging a problem"

```
Logs: docker compose logs rt-llm-proxy
  ↓
Stats: curl http://localhost:6060/stats
  ↓
FAQ (search symptom)
  ↓
Deployment — Troubleshooting (DEPLOYMENT.md#troubleshooting)
  ↓
ARCHITECTURE.md §1.4 (fault tolerance strategy)
```

**Duration:** depends on issue

---

## 🔗 Quick Links

**Getting started:**
- [Quick Start](QUICK_START.md)
- [README.md](../README.md)

**Deployment:**
- [Deployment Guide](DEPLOYMENT.md)
- [Docker Compose Overlays](../README.md#docker-compose)

**Architecture:**
- [ARCHITECTURE.md](ARCHITECTURE.md)
- [CONTEXT.md](../CONTEXT.md)

**Performance:**
- [Benchmarks](bench/README.md)
- [Deployment — Tuning](DEPLOYMENT.md#performance-tuning)

**Support:**
- [FAQ.md](FAQ.md) — Questions & answers
- GitHub Issues — Bug reports
- GitHub Discussions — Q&A

---

## How to Use This Index

1. **You're new?** → Start with [Quick Start](QUICK_START.md)
2. **You're deploying?** → Go to [Deployment Guide](DEPLOYMENT.md)
3. **You have a question?** → Search [FAQ](FAQ.md)
4. **You want details?** → Read [ARCHITECTURE.md](ARCHITECTURE.md)
5. **You're debugging?** → Check [Deployment — Troubleshooting](DEPLOYMENT.md#troubleshooting)

---

**Last updated:** 2026

