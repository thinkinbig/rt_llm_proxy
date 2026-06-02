# Build Plan — Cascade ASR→LLM→TTS Pipeline

M1 scaffolding complete. M2-M6 roadmap for real self-hosted cascade on Volcano Engine (火山引擎).
Read `ARCHITECTURE.md` first — this plan extends it and inherits its invariants.

## Guiding bar

Production-grade **core server**, but **business-agnostic**: every place a
business-specific seam goes is an *interface + default impl + opt-out*,
never baked-in logic. The template is `internal/ratelimit`: a real
implementation, `addr=="" ` disables it, and it **fails open** so the control
plane never takes down the data plane.

**Load-bearing invariant:** the control / personalization plane must never gate
the real-time data plane. LLM intercept and audio mixing are optional seams;
a failure in either degrades the feature, never the call.

## Cascade Decisions (2026-06-02)

| # | Decision |
|---|---|
| Provider topology | **Single co-located GPU on Volcano Engine (L20, 24GB).** ASR/LLM/TTS all self-hosted, not cloud API. Latency intra-region ~1–5ms (tolerates the two seams). |
| Model stack | **Whisper-Base (ASR)** + **vLLM/DeepSeek-R1-7B INT8 (LLM)** + **Coqui TTS FastPitch (TTS)**. Minimal, verified on L20. |
| Inference framework | **vLLM** (OpenAI-compat streaming, context cancel propagates to HTTP cancel). |
| LLM intercept seam | **`OnLLMToken(token, accumulated) (string, bool)` callback** in `cascade.Config`. Called per-token before TTS; returns substitution or pass-through. DJ song-pick logic lives here, outside `cascade/`. |
| Output mix seam | **`AudioSource` interface** — default reads TTS PCM; DJ can `SetAudioSource` to inject real MP3 or mixed audio. Core proxy indifferent to what `Recv()` returns. |
| Two-phase TTS | **Quick answer** (up to first sentence boundary) synthesized immediately while LLM continues; **Final answer** (remainder) queued sequentially. Reduces TTFA perception. |
| Barge-in cancel | **Ordered sequence** (LLM → TTS → Audio drain); context cancel propagates; dedup-protected via text similarity ≥0.9 Jaccard. |
| Fault tolerance | **Minimal.** API retry 1× on timeout; fast-fail on second. Connection restore via Kafka session_id+seq (already wired). Single-point machine failure accepted; documented in README. |
| Testing | All phases must pass existing `cascade_test.go` (fakestage tests stay green). E2E: real browser session, voice I/O, barge-in, LLM intercept hook fires. |

## M1 Completion (baseline, 2026-05-31)

- [x] `internal/model/cascade/` scaffolding (`stage.go`, `cascade.go`, `orchestrate.go`)
- [x] `fakestage/` stubs (no-op ASR/LLM/TTS for demo)
- [x] Factory wired in `offer/models.go` + `offer/provider.go` under `?model=cascade`
- [x] `model.Transcriber` impl (natively supported by Cascade)
- [x] M1 limitation noted: `respond()` blocks `run()` event loop (barge-in ineffective until M3)

## Phases (M2–M6)

- [x] **M2 — Real Stage Adapters.** `stage_whisper.go` (faster-whisper-server WebSocket, 48→16kHz resample), `stage_deepseek.go` (vLLM OpenAI-compat SSE, context cancel → HTTP cancel), `stage_coqui.go` (Coqui TTS WAV response, WAV parser, chunk streaming).
- [x] **M3 — Quick/Final Two-Phase TTS.** `respond()` accumulates tokens to first sentence boundary → quick TTS immediately, final TTS after. `respond()` moved to goroutine → `run()` event loop unblocked.
- [x] **M4 — Barge-in.** `bargeIn()` ordered cancel: cancel genCtx → wait `genDone` → drain `recvCh`. Jaccard similarity ≥0.9 dedup on `ASRFinal` and `textIn`.
- [x] **M5 — LLM Intercept Seam.** `Config.OnLLMToken func(token, accumulated string) (string, bool)`. Called per-token before TTS; history always records original token.
- [x] **M6 — Output-Mix Seam.** `AudioSource` interface + `Cascade.SetAudioSource()`. `Recv()` reads from source until `io.EOF`, then falls back to TTS `recvCh`.
- [x] **M7 — Minimal Fault Tolerance + README.** DeepSeek + Coqui retry once on transient failure, fast-fail on second. README: cascade architecture diagram, two seams, barge-in, VRAM budget, fault tolerance trade-offs.

## Critical Files to Modify/Create

| File | Change |
|------|--------|
| `internal/model/cascade/stage.go` | Add `OnLLMToken` to `Config` |
| `internal/model/cascade/orchestrate.go` | (M3) quick/final split in `respond()`; (M4) move `respond()` to goroutine + cancel sequence; (M5) call `OnLLMToken` |
| `internal/model/cascade/cascade.go` | (M6) Add `SetAudioSource()`; expose audio mux |
| `internal/model/cascade/stage_whisper.go` | **(M2) NEW** — Whisper-Base adapter |
| `internal/model/cascade/stage_deepseek.go` | **(M2) NEW** — vLLM/DeepSeek adapter |
| `internal/model/cascade/stage_coqui.go` | **(M2) NEW** — Coqui TTS adapter |
| `internal/offer/models.go` | (M2) Swap `fakestage.*` for real adapters in `ProdModelFactory` |
| `docs/ARCHITECTURE.md` or README | (M7) Add cascade section + two seams + trade-offs |

## Known sharp edges

- **M1 limitation**: `respond()` blocks the event loop, so barge-in fires but can't cancel in-flight LLM. Unblocks in M4.
- **Context cancel propagation**: vLLM and Coqui must both respect context cancel and close their HTTP connections cleanly.
- **PCM frame size consistency**: all three stages must emit/accept 48kHz mono s16 PCM at the same frame size (check bridge Ticker).
- **Session affinity**: don't move `respond()` work across goroutines without proper mutex on `genCancel`; M4 already has `abort_lock`.
