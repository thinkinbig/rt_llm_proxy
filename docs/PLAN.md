# Build Plan — Sessions, Side-channel, Load test

Blueprint for three additions, grilled into shape on 2026-05-31. Read
`ARCHITECTURE.md` first — this plan extends it and inherits its invariants.

## Guiding bar

Production-grade **core server**, but **business-agnostic**: every place a
business specific would go is a *seam* (interface + default impl + opt-out),
never baked-in logic. The template is `internal/ratelimit`: a real
implementation, `addr=="" ` disables it, and it **fails open** so the control
plane never takes down the data plane.

**Load-bearing invariant (inherited, §3.3):** the control / personalization
plane must never gate the real-time data plane. Identity resolution and event
publishing are best-effort side concerns; a failure in either degrades the
feature, never the call.

## Decisions (the grill, settled)

| # | Decision |
|---|---|
| Payload | Side-channel publishes **text transcripts** only — never audio. Lines come from the Bridge `Recorder` via `sidechannel.Tap`. |
| Multi-session | Upgrade `Hub` in place into a **`SessionManager`**: `session` gains `id`/`userID`/metadata/created-at; map keyed by `session_id`; lookup-by-id. |
| Identity | `Authenticator` seam → `UserID`; default reads `Authorization: Bearer` via an injectable `TokenVerifier` (dev default). `session_id` minted server-side (UUID). |
| Identity failure | **fail-open to anonymous** on the media path (`user_id=""`), call continues. |
| session_id visibility | returned to browser via `X-Session-ID` response header on the SDP answer. |
| Tap location | Bridge **`transcript.Recorder`** is the single recording point; **`sidechannel.Tap`** implements `transcript.Listener` and publishes with the Bridge-assigned seq. |
| Transport | **Kafka**, partition by `user_id` for in-partition ordering. |
| Partition key | `user_id`, **falling back to `session_id` when anonymous** (avoids an anonymous hot partition). |
| Delivery | bounded channel → async producer; **drop-on-full + `dropped_total`**; `acks=1`; **at-most-once, ordered-per-partition, lossy-under-pressure**. |
| Publisher seam | `Publisher.Publish(ev)`; default **Kafka** impl + **no-op/stdout** fallback (no broker configured ⇒ disabled, mirrors `-redis`). |
| Schema | **Protobuf** (`.proto` + `protoc-gen-go`, **no Schema Registry**). Fields: `schema_version,event_id,session_id,user_id,seq,role,text,ts,provider`. |
| `seq` | **`transcript.Recorder`** assigns seq once per session; data channel, reconnect history, and Kafka events all share it. |
| Consumer | **out of scope**; ship only a stdout demo consumer. |
| Load test SUT | a **`loopback` fake provider** (`?model=loopback`) isolates the proxy from upstream cost/latency/limits. |
| Load generator | **real pion headless clients**; **pre-encoded Opus replayed** to all sessions; run **off-box**; measure proxy-side resource delta via pprof/`/proc`. |
| Real SLO | **outbound frame-interval p99 < 25ms** (§3.1 zero-drift holds under concurrency) + goroutines return to baseline after teardown + `dropped_total==0` at nominal + graceful degradation past N. |
| Deliverable | a **capacity curve** (concurrency vs CPU, concurrency vs frame-interval p99); find the knee. Target concurrency: start ~100, central estimate ~500/modest node. |

## Phases (build order = dependency order)

- [x] **P1 — Foundation.** `SessionManager` (Hub upgrade) + `Authenticator`
  seam + minted `session_id` + `X-Session-ID` header. No behavior change to
  media; just identity + registry.
- [x] **P2 — Side-channel.** `event.proto` + generated Go; `Publisher`
  seam with Nop/Stdout defaults + Kafka impl; `sidechannel.Tap` listener on
  Bridge `Recorder`; wired in `offer.Handler` before `hub.Serve`; partition-key +
  drop-on-full + shared `seq` + `dropped_total`; Stdout demo consumer
  (`-sidechannel=off|stdout|kafka`).
- [x] **P3 — Loopback provider.** `internal/model/loopback`, `?model=loopback`:
  `SendAudio` discards, `Recv` returns a looping 440Hz sine frame (paced by the
  bridge Ticker, not silence so DTX can't suppress it), `RecvTranscript` emits a
  synthetic transcript every 2s.
- [x] **P4 — Load generator + metrics.** `internal/metrics` lock-free
  frame-interval histogram instrumented in `writeOutbound`; admin listener
  (`-admin`) serves `/stats` (goroutines, sessions, frame buckets,
  `sidechannel_dropped`) + `/debug/pprof`; `cmd/loadgen` drives N pion sessions
  with pre-encoded Opus replay, polls `/stats`. E2E-verified at n=5.
- [x] **P5 — Benchmark.** `internal/audio/opus_bench_test.go`: **encode
  ~161µs/frame, decode ~19µs, roundtrip ~187µs** (16-core box). So one
  full-duplex session costs ~0.94% of a core (50 fps), i.e. **~107
  sessions/core** for Opus alone; computed dedicated-box ceiling ~600-1000 here.
  Local sweep (loadgen co-located, pessimistic): pacing SLO healthy at n=50
  (2.6% frames >=30ms), broken by n=150 (28%). **Encode dominates decode 8.5x**;
  1-2 heap allocs/frame. Knee is artificially low from loadgen/proxy CPU
  contention — run loadgen off-box for real numbers.
- [x] **P6 — Optimization: zero-alloc audio hot path.** The benchmark redirected
  P6 from the timing wheel (Opus *encode CPU*, not scheduling, is the wall) to
  per-frame allocations. `Encoder`/`Decoder` now reuse an internal buffer
  (bufio.Scanner-style; safe because providers resample-copy and pion's Opus
  payloader copies in `WriteSample`). Result (benchstat, `count=10`): **Encode/
  Decode/Roundtrip allocs 1/1/2 → 0/0/0 (-100%)**, no latency regression (Decode
  -4.6%). Eliminates ~500k allocs/s at 5000 sessions → less GC, which also eases
  the pacing jitter. See [`bench/README.md`](bench/README.md).
  - **Shared timing wheel: TRIED → REVERTED.** A single-ticker, slot-sharded
    pacer replacing the per-session `time.Ticker` *regressed* pacing badly under
    load (n=50: 2.6% → 19%; n=150: 28% → 76% frames ≥30ms). Root cause: the
    wheel advances one slot per *received* 1ms tick, so when the lone pacer
    goroutine misses ticks under CPU contention the whole wheel slows and drags
    every session's cadence. Per-session `time.Ticker`s are independent and the
    runtime maintains their wall-clock rate — §3.1's design is empirically
    vindicated. Don't re-attempt without a wall-clock-advance wheel, and even
    then it's complexity for a non-bottleneck.
  - **Opus complexity tuning + adaptive load-shedding (done).** The only lever
    on the 161µs encode wall. Measured: c=10→c=5 halves encode (~2× capacity),
    c=8→c=10 is free. Live-adjustable via an atomic the encoder re-reads per
    frame, driven by two controllers (`internal/adaptive`, `-adaptive`):
    **`sessions`** (proactive step function of session count, hysteresis — the
    **recommended default**) and **`drift`** (reactive on the >=30ms bucket).
    A/B under load: both cut drift 21%→~15%, but `drift` **oscillates** (sheds
    CPU, pacing recovers, it raises complexity back under sustained load, repeat)
    — same feedback-loop hazard as the pacer; `sessions` settles stably. `drift`
    kept as a labelled experimental option. See [`bench/README.md`](bench/README.md).

## Known sharp edges (don't trip on these)

- **`model.Transcriber` assertion.** The Bridge does `m.(model.Transcriber)` to
  decide whether to forward STT transcripts. Providers implement
  `RecvTranscript()` directly — no decorator wrapper needed.
- **Anonymous hot partition** — already handled by the `session_id` fallback
  key; don't regress it by partitioning on a blank `user_id`.
- **Generator self-cost** — if the load client re-encodes Opus per session it
  saturates before the proxy does; pre-encode once and replay, and run off-box.
- **`X-Session-ID` placement** — it's an HTTP response header on the SDP
  answer, not part of the SDP body.
