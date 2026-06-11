# Integration Guide — embedding rt-llm-proxy in a downstream service

This doc is for the team copying rt-llm-proxy (the **voice service**) into a
larger product. The proxy ships **runnable but with three seams left open on
purpose** — they default to local-dev stubs so the demo works out of the box,
and a real deployment must close them. Everything else (WebRTC, providers,
transcript side-channel, reconnect, config file) works as shipped.

All three are wired in one place: `cmd/proxy/root.go`, function `runProxy`.

---

## Pre-prod checklist

| # | Seam | Ships as | Symbol to replace | Must close before |
|---|------|----------|-------------------|-------------------|
| 1 | Identity | `auth.DevVerifier` (token == user id, no crypto) | `auth.TokenVerifier` | any untrusted caller |
| 2 | Memory   | unset (`nil`) → dev `X-Listener-Brief` header | `offer.MemoryProvider` | personalization / prod |
| 3 | Offer endpoint trust | open `mux.Handle("/", offerHandler)` | network/auth policy | exposing to browsers |

These are the same items tracked as DEFERRED in the architecture notes — nothing
is missing from the design; the wiring is just intentionally left to the host.

---

## 1. Identity — replace `DevVerifier`

`cmd/proxy/root.go:34`:

```go
authn := auth.New(auth.DevVerifier{})
```

`DevVerifier` treats the bearer token *as* the user id with no verification
(`internal/auth/auth.go:66`). Replace it with a verifier that validates your
real token (JWT, opaque session, IdP introspection):

```go
type myVerifier struct{ /* keys, IdP client, … */ }

func (v myVerifier) Verify(token string) (identity.UserID, error) {
    claims, err := v.parseAndCheck(token) // signature, expiry, audience
    if err != nil {
        return "", err
    }
    return identity.UserID(claims.Subject), nil
}

authn := auth.New(myVerifier{...})
```

The seam is `auth.TokenVerifier` (one method, `internal/auth/auth.go:22`).
Identity failure is non-fatal by design: a bad/missing token degrades the
request to **anonymous** (`UserID == ""`), it does not block the call. Anonymous
sessions get no per-user memory (see §2).

---

## 2. Memory — implement `MemoryProvider`

The proxy can inject a per-user **listener brief** as the model's *system
instruction* at session start, so the model is personalized from the first word
without spending a dialogue turn (and without polluting the transcript — see
[ARCHITECTURE](ARCHITECTURE.md) / the memory-feedback-loop note in the README).

The seam (`internal/offer/memory.go:21`):

```go
type MemoryProvider interface {
    ListenerBrief(ctx context.Context, userID identity.UserID) (string, error)
}
```

It is **not wired by default** — `runProxy` builds `offer.HandlerFields` without
a `Memory` field, so it ships `nil`. With `nil`, the proxy falls back to the dev
`X-Listener-Brief` header (forgeable — dev only). To connect your Profile / mem0
service, implement the interface and set the field:

```go
type profileMemory struct{ client *profile.Client }

func (m profileMemory) ListenerBrief(ctx context.Context, uid identity.UserID) (string, error) {
    return m.client.Brief(ctx, string(uid)) // e.g. "likes Jay Chou, studying"
}

offer.HandlerFields{
    Auth:   authn,
    Memory: profileMemory{client: profileClient}, // ← add this line
    // … existing fields …
}.Build()
```

Contract notes:

- **Authoritative once set.** With a provider wired, the forgeable
  `X-Listener-Brief` header is ignored (`resolveBrief`, `memory.go:29`).
- **Budgeted.** The lookup runs before media flows and is capped at
  `memoryBudget = 300ms` (`memory.go:13`); a slow or erroring provider yields an
  empty brief rather than stalling session start.
- **Anonymous users get nothing** — there's no stable key to fetch a per-user
  brief, so the provider isn't called.
- **Read side only.** This is the memory *read* path. Memory *writes* flow the
  other way: the transcript side-channel (Kafka) carries role-tagged turns
  (`internal/sidechannel/tap.go`), and your downstream Profile consumer selects
  which turns to feed mem0. The proxy does **not** filter — the same stream also
  drives reconnect restore, which needs both user and model turns.

---

## 3. Lock down the offer endpoint

`runProxy` serves the offer handler at `/` on the public mux, alongside the
`/demo/` static files (`root.go:97-99`). Because the listener brief is **trusted
from the caller**, the offer endpoint must be reachable **only by your
orchestrator/control plane, not by browsers** — otherwise a browser can forge
identity (until §1 is done) or a brief (until §2 is done).

Options, in increasing strength:

- Don't expose `/` publicly; put the offer call behind your orchestrator and
  reach the proxy over an internal network only.
- Require a service-to-service credential on the offer route (validated by your
  §1 verifier or a separate gateway).
- Drop `mux.Handle("/demo/", …)` for non-dev builds so the browser demo (and its
  `?brief=` forgery helper in `demo/client.js`) is never served near prod.

---

## What you do *not* need to change

- **Providers** (Gemini Live 3.1, Doubao, Cascade) — configured via `proxy.yaml`
  + env; see README "Provider behavior via config file".
- **Tool calling** — declared in `proxy.yaml` under `gemini.tools`,
  business-neutral; calls are forwarded to the browser over the data channel.
- **Transcript / reconnect** — the Kafka side-channel and replay index work as
  shipped; role tags are already emitted per turn.

See [INDEX.md](INDEX.md) for the full doc map.
