// Package auth resolves a stable user identity for the SDP offer endpoint. It
// sits on the control plane and is deliberately business-agnostic: the actual
// token validation is an injectable TokenVerifier seam, so the core server is
// not married to any identity provider.
//
// Failure policy mirrors internal/ratelimit: identity is a soft, side-channel
// concern. A missing/invalid token, or no verifier configured, resolves to the
// anonymous user ("") — it never rejects the call. The real-time media path
// must never be gated by identity (ARCHITECTURE §3.3).
package auth

import (
	"log"
	"net/http"
	"strings"

	"github.com/thinkinbig/rt-llm-proxy/internal/identity"
)

// TokenVerifier maps an opaque bearer token to a stable user id. Implementations
// plug in a real IdP / token format; the core server only depends on this seam.
type TokenVerifier interface {
	Verify(token string) (userID identity.UserID, err error)
}

// Authenticator resolves the user id of an incoming request via its verifier.
// A nil verifier disables identity (everyone is anonymous), mirroring how an
// empty Redis addr disables the limiter.
type Authenticator struct{ v TokenVerifier }

// New returns an Authenticator. v=nil makes every request anonymous.
func New(v TokenVerifier) *Authenticator { return &Authenticator{v: v} }

// UserID returns the request's stable user id, or "" (anonymous) if there is no
// verifier, no bearer token, or the token fails to verify. It never errors:
// identity failure degrades the side-channel to anonymous, it does not block.
func (a *Authenticator) UserID(r *http.Request) identity.UserID {
	if a == nil || a.v == nil {
		return ""
	}
	tok := bearer(r)
	if tok == "" {
		return ""
	}
	uid, err := a.v.Verify(tok)
	if err != nil {
		log.Printf("auth: token verify failed, treating as anonymous: %v", err)
		return ""
	}
	return uid
}

// bearer extracts the token from an "Authorization: Bearer <token>" header.
func bearer(r *http.Request) string {
	h := r.Header.Get("Authorization")
	const p = "Bearer "
	if len(h) > len(p) && strings.EqualFold(h[:len(p)], p) {
		return strings.TrimSpace(h[len(p):])
	}
	return ""
}

// DevVerifier treats the bearer token itself as the user id. It performs no
// cryptographic verification and exists only as a runnable local-dev default —
// a real deployment injects a verifier that validates a signed token.
type DevVerifier struct{}

func (DevVerifier) Verify(token string) (identity.UserID, error) { return identity.UserID(token), nil }
