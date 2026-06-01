// Package identity defines the two distinct identifiers the proxy tracks. They
// are different layers and must not be transposed, so each is its own named type
// rather than a bare string:
//
//   - UserID is an authenticated identity (account). It is stable across many
//     connections and is what reconnect ownership and side-channel
//     personalization key on. "" means anonymous.
//   - SessionID is one connection's transcript stream — the key for the seq
//     sequence, in-memory history, and reconnect. One UserID owns many
//     SessionIDs (1:N).
//
// String fields at the edges (HTTP headers, UUID minting, Kafka proto) stay
// plain strings; convert explicitly at those boundaries so the conversion marks
// where an untrusted/serialized value crosses into the typed domain.
package identity

// UserID is an authenticated identity; the empty value is anonymous.
type UserID string

// SessionID identifies one connection's transcript stream.
type SessionID string

// Anonymous reports whether the user is unauthenticated. Anonymous users are not
// reconnectable: without a stable identity to bind ownership to, a session id
// alone is a guessable bearer capability.
func (u UserID) Anonymous() bool { return u == "" }
