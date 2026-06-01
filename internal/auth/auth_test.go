package auth

import (
	"errors"
	"net/http"
	"testing"

	"github.com/thinkinbig/rt-llm-proxy/internal/identity"
)

type errVerifier struct{}

func (errVerifier) Verify(string) (identity.UserID, error) { return "", errors.New("bad token") }

func req(authHeader string) *http.Request {
	r, _ := http.NewRequest(http.MethodPost, "/", nil)
	if authHeader != "" {
		r.Header.Set("Authorization", authHeader)
	}
	return r
}

func TestUserID(t *testing.T) {
	tests := []struct {
		name   string
		authn  *Authenticator
		header string
		want   identity.UserID
	}{
		{"nil verifier is anonymous", New(nil), "Bearer alice", ""},
		{"no header is anonymous", New(DevVerifier{}), "", ""},
		{"non-bearer header is anonymous", New(DevVerifier{}), "Basic xyz", ""},
		{"dev verifier returns token as id", New(DevVerifier{}), "Bearer alice", "alice"},
		{"case-insensitive bearer scheme", New(DevVerifier{}), "bearer bob", "bob"},
		{"verify error degrades to anonymous", New(errVerifier{}), "Bearer alice", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.authn.UserID(req(tt.header)); got != tt.want {
				t.Errorf("UserID = %q, want %q", got, tt.want)
			}
		})
	}
}
