package offer

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/thinkinbig/rt-llm-proxy/internal/modelcb"
	"github.com/thinkinbig/rt-llm-proxy/internal/ratelimit"
)

func TestJoinSystem(t *testing.T) {
	cases := []struct{ base, suffix, want string }{
		{"persona", "brief", "persona\n\nbrief"},
		{"persona", "", "persona"},
		{"", "brief", "brief"},
		{"", "", ""},
	}
	for _, c := range cases {
		if got := joinSystem(c.base, c.suffix); got != c.want {
			t.Fatalf("joinSystem(%q,%q) = %q, want %q", c.base, c.suffix, got, c.want)
		}
	}
}

func TestDecodeListenerBrief(t *testing.T) {
	brief := "听众：爱周杰伦、在备考"
	enc := base64.StdEncoding.EncodeToString([]byte(brief))
	if got := decodeListenerBrief(enc); got != brief {
		t.Fatalf("decode = %q, want %q", got, brief)
	}
	if got := decodeListenerBrief(""); got != "" {
		t.Fatalf("empty header should decode to empty, got %q", got)
	}
	if got := decodeListenerBrief("!!!not base64!!!"); got != "" {
		t.Fatalf("bad base64 should decode to empty, got %q", got)
	}
}

func TestDecodeListenerBriefCaps(t *testing.T) {
	big := strings.Repeat("a", maxListenerBrief+1000)
	got := decodeListenerBrief(base64.StdEncoding.EncodeToString([]byte(big)))
	if len(got) > maxListenerBrief {
		t.Fatalf("decoded brief = %d bytes, want <= %d", len(got), maxListenerBrief)
	}
}

// The base64 brief header must reach the factory as SessionParams.SystemSuffix.
func TestIntakeForwardsListenerBrief(t *testing.T) {
	factory := &fakeFactory{m: &fakeModel{}}
	in := Intake{
		Limiter: ratelimit.New("", 0, time.Minute),
		Guard:   modelcb.New(modelcb.Config{}, nil),
		Models:  factory,
		Hub:     &fakeHub{},
	}
	brief := "这位听众喜欢爵士"
	res := in.ServeOffer(IntakeRequest{
		Ctx:                 context.Background(),
		ClientIP:            "1.2.3.4",
		Model:               "gemini",
		OfferSDP:            []byte("sdp"),
		ListenerBriefHeader: base64.StdEncoding.EncodeToString([]byte(brief)),
	})
	if res.Status != 200 {
		t.Fatalf("status = %d", res.Status)
	}
	if factory.lastParams.SystemSuffix != brief {
		t.Fatalf("SystemSuffix = %q, want %q", factory.lastParams.SystemSuffix, brief)
	}
}
