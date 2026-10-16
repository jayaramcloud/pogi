package aimodel_test

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nrndev/pogi/internal/aimodel"
)

func TestCannedIsStable(t *testing.T) {
	g := aimodel.New("", "", nil)
	req := aimodel.Request{Messages: []aimodel.Message{{Role: "user", Content: "hello"}}}
	a, _ := g.Chat(context.Background(), "amazon.nova-lite-v1:0", req)
	b, _ := g.Chat(context.Background(), "amazon.nova-lite-v1:0", req)
	req.Messages[0].Content = "other"
	c, _ := g.Chat(context.Background(), "amazon.nova-lite-v1:0", req)
	if !a.Canned || a.Text != b.Text || a.Text == c.Text || !strings.Contains(a.Text, "canned") {
		t.Fatalf("%q %q %q", a.Text, b.Text, c.Text)
	}
}

func TestLocalModelAndGlobs(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" || r.Header.Get("Authorization") != "Bearer k" {
			http.Error(w, "bad", 400)
			return
		}
		json.NewDecoder(r.Body).Decode(&got)
		w.Write([]byte(`{"choices":[{"message":{"content":"","tool_calls":[{"id":"c1","function":{"name":"f","arguments":"{}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":7,"completion_tokens":3}}`))
	}))
	defer srv.Close()
	g := aimodel.New(srv.URL+"/v1/", "k", map[string]string{"anthropic.claude-*": "llama3", "anthropic.claude-3-*": "special"})
	if m, ok := g.Local("anthropic.claude-3-sonnet"); !ok || m != "special" {
		t.Fatalf("most specific glob: %q %v", m, ok)
	}
	if _, ok := g.Local("amazon.titan"); ok {
		t.Fatal("unmapped id served locally")
	}
	r, err := g.Chat(context.Background(), "anthropic.claude-v2", aimodel.Request{System: "s", Messages: []aimodel.Message{{Role: "user", Content: "hi"}},
		Tools: []aimodel.Tool{{Name: "f", Schema: map[string]any{"type": "object"}}}})
	if err != nil || r.Canned || r.Stop != "tool_use" || r.ToolCalls[0].Name != "f" || r.InputTokens != 7 || got["model"] != "llama3" {
		t.Fatalf("%+v %v %v", r, err, got)
	}
	if n := len(got["messages"].([]any)); n != 2 {
		t.Fatalf("messages: %v", got["messages"])
	}
}

func TestEmbed(t *testing.T) {
	a, b := aimodel.Embed("x", 1536), aimodel.Embed("x", 1536)
	var n float64
	for i := range a {
		if a[i] != b[i] {
			t.Fatal("not stable")
		}
		n += a[i] * a[i]
	}
	if len(a) != 1536 || math.Abs(n-1) > 1e-3 || aimodel.Embed("y", 4)[0] == aimodel.Embed("x", 4)[0] {
		t.Fatalf("len %d norm %v", len(a), n)
	}
}
