// Package aimodel is pogi's local model gateway.
//
// An OpenAI-compatible chat-completions endpoint (Ollama, LM Studio, vLLM) is set up in the
// `ai:` section of pogi.yaml. Bedrock, Comprehend, Translate and the like ask the Gateway
// for an answer; the model-id globs in `ai.models` choose the local model. When no endpoint
// is configured, or no glob matches, the answer is a canned one: the same input gives the
// same output, and the text says it is canned.
package aimodel

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"path"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

// Gateway sends chat requests to the configured endpoint.
type Gateway struct {
	Endpoint string            // base URL, e.g. http://localhost:11434/v1
	Key      string            // bearer token, optional
	Models   map[string]string // model id glob -> local model name
	HTTP     *http.Client
}

// New builds a Gateway from the pogi.yaml `ai:` settings.
func New(endpoint, key string, models map[string]string) *Gateway {
	return &Gateway{Endpoint: strings.TrimRight(endpoint, "/"), Key: key, Models: models, HTTP: &http.Client{Timeout: 5 * time.Minute}}
}

// Local returns the local model that serves a model id, or false when the answer is canned.
func (g *Gateway) Local(id string) (string, bool) {
	if g == nil || g.Endpoint == "" {
		return "", false
	}
	if len(g.Models) == 0 {
		return id, true // no mapping at all: the endpoint is asked for the id itself
	}
	pats := make([]string, 0, len(g.Models))
	for p := range g.Models {
		pats = append(pats, p)
	}
	// Longest (most specific) glob first, then alphabetical, so the choice is stable.
	slices.SortFunc(pats, func(a, b string) int {
		if len(a) != len(b) {
			return len(b) - len(a)
		}
		return strings.Compare(a, b)
	})
	for _, p := range pats {
		if ok, _ := path.Match(p, id); ok {
			return g.Models[p], true
		}
	}
	return "", false
}

// Any is Local for callers that have no model id (Comprehend, Translate): a glob that matches
// the id wins, else the first configured model.
func (g *Gateway) Any(id string) (string, bool) {
	if m, ok := g.Local(id); ok {
		return m, true
	}
	if g == nil || g.Endpoint == "" || len(g.Models) == 0 {
		return "", false
	}
	vals := make([]string, 0, len(g.Models))
	for _, v := range g.Models {
		vals = append(vals, v)
	}
	slices.Sort(vals)
	return vals[0], true
}

// ToolCall is a tool use the model asked for; Arguments is JSON.
type ToolCall struct{ ID, Name, Arguments string }

// Message is one chat turn. Role is user, assistant or tool.
type Message struct {
	Role       string
	Content    string
	ToolCalls  []ToolCall // assistant turns
	ToolCallID string     // tool turns
}

// Tool is a function the model may call; Schema is a JSON Schema.
type Tool struct {
	Name, Description string
	Schema            any
}

// Request is a chat request in provider-neutral form.
type Request struct {
	System      string
	Messages    []Message
	MaxTokens   int
	Temperature *float64
	TopP        *float64
	Stop        []string
	Tools       []Tool
}

// Response is the answer. Stop is the Bedrock stop reason: end_turn, max_tokens, tool_use or stop_sequence.
type Response struct {
	Text         string
	ToolCalls    []ToolCall
	Stop         string
	InputTokens  int
	OutputTokens int
	Canned       bool
}

// Text is the plain text of a request: the system prompt and every turn.
func (r Request) Text() string {
	var b strings.Builder
	b.WriteString(r.System)
	for _, m := range r.Messages {
		b.WriteString("\n" + m.Content)
	}
	return b.String()
}

// Tokens estimates the token count of a text (about four characters each).
func Tokens(s string) int { return (utf8.RuneCountInString(s) + 3) / 4 }

// CannedText is the deterministic answer used when no local model serves a request.
func CannedText(input string) string {
	sum := sha256.Sum256([]byte(input))
	return fmt.Sprintf("[pogi canned response %x] No local model is configured, so this is a canned answer. The input was %d characters long.",
		sum[:4], utf8.RuneCountInString(input))
}

// Chat answers a request for a Bedrock-style model id. It is canned when no local model serves
// the id, and an error when the local endpoint fails.
func (g *Gateway) Chat(ctx context.Context, modelID string, req Request) (Response, error) {
	model, ok := g.Local(modelID)
	if !ok {
		text := CannedText(modelID + "\n" + req.Text())
		return Response{Text: text, Stop: "end_turn", InputTokens: Tokens(req.Text()), OutputTokens: Tokens(text), Canned: true}, nil
	}
	return g.chat(ctx, model, req)
}

// Complete asks for a plain-text answer to a prompt, for the services that are not chat APIs.
// ok is false when no model is configured: the caller then makes its own canned result.
func (g *Gateway) Complete(ctx context.Context, id, system, prompt string) (text string, ok bool, err error) {
	model, ok := g.Any(id)
	if !ok {
		return "", false, nil
	}
	r, err := g.chat(ctx, model, Request{System: system, Messages: []Message{{Role: "user", Content: prompt}}, Temperature: new(0.0)})
	return r.Text, true, err
}

func (g *Gateway) chat(ctx context.Context, model string, req Request) (Response, error) {
	type fn struct {
		Name        string `json:"name"`
		Description string `json:"description,omitempty"`
		Parameters  any    `json:"parameters,omitempty"`
		Arguments   string `json:"arguments,omitempty"`
	}
	type call struct {
		ID       string `json:"id"`
		Type     string `json:"type"`
		Function fn     `json:"function"`
	}
	type msg struct {
		Role       string `json:"role"`
		Content    any    `json:"content"`
		ToolCalls  []call `json:"tool_calls,omitempty"`
		ToolCallID string `json:"tool_call_id,omitempty"`
	}
	body := map[string]any{"model": model}
	var msgs []msg
	if req.System != "" {
		msgs = append(msgs, msg{Role: "system", Content: req.System})
	}
	for _, m := range req.Messages {
		x := msg{Role: m.Role, Content: m.Content, ToolCallID: m.ToolCallID}
		for _, tc := range m.ToolCalls {
			x.ToolCalls = append(x.ToolCalls, call{ID: tc.ID, Type: "function", Function: fn{Name: tc.Name, Arguments: tc.Arguments}})
		}
		if len(x.ToolCalls) > 0 && m.Content == "" {
			x.Content = nil
		}
		msgs = append(msgs, x)
	}
	body["messages"] = msgs
	if req.MaxTokens > 0 {
		body["max_tokens"] = req.MaxTokens
	}
	if req.Temperature != nil {
		body["temperature"] = *req.Temperature
	}
	if req.TopP != nil {
		body["top_p"] = *req.TopP
	}
	if len(req.Stop) > 0 {
		body["stop"] = req.Stop
	}
	if len(req.Tools) > 0 {
		var tools []map[string]any
		for _, t := range req.Tools {
			tools = append(tools, map[string]any{"type": "function", "function": fn{Name: t.Name, Description: t.Description, Parameters: t.Schema}})
		}
		body["tools"] = tools
	}
	b, _ := json.Marshal(body)
	hr, err := http.NewRequestWithContext(ctx, http.MethodPost, g.Endpoint+"/chat/completions", bytes.NewReader(b))
	if err != nil {
		return Response{}, err
	}
	hr.Header.Set("Content-Type", "application/json")
	if g.Key != "" {
		hr.Header.Set("Authorization", "Bearer "+g.Key)
	}
	resp, err := g.HTTP.Do(hr)
	if err != nil {
		return Response{}, fmt.Errorf("local model: %w", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if resp.StatusCode/100 != 2 {
		return Response{}, fmt.Errorf("local model: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	var out struct {
		Choices []struct {
			Message struct {
				Content   string `json:"content"`
				ToolCalls []struct {
					ID       string `json:"id"`
					Function struct{ Name, Arguments string }
				} `json:"tool_calls"`
			}
			FinishReason string `json:"finish_reason"`
		}
		Usage struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
		}
	}
	if err := json.Unmarshal(data, &out); err != nil || len(out.Choices) == 0 {
		return Response{}, fmt.Errorf("local model: unusable answer: %.200s", data)
	}
	ch := out.Choices[0]
	r := Response{Text: ch.Message.Content, InputTokens: out.Usage.PromptTokens, OutputTokens: out.Usage.CompletionTokens}
	for i, tc := range ch.Message.ToolCalls {
		id := tc.ID
		if id == "" {
			id = fmt.Sprintf("tooluse_%d", i)
		}
		r.ToolCalls = append(r.ToolCalls, ToolCall{ID: id, Name: tc.Function.Name, Arguments: tc.Function.Arguments})
	}
	switch {
	case len(r.ToolCalls) > 0:
		r.Stop = "tool_use"
	case ch.FinishReason == "length":
		r.Stop = "max_tokens"
	default:
		r.Stop = "end_turn"
	}
	if r.InputTokens == 0 {
		r.InputTokens = Tokens(req.Text())
	}
	if r.OutputTokens == 0 {
		r.OutputTokens = Tokens(r.Text)
	}
	return r, nil
}

// Embed is a deterministic unit vector of dim floats made from the hash of the text.
func Embed(text string, dim int) []float64 {
	v := make([]float64, dim)
	var norm float64
	for i := 0; i < dim; i += 8 {
		sum := sha256.Sum256(fmt.Appendf(nil, "%d:%s", i, text))
		for j := 0; j < 8 && i+j < dim; j++ {
			f := float64(int32(binary.BigEndian.Uint32(sum[j*4:]))) / math.MaxInt32
			v[i+j] = f
			norm += f * f
		}
	}
	norm = math.Sqrt(norm)
	for i := range v {
		v[i] = math.Round(v[i]/norm*1e6) / 1e6
	}
	return v
}
