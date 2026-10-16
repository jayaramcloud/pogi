package generic_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/document"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime/types"
	"github.com/aws/smithy-go"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
)

// aiStart runs a whole pogi with the given AI settings and returns an SDK config for it.
func aiStart(t *testing.T, cfg pogi.Config) aws.Config {
	t.Helper()
	return awstest.Config(t, pogi.Start(t, cfg).URL)
}

// aiFakeModel is an OpenAI-compatible chat-completions server that answers "local says: <last user text>".
type aiFakeModel struct {
	*httptest.Server
	mu   sync.Mutex
	reqs []map[string]any
	tool bool // aiAnswer with a tool call
	// reply, when set, makes the aiAnswer from the last message
	reply func(last string) string
}

func aiNewFakeModel(t *testing.T) *aiFakeModel {
	f := &aiFakeModel{}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		json.NewDecoder(r.Body).Decode(&req)
		f.mu.Lock()
		f.reqs = append(f.reqs, req)
		tool, reply := f.tool, f.reply
		f.mu.Unlock()
		msgs := req["messages"].([]any)
		last, _ := msgs[len(msgs)-1].(map[string]any)["content"].(string)
		aiAnswer := "local says: " + last
		if reply != nil {
			aiAnswer = reply(last)
		}
		msg := map[string]any{"role": "assistant", "content": aiAnswer}
		fin := "stop"
		if tool {
			msg = map[string]any{"role": "assistant", "content": nil, "tool_calls": []any{map[string]any{"id": "call_1", "type": "function",
				"function": map[string]any{"name": "get_weather", "arguments": `{"city":"Oslo"}`}}}}
			fin = "tool_calls"
		}
		json.NewEncoder(w).Encode(map[string]any{"choices": []any{map[string]any{"message": msg, "finish_reason": fin}},
			"usage": map[string]any{"prompt_tokens": 11, "completion_tokens": 5}})
	}))
	t.Cleanup(f.Close)
	return f
}

func (f *aiFakeModel) setReply(fn func(last string) string) {
	f.mu.Lock()
	f.reply = fn
	f.mu.Unlock()
}

func aiUserText(s string) []types.Message {
	return []types.Message{{Role: types.ConversationRoleUser, Content: []types.ContentBlock{&types.ContentBlockMemberText{Value: s}}}}
}

func aiAnswer(t *testing.T, out *bedrockruntime.ConverseOutput) string {
	t.Helper()
	m, ok := out.Output.(*types.ConverseOutputMemberMessage)
	if !ok || len(m.Value.Content) == 0 {
		t.Fatalf("output: %+v", out.Output)
	}
	var sb strings.Builder
	for _, b := range m.Value.Content {
		if tb, ok := b.(*types.ContentBlockMemberText); ok {
			sb.WriteString(tb.Value)
		}
	}
	return sb.String()
}

func TestConverseLocalModel(t *testing.T) {
	ctx := context.Background()
	fm := aiNewFakeModel(t)
	c := bedrockruntime.NewFromConfig(aiStart(t, pogi.Config{AIEndpoint: fm.URL, AIKey: "k", AIModels: map[string]string{"anthropic.claude-*": "llama3.2"}}))

	out, err := c.Converse(ctx, &bedrockruntime.ConverseInput{ModelId: aws.String("anthropic.claude-3-haiku-20240307-v1:0"), Messages: aiUserText("hello there"),
		System:          []types.SystemContentBlock{&types.SystemContentBlockMemberText{Value: "be brief"}},
		InferenceConfig: &types.InferenceConfiguration{MaxTokens: aws.Int32(50), Temperature: aws.Float32(0.5)}})
	if err != nil {
		t.Fatal(err)
	}
	if got := aiAnswer(t, out); got != "local says: hello there" || out.StopReason != types.StopReasonEndTurn ||
		aws.ToInt32(out.Usage.InputTokens) != 11 || aws.ToInt32(out.Usage.OutputTokens) != 5 || aws.ToInt32(out.Usage.TotalTokens) != 16 {
		t.Fatalf("converse: %q %v %+v", got, out.StopReason, out.Usage)
	}
	req := fm.reqs[0]
	msgs := req["messages"].([]any)
	if req["model"] != "llama3.2" || len(msgs) != 2 || msgs[0].(map[string]any)["role"] != "system" || msgs[0].(map[string]any)["content"] != "be brief" ||
		req["max_tokens"] != 50.0 || req["temperature"] != 0.5 {
		t.Fatalf("what the model saw: %v", req)
	}

	// A tool the model wants to use comes back as a toolUse block, and a toolResult goes back out as a tool message.
	fm.mu.Lock()
	fm.tool = true
	fm.mu.Unlock()
	schema := document.NewLazyDocument(map[string]any{"type": "object", "properties": map[string]any{"city": map[string]any{"type": "string"}}})
	out, err = c.Converse(ctx, &bedrockruntime.ConverseInput{ModelId: aws.String("anthropic.claude-3-haiku-20240307-v1:0"), Messages: aiUserText("weather?"),
		ToolConfig: &types.ToolConfiguration{Tools: []types.Tool{&types.ToolMemberToolSpec{Value: types.ToolSpecification{Name: aws.String("get_weather"),
			Description: aws.String("weather"), InputSchema: &types.ToolInputSchemaMemberJson{Value: schema}}}}}})
	if err != nil {
		t.Fatal(err)
	}
	var use *types.ToolUseBlock
	for _, b := range out.Output.(*types.ConverseOutputMemberMessage).Value.Content {
		if tu, ok := b.(*types.ContentBlockMemberToolUse); ok {
			use = &tu.Value
		}
	}
	if use == nil || out.StopReason != types.StopReasonToolUse || aws.ToString(use.Name) != "get_weather" || aws.ToString(use.ToolUseId) != "call_1" {
		t.Fatalf("tool use: %+v %v", use, out.StopReason)
	}
	var in map[string]any
	if err := use.Input.UnmarshalSmithyDocument(&in); err != nil || in["city"] != "Oslo" {
		t.Fatalf("tool input: %v %v", in, err)
	}
	if tools := fm.reqs[1]["tools"].([]any); len(tools) != 1 || tools[0].(map[string]any)["function"].(map[string]any)["name"] != "get_weather" {
		t.Fatalf("tools sent: %v", fm.reqs[1]["tools"])
	}
	fm.mu.Lock()
	fm.tool = false
	fm.mu.Unlock()
	msgsIn := append(aiUserText("weather?"), types.Message{Role: types.ConversationRoleAssistant, Content: []types.ContentBlock{&types.ContentBlockMemberToolUse{Value: *use}}},
		types.Message{Role: types.ConversationRoleUser, Content: []types.ContentBlock{&types.ContentBlockMemberToolResult{Value: types.ToolResultBlock{
			ToolUseId: use.ToolUseId, Content: []types.ToolResultContentBlock{&types.ToolResultContentBlockMemberText{Value: "sunny"}}}}}})
	out, err = c.Converse(ctx, &bedrockruntime.ConverseInput{ModelId: aws.String("anthropic.claude-3-haiku-20240307-v1:0"), Messages: msgsIn})
	if err != nil || aiAnswer(t, out) != "local says: sunny" {
		t.Fatalf("tool result round: %v %v", err, out)
	}

	// A model id that no glob maps stays canned even with an endpoint.
	out, err = c.Converse(ctx, &bedrockruntime.ConverseInput{ModelId: aws.String("amazon.nova-lite-v1:0"), Messages: aiUserText("hi")})
	if err != nil || !strings.Contains(aiAnswer(t, out), "canned") {
		t.Fatalf("unmapped: %v %q", err, aiAnswer(t, out))
	}
}

func TestConverseCannedIsStable(t *testing.T) {
	ctx := context.Background()
	c := bedrockruntime.NewFromConfig(aiStart(t, pogi.Config{}))
	ask := func(text string) string {
		out, err := c.Converse(ctx, &bedrockruntime.ConverseInput{ModelId: aws.String("amazon.nova-lite-v1:0"), Messages: aiUserText(text)})
		if err != nil {
			t.Fatal(err)
		}
		if out.StopReason != types.StopReasonEndTurn || aws.ToInt32(out.Usage.OutputTokens) == 0 {
			t.Fatalf("%v %+v", out.StopReason, out.Usage)
		}
		return aiAnswer(t, out)
	}
	a, b, other := ask("same question"), ask("same question"), ask("another question")
	if a != b || a == other || !strings.Contains(a, "canned") {
		t.Fatalf("not stable or not marked: %q %q %q", a, b, other)
	}
}

func TestConverseStream(t *testing.T) {
	ctx := context.Background()
	fm := aiNewFakeModel(t)
	for name, cfg := range map[string]pogi.Config{"canned": {}, "local": {AIEndpoint: fm.URL, AIModels: map[string]string{"*": "m"}}} {
		t.Run(name, func(t *testing.T) {
			c := bedrockruntime.NewFromConfig(aiStart(t, cfg))
			want, err := c.Converse(ctx, &bedrockruntime.ConverseInput{ModelId: aws.String("meta.llama3-1-8b-instruct-v1:0"), Messages: aiUserText("stream me please")})
			if err != nil {
				t.Fatal(err)
			}
			out, err := c.ConverseStream(ctx, &bedrockruntime.ConverseStreamInput{ModelId: aws.String("meta.llama3-1-8b-instruct-v1:0"), Messages: aiUserText("stream me please")})
			if err != nil {
				t.Fatal(err)
			}
			stream := out.GetStream()
			defer stream.Close()
			var text strings.Builder
			var deltas, stops int
			var meta *types.ConverseStreamMetadataEvent
			var stop types.StopReason
			for ev := range stream.Events() {
				switch e := ev.(type) {
				case *types.ConverseStreamOutputMemberContentBlockDelta:
					deltas++
					text.WriteString(e.Value.Delta.(*types.ContentBlockDeltaMemberText).Value)
				case *types.ConverseStreamOutputMemberContentBlockStop:
					stops++
				case *types.ConverseStreamOutputMemberMessageStop:
					stop = e.Value.StopReason
				case *types.ConverseStreamOutputMemberMetadata:
					meta = &e.Value
				}
			}
			if err := stream.Err(); err != nil {
				t.Fatal(err)
			}
			if text.String() != aiAnswer(t, want) || deltas < 2 || stops != 1 || stop != types.StopReasonEndTurn || meta == nil || aws.ToInt32(meta.Usage.TotalTokens) == 0 {
				t.Fatalf("stream: %q (want %q) deltas %d stops %d stop %v meta %+v", text.String(), aiAnswer(t, want), deltas, stops, stop, meta)
			}
		})
	}
}

// aiInvoke calls InvokeModel with a JSON body and decodes the JSON aiAnswer.
func aiInvoke(t *testing.T, c *bedrockruntime.Client, model string, body map[string]any) map[string]any {
	t.Helper()
	b, _ := json.Marshal(body)
	out, err := c.InvokeModel(context.Background(), &bedrockruntime.InvokeModelInput{ModelId: aws.String(model), Body: b, ContentType: aws.String("application/json")})
	if err != nil {
		t.Fatalf("%s: %v", model, err)
	}
	var m map[string]any
	if err := json.Unmarshal(out.Body, &m); err != nil {
		t.Fatalf("%s: %v: %s", model, err, out.Body)
	}
	return m
}

func TestInvokeModelFormats(t *testing.T) {
	fm := aiNewFakeModel(t)
	c := bedrockruntime.NewFromConfig(aiStart(t, pogi.Config{AIEndpoint: fm.URL, AIModels: map[string]string{"*": "m"}}))
	const want = "local says: ping"

	m := aiInvoke(t, c, "anthropic.claude-3-sonnet-20240229-v1:0", map[string]any{"anthropic_version": "bedrock-2023-05-31", "max_tokens": 100, "system": "s",
		"messages": []any{map[string]any{"role": "user", "content": []any{map[string]any{"type": "text", "text": "ping"}}}}})
	cb := m["content"].([]any)[0].(map[string]any)
	if cb["text"] != want || m["role"] != "assistant" || m["type"] != "message" || m["stop_reason"] != "end_turn" || m["usage"].(map[string]any)["input_tokens"] != 11.0 {
		t.Fatalf("anthropic: %v", m)
	}
	// a cross-region inference profile id is the same model
	m = aiInvoke(t, c, "us.anthropic.claude-3-sonnet-20240229-v1:0", map[string]any{"max_tokens": 10, "messages": []any{map[string]any{"role": "user", "content": "ping"}}})
	if m["content"].([]any)[0].(map[string]any)["text"] != want {
		t.Fatalf("profile: %v", m)
	}

	m = aiInvoke(t, c, "amazon.titan-text-express-v1", map[string]any{"inputText": "ping", "textGenerationConfig": map[string]any{"maxTokenCount": 20}})
	r := m["results"].([]any)[0].(map[string]any)
	if r["outputText"] != want || r["completionReason"] != "FINISH" || m["inputTextTokenCount"] != 11.0 {
		t.Fatalf("titan: %v", m)
	}
	m = aiInvoke(t, c, "meta.llama3-8b-instruct-v1:0", map[string]any{"prompt": "ping", "max_gen_len": 20})
	if m["generation"] != want || m["stop_reason"] != "stop" || m["generation_token_count"] != 5.0 {
		t.Fatalf("llama: %v", m)
	}
	m = aiInvoke(t, c, "mistral.mistral-7b-instruct-v0:2", map[string]any{"prompt": "ping", "max_tokens": 20})
	if m["outputs"].([]any)[0].(map[string]any)["text"] != want {
		t.Fatalf("mistral: %v", m)
	}
	m = aiInvoke(t, c, "mistral.mistral-large-2407-v1:0", map[string]any{"messages": []any{map[string]any{"role": "user", "content": "ping"}}})
	if m["choices"].([]any)[0].(map[string]any)["message"].(map[string]any)["content"] != want {
		t.Fatalf("mistral chat: %v", m)
	}
	m = aiInvoke(t, c, "cohere.command-text-v14", map[string]any{"prompt": "ping", "max_tokens": 20})
	if m["generations"].([]any)[0].(map[string]any)["text"] != want {
		t.Fatalf("cohere: %v", m)
	}
	m = aiInvoke(t, c, "cohere.command-r-v1:0", map[string]any{"message": "ping", "chat_history": []any{map[string]any{"role": "USER", "message": "earlier"}}})
	if m["text"] != want || m["finish_reason"] != "COMPLETE" {
		t.Fatalf("command r: %v", m)
	}
	m = aiInvoke(t, c, "amazon.nova-pro-v1:0", map[string]any{"messages": []any{map[string]any{"role": "user", "content": []any{map[string]any{"text": "ping"}}}}})
	if m["output"].(map[string]any)["message"].(map[string]any)["content"].([]any)[0].(map[string]any)["text"] != want || m["stopReason"] != "end_turn" {
		t.Fatalf("nova: %v", m)
	}
	if last := fm.reqs[len(fm.reqs)-1]; last["model"] != "m" {
		t.Fatalf("model sent: %v", last)
	}

	// Errors are the typed ones the SDK branches on.
	for name, in := range map[string]struct{ model, body string }{
		"unknown model": {"acme.unknown-v1", `{"prompt":"x"}`},
		"bad body":      {"amazon.titan-text-express-v1", `not json`},
		"no prompt":     {"meta.llama3-8b-instruct-v1:0", `{}`},
	} {
		_, err := c.InvokeModel(context.Background(), &bedrockruntime.InvokeModelInput{ModelId: aws.String(in.model), Body: []byte(in.body)})
		var ve *types.ValidationException
		if !errors.As(err, &ve) {
			t.Errorf("%s: %T %v", name, err, err)
		}
	}
}

func TestInvokeModelWithResponseStream(t *testing.T) {
	ctx := context.Background()
	c := bedrockruntime.NewFromConfig(aiStart(t, pogi.Config{}))
	run := func(model string, body map[string]any) []map[string]any {
		b, _ := json.Marshal(body)
		out, err := c.InvokeModelWithResponseStream(ctx, &bedrockruntime.InvokeModelWithResponseStreamInput{ModelId: aws.String(model), Body: b})
		if err != nil {
			t.Fatal(err)
		}
		stream := out.GetStream()
		defer stream.Close()
		var chunks []map[string]any
		for ev := range stream.Events() {
			ch, ok := ev.(*types.ResponseStreamMemberChunk)
			if !ok {
				t.Fatalf("event %T", ev)
			}
			var m map[string]any
			if err := json.Unmarshal(ch.Value.Bytes, &m); err != nil {
				t.Fatal(err)
			}
			chunks = append(chunks, m)
		}
		if err := stream.Err(); err != nil {
			t.Fatal(err)
		}
		return chunks
	}

	full := aiInvoke(t, c, "anthropic.claude-3-haiku-20240307-v1:0", map[string]any{"max_tokens": 10, "messages": []any{map[string]any{"role": "user", "content": "go"}}})
	want := full["content"].([]any)[0].(map[string]any)["text"].(string)
	var got strings.Builder
	var types_ []string
	for _, m := range run("anthropic.claude-3-haiku-20240307-v1:0", map[string]any{"max_tokens": 10, "messages": []any{map[string]any{"role": "user", "content": "go"}}}) {
		types_ = append(types_, m["type"].(string))
		if d, ok := m["delta"].(map[string]any); ok && d["text"] != nil {
			got.WriteString(d["text"].(string))
		}
	}
	if got.String() != want || types_[0] != "message_start" || types_[len(types_)-1] != "message_stop" || len(types_) < 6 {
		t.Fatalf("anthropic stream: %q (want %q) %v", got.String(), want, types_)
	}

	got.Reset()
	chunks := run("amazon.titan-text-express-v1", map[string]any{"inputText": "go"})
	for _, m := range chunks {
		got.WriteString(m["outputText"].(string))
	}
	if last := chunks[len(chunks)-1]; got.String() == "" || last["completionReason"] != "FINISH" || last["amazon-bedrock-invocationMetrics"] == nil || chunks[0]["completionReason"] != nil {
		t.Fatalf("titan stream: %q %v", got.String(), chunks)
	}
	got.Reset()
	for _, m := range run("meta.llama3-8b-instruct-v1:0", map[string]any{"prompt": "go"}) {
		got.WriteString(m["generation"].(string))
	}
	if !strings.Contains(got.String(), "canned") {
		t.Fatalf("llama stream: %q", got.String())
	}
}

func TestEmbeddings(t *testing.T) {
	c := bedrockruntime.NewFromConfig(aiStart(t, pogi.Config{}))
	vec := func(m map[string]any, key string) []any { return m[key].([]any) }

	a, b := aiInvoke(t, c, "amazon.titan-embed-text-v1", map[string]any{"inputText": "hello"}), aiInvoke(t, c, "amazon.titan-embed-text-v1", map[string]any{"inputText": "hello"})
	other := aiInvoke(t, c, "amazon.titan-embed-text-v1", map[string]any{"inputText": "world"})
	if len(vec(a, "embedding")) != 1536 || a["inputTextTokenCount"] != 2.0 || vec(a, "embedding")[0] != vec(b, "embedding")[0] || vec(a, "embedding")[0] == vec(other, "embedding")[0] {
		t.Fatalf("titan v1: dim %d", len(vec(a, "embedding")))
	}
	if v2 := aiInvoke(t, c, "amazon.titan-embed-text-v2:0", map[string]any{"inputText": "hello", "dimensions": 256}); len(vec(v2, "embedding")) != 256 {
		t.Fatalf("titan v2 dimensions: %d", len(vec(v2, "embedding")))
	}
	if v2 := aiInvoke(t, c, "amazon.titan-embed-text-v2:0", map[string]any{"inputText": "hello"}); len(vec(v2, "embedding")) != 1024 {
		t.Fatalf("titan v2 default: %d", len(vec(v2, "embedding")))
	}
	co := aiInvoke(t, c, "cohere.embed-english-v3", map[string]any{"texts": []any{"a", "b"}, "input_type": "search_document"})
	embs := co["embeddings"].([]any)
	if len(embs) != 2 || len(embs[0].([]any)) != 1024 || co["response_type"] != "embeddings_floats" || embs[0].([]any)[0] == embs[1].([]any)[0] {
		t.Fatalf("cohere: %d vectors", len(embs))
	}
	// Converse is not for embedding models.
	_, err := c.Converse(context.Background(), &bedrockruntime.ConverseInput{ModelId: aws.String("amazon.titan-embed-text-v1"), Messages: aiUserText("x")})
	var ae smithy.APIError
	if !errors.As(err, &ae) || ae.ErrorCode() != "ValidationException" {
		t.Fatalf("converse on embedding model: %v", err)
	}
}

func TestBedrockRuntimeMisc(t *testing.T) {
	ctx := context.Background()
	c := bedrockruntime.NewFromConfig(aiStart(t, pogi.Config{}))
	g, err := c.ApplyGuardrail(ctx, &bedrockruntime.ApplyGuardrailInput{GuardrailIdentifier: aws.String("gr-1"), GuardrailVersion: aws.String("DRAFT"), Source: types.GuardrailContentSourceInput,
		Content: []types.GuardrailContentBlock{&types.GuardrailContentBlockMemberText{Value: types.GuardrailTextBlock{Text: aws.String("hi")}}}})
	if err != nil || g.Action != types.GuardrailActionNone {
		t.Fatalf("apply guardrail: %v %+v", err, g)
	}
}
