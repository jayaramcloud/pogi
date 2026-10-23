package generic

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/nrndev/pogi/internal/aimodel"
	"github.com/nrndev/pogi/internal/core"
)

// Bedrock Runtime: InvokeModel and Converse (and their streams) answer through the local model
// aiGateway, in each model family's own body format.

// turn is one answered request: the model id, what was asked, and the answer.
type turn struct {
	id string
	q  aimodel.Request
	r  aimodel.Response
	in map[string]any // the request body
}

// family is one model family's body format: how to read a request, and how to write the whole
// answer or the chunks of a streamed one.
type family struct {
	req    func(in map[string]any) (aimodel.Request, error)
	resp   func(t turn) map[string]any
	chunks func(t turn) []map[string]any
}

var errMalformed = fmt.Errorf("Malformed input request, please reformat your input and try again.")

// regionPrefixes start the id of a cross-region inference profile ("us.anthropic.claude-...").
var regionPrefixes = map[string]bool{"us": true, "eu": true, "apac": true, "global": true, "us-gov": true, "jp": true, "au": true, "ca": true}

// modelKey is a model id without ARN or inference-profile decoration.
func modelKey(id string) string {
	if strings.HasPrefix(id, "arn:") {
		id = id[strings.LastIndex(id, "/")+1:]
	}
	if p, rest, ok := strings.Cut(id, "."); ok && regionPrefixes[p] {
		id = rest
	}
	return id
}

func familyOf(id string) *family {
	switch k := modelKey(id); {
	case strings.HasPrefix(k, "anthropic."):
		return anthropicFamily
	case strings.HasPrefix(k, "amazon.nova"):
		return novaFamily
	case strings.HasPrefix(k, "amazon.titan"):
		return titanFamily
	case strings.HasPrefix(k, "meta."):
		return llamaFamily
	case strings.HasPrefix(k, "mistral."):
		return mistralFamily
	case strings.HasPrefix(k, "cohere.command-r"):
		return commandRFamily
	case strings.HasPrefix(k, "cohere.command"):
		return cohereFamily
	}
	return nil
}

func isEmbedding(id string) bool {
	k := modelKey(id)
	return strings.HasPrefix(k, "amazon.titan-embed") || strings.HasPrefix(k, "cohere.embed") || strings.HasPrefix(k, "amazon.nova-2-multimodal-embed")
}

// embedding answers an embeddings request: stable vectors of the model's dimension.
func embedding(id string, in map[string]any) (map[string]any, bool) {
	k := modelKey(id)
	switch {
	case strings.HasPrefix(k, "amazon.titan-embed"):
		text := str(in["inputText"])
		dim := 1536
		if strings.Contains(k, "-v2") {
			dim = 1024
			if d := int(aiNum(in["dimensions"])); d > 0 {
				dim = d
			}
		}
		v := aimodel.Embed(text, dim)
		out := map[string]any{"embedding": v, "inputTextTokenCount": aimodel.Tokens(text)}
		if strings.Contains(k, "-v2") {
			out["embeddingsByType"] = map[string]any{"float": v}
		}
		return out, true
	case strings.HasPrefix(k, "cohere.embed"):
		texts := aiStrList(in["texts"])
		dim := 1024
		if strings.Contains(k, "embed-v4") {
			dim = 1536
			if d := int(aiNum(in["output_dimension"])); d > 0 {
				dim = d
			}
		}
		vs := make([][]float64, len(texts))
		for i, t := range texts {
			vs[i] = aimodel.Embed(t, dim)
		}
		sum := sha256.Sum256([]byte(strings.Join(texts, "\x00")))
		out := map[string]any{"id": fmt.Sprintf("%x-%x-%x-%x-%x", sum[:4], sum[4:6], sum[6:8], sum[8:10], sum[10:16]), "texts": texts}
		if strings.Contains(k, "embed-v4") || len(asList(in["embedding_types"])) > 0 {
			out["embeddings"], out["response_type"] = map[string]any{"float": vs}, "embeddings_by_type"
		} else {
			out["embeddings"], out["response_type"] = vs, "embeddings_floats"
		}
		return out, true
	}
	return nil, false
}

// ask runs a request through the gateway; a failing local model is Bedrock's ModelErrorException.
func ask(c *Call, id string, q aimodel.Request) (aimodel.Response, error) {
	r, err := aiGateway(c).Chat(aiCtx(c), id, q)
	if err != nil {
		return r, core.Errorf(424, "ModelErrorException", "%v", err)
	}
	return r, nil
}

func init() {
	Override("bedrock-runtime", "InvokeModel", func(c *Call) (map[string]any, error) {
		id := str(c.In["modelId"])
		in, ok := aiBlobJSON(c.In["body"])
		if !ok {
			return nil, c.Invalid("%v", errMalformed)
		}
		if out, ok := embedding(id, in); ok {
			return map[string]any{"body": aiJsonBlob(out), "contentType": "application/json"}, nil
		}
		f := familyOf(id)
		if f == nil {
			return nil, c.Invalid("The provided model identifier is invalid.")
		}
		q, err := f.req(in)
		if err != nil {
			return nil, c.Invalid("%v", err)
		}
		r, err := ask(c, id, q)
		if err != nil {
			return nil, err
		}
		return map[string]any{"body": aiJsonBlob(f.resp(turn{id, q, r, in})), "contentType": "application/json"}, nil
	})

	Override("bedrock-runtime", "InvokeModelWithResponseStream", func(c *Call) (map[string]any, error) {
		id := str(c.In["modelId"])
		in, ok := aiBlobJSON(c.In["body"])
		if !ok {
			return nil, c.Invalid("%v", errMalformed)
		}
		f := familyOf(id)
		if f == nil {
			return nil, c.Invalid("The provided model identifier is invalid.")
		}
		q, err := f.req(in)
		if err != nil {
			return nil, c.Invalid("%v", err)
		}
		r, err := ask(c, id, q)
		if err != nil {
			return nil, err
		}
		var evs []aiEvent
		for _, ch := range f.chunks(turn{id, q, r, in}) {
			b, _ := json.Marshal(ch)
			evs = append(evs, aiEvent{"chunk", map[string]any{"bytes": b}}) // []byte marshals as base64
		}
		c.Stream = func(w http.ResponseWriter) {
			aiWriteEvents(w, map[string]string{"X-Amzn-Bedrock-Content-Type": "application/json"}, evs)
		}
		return nil, nil
	})

	Override("bedrock-runtime", "Converse", func(c *Call) (map[string]any, error) {
		id := str(c.In["modelId"])
		q, err := converseReq(c, id)
		if err != nil {
			return nil, err
		}
		r, err := ask(c, id, q)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"output":     map[string]any{"message": map[string]any{"role": "assistant", "content": converseContent(r)}},
			"stopReason": r.Stop,
			"usage":      usage(r),
			"metrics":    map[string]any{"latencyMs": 1.0},
		}, nil
	})

	Override("bedrock-runtime", "ConverseStream", func(c *Call) (map[string]any, error) {
		id := str(c.In["modelId"])
		q, err := converseReq(c, id)
		if err != nil {
			return nil, err
		}
		r, err := ask(c, id, q)
		if err != nil {
			return nil, err
		}
		evs := []aiEvent{{"messageStart", map[string]any{"role": "assistant"}}}
		idx := 0
		if r.Text != "" || len(r.ToolCalls) == 0 {
			for _, w := range aiPieces(r.Text) {
				evs = append(evs, aiEvent{"contentBlockDelta", map[string]any{"contentBlockIndex": idx, "delta": map[string]any{"text": w}}})
			}
			evs = append(evs, aiEvent{"contentBlockStop", map[string]any{"contentBlockIndex": idx}})
			idx++
		}
		for _, tc := range r.ToolCalls {
			evs = append(evs,
				aiEvent{"contentBlockStart", map[string]any{"contentBlockIndex": idx, "start": map[string]any{"toolUse": map[string]any{"toolUseId": tc.ID, "name": tc.Name}}}},
				aiEvent{"contentBlockDelta", map[string]any{"contentBlockIndex": idx, "delta": map[string]any{"toolUse": map[string]any{"input": tc.Arguments}}}},
				aiEvent{"contentBlockStop", map[string]any{"contentBlockIndex": idx}})
			idx++
		}
		evs = append(evs,
			aiEvent{"messageStop", map[string]any{"stopReason": r.Stop}},
			aiEvent{"metadata", map[string]any{"usage": usage(r), "metrics": map[string]any{"latencyMs": 1}}})
		c.Stream = func(w http.ResponseWriter) { aiWriteEvents(w, nil, evs) }
		return nil, nil
	})

	// The guardrail is not evaluated: every input passes.
	Override("bedrock-runtime", "ApplyGuardrail", func(c *Call) (map[string]any, error) {
		return map[string]any{"action": "NONE", "outputs": []any{}, "assessments": []any{},
			"usage": map[string]any{"topicPolicyUnits": 0.0, "contentPolicyUnits": 0.0, "wordPolicyUnits": 0.0,
				"sensitiveInformationPolicyUnits": 0.0, "sensitiveInformationPolicyFreeUnits": 0.0, "contextualGroundingPolicyUnits": 0.0}}, nil
	})

	Override("bedrock-runtime", "CountTokens", func(c *Call) (map[string]any, error) {
		b, _ := json.Marshal(c.In["input"])
		return map[string]any{"inputTokens": float64(aimodel.Tokens(string(b)))}, nil
	})
}

func usage(r aimodel.Response) map[string]any {
	return map[string]any{"inputTokens": r.InputTokens, "outputTokens": r.OutputTokens, "totalTokens": r.InputTokens + r.OutputTokens}
}

// converseContent is the answer as Converse content blocks.
func converseContent(r aimodel.Response) []any {
	var out []any
	if r.Text != "" || len(r.ToolCalls) == 0 {
		out = append(out, map[string]any{"text": r.Text})
	}
	for _, tc := range r.ToolCalls {
		var in any = map[string]any{}
		json.Unmarshal([]byte(tc.Arguments), &in)
		out = append(out, map[string]any{"toolUse": map[string]any{"toolUseId": tc.ID, "name": tc.Name, "input": in}})
	}
	return out
}

// converseReq turns a Converse or ConverseStream input into a chat request.
func converseReq(c *Call, id string) (aimodel.Request, error) {
	var q aimodel.Request
	if isEmbedding(id) {
		return q, c.Invalid("This model doesn't support the converse operation.")
	}
	var sys []string
	for _, s := range asList(c.In["system"]) {
		if t := str(aiAmap(s)["text"]); t != "" {
			sys = append(sys, t)
		}
	}
	q.System = strings.Join(sys, "\n")
	for _, m := range asList(c.In["messages"]) {
		mm := aiAmap(m)
		msg := aimodel.Message{Role: str(mm["role"])}
		var text []string
		for _, b := range asList(mm["content"]) {
			bm := aiAmap(b)
			switch {
			case bm["text"] != nil:
				text = append(text, str(bm["text"]))
			case bm["toolUse"] != nil:
				tu := aiAmap(bm["toolUse"])
				args, _ := json.Marshal(tu["input"])
				msg.ToolCalls = append(msg.ToolCalls, aimodel.ToolCall{ID: str(tu["toolUseId"]), Name: str(tu["name"]), Arguments: string(args)})
			case bm["toolResult"] != nil:
				tr := aiAmap(bm["toolResult"])
				var parts []string
				for _, p := range asList(tr["content"]) {
					pm := aiAmap(p)
					if pm["text"] != nil {
						parts = append(parts, str(pm["text"]))
					} else if pm["json"] != nil {
						j, _ := json.Marshal(pm["json"])
						parts = append(parts, string(j))
					}
				}
				q.Messages = append(q.Messages, aimodel.Message{Role: "tool", ToolCallID: str(tr["toolUseId"]), Content: strings.Join(parts, "\n")})
			default: // images, documents, video: only their presence reaches the model
				text = append(text, "[attachment]")
			}
		}
		if msg.Content = strings.Join(text, "\n"); msg.Content != "" || len(msg.ToolCalls) > 0 {
			q.Messages = append(q.Messages, msg)
		}
	}
	ic := aiAmap(c.In["inferenceConfig"])
	q.MaxTokens, q.Temperature, q.TopP, q.Stop = int(aiNum(ic["maxTokens"])), aiOptNum(ic["temperature"]), aiOptNum(ic["topP"]), aiStrList(ic["stopSequences"])
	for _, t := range asList(aiAmap(c.In["toolConfig"])["tools"]) {
		if ts := aiAmap(aiAmap(t)["toolSpec"]); ts != nil {
			q.Tools = append(q.Tools, aimodel.Tool{Name: str(ts["name"]), Description: str(ts["description"]), Schema: aiAmap(ts["inputSchema"])["json"]})
		}
	}
	return q, nil
}

// ---- model families

func metrics(t turn) map[string]any {
	return map[string]any{"inputTokenCount": t.r.InputTokens, "outputTokenCount": t.r.OutputTokens, "invocationLatency": 1, "firstByteLatency": 1}
}

// texts is a prompt body field that is either a string or Anthropic-style content blocks.
func blocksText(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	var parts []string
	for _, b := range asList(v) {
		if s, ok := b.(string); ok {
			parts = append(parts, s)
		} else if t := str(aiAmap(b)["text"]); t != "" {
			parts = append(parts, t)
		}
	}
	return strings.Join(parts, "\n")
}

// prompted reads the prompt-style families (Titan aside): one prompt string and sampling members.
func prompted(in map[string]any, field, maxTokens, temp, topP, aiStop string) (aimodel.Request, error) {
	p := str(in[field])
	if p == "" {
		return aimodel.Request{}, errMalformed
	}
	return aimodel.Request{Messages: []aimodel.Message{{Role: "user", Content: p}}, MaxTokens: int(aiNum(in[maxTokens])),
		Temperature: aiOptNum(in[temp]), TopP: aiOptNum(in[topP]), Stop: aiStrList(in[aiStop])}, nil
}

func finish(t turn, done, cut string) string {
	if t.r.Stop == "max_tokens" {
		return cut
	}
	return done
}

var anthropicFamily = &family{
	req: func(in map[string]any) (aimodel.Request, error) {
		q := aimodel.Request{System: blocksText(in["system"]), MaxTokens: int(aiNum(in["max_tokens"])),
			Temperature: aiOptNum(in["temperature"]), TopP: aiOptNum(in["top_p"]), Stop: aiStrList(in["stop_sequences"])}
		for _, m := range asList(in["messages"]) {
			mm := aiAmap(m)
			if s, ok := mm["content"].(string); ok {
				q.Messages = append(q.Messages, aimodel.Message{Role: str(mm["role"]), Content: s})
				continue
			}
			msg := aimodel.Message{Role: str(mm["role"])}
			var text []string
			for _, b := range asList(mm["content"]) {
				bm := aiAmap(b)
				switch str(bm["type"]) {
				case "text":
					text = append(text, str(bm["text"]))
				case "tool_use":
					args, _ := json.Marshal(bm["input"])
					msg.ToolCalls = append(msg.ToolCalls, aimodel.ToolCall{ID: str(bm["id"]), Name: str(bm["name"]), Arguments: string(args)})
				case "tool_result":
					q.Messages = append(q.Messages, aimodel.Message{Role: "tool", ToolCallID: str(bm["tool_use_id"]), Content: blocksText(bm["content"])})
				default:
					text = append(text, "[attachment]")
				}
			}
			if msg.Content = strings.Join(text, "\n"); msg.Content != "" || len(msg.ToolCalls) > 0 {
				q.Messages = append(q.Messages, msg)
			}
		}
		if len(q.Messages) == 0 {
			return q, errMalformed
		}
		for _, t := range asList(in["tools"]) {
			tm := aiAmap(t)
			q.Tools = append(q.Tools, aimodel.Tool{Name: str(tm["name"]), Description: str(tm["description"]), Schema: tm["input_schema"]})
		}
		return q, nil
	},
	resp: func(t turn) map[string]any {
		var content []any
		if t.r.Text != "" || len(t.r.ToolCalls) == 0 {
			content = append(content, map[string]any{"type": "text", "text": t.r.Text})
		}
		for _, tc := range t.r.ToolCalls {
			var in any = map[string]any{}
			json.Unmarshal([]byte(tc.Arguments), &in)
			content = append(content, map[string]any{"type": "tool_use", "id": tc.ID, "name": tc.Name, "input": in})
		}
		return map[string]any{"id": msgID(t), "type": "message", "role": "assistant", "model": modelKey(t.id), "content": content,
			"stop_reason": t.r.Stop, "stop_sequence": nil, "usage": map[string]any{"input_tokens": t.r.InputTokens, "output_tokens": t.r.OutputTokens}}
	},
	chunks: func(t turn) []map[string]any {
		out := []map[string]any{{"type": "message_start", "message": map[string]any{"id": msgID(t), "type": "message", "role": "assistant",
			"model": modelKey(t.id), "content": []any{}, "stop_reason": nil, "stop_sequence": nil,
			"usage": map[string]any{"input_tokens": t.r.InputTokens, "output_tokens": 1}}}}
		idx := 0
		if t.r.Text != "" || len(t.r.ToolCalls) == 0 {
			out = append(out, map[string]any{"type": "content_block_start", "index": idx, "content_block": map[string]any{"type": "text", "text": ""}})
			for _, w := range aiPieces(t.r.Text) {
				out = append(out, map[string]any{"type": "content_block_delta", "index": idx, "delta": map[string]any{"type": "text_delta", "text": w}})
			}
			out = append(out, map[string]any{"type": "content_block_stop", "index": idx})
			idx++
		}
		for _, tc := range t.r.ToolCalls {
			out = append(out,
				map[string]any{"type": "content_block_start", "index": idx, "content_block": map[string]any{"type": "tool_use", "id": tc.ID, "name": tc.Name, "input": map[string]any{}}},
				map[string]any{"type": "content_block_delta", "index": idx, "delta": map[string]any{"type": "input_json_delta", "partial_json": tc.Arguments}},
				map[string]any{"type": "content_block_stop", "index": idx})
			idx++
		}
		return append(out,
			map[string]any{"type": "message_delta", "delta": map[string]any{"stop_reason": t.r.Stop, "stop_sequence": nil}, "usage": map[string]any{"output_tokens": t.r.OutputTokens}},
			map[string]any{"type": "message_stop", "amazon-bedrock-invocationMetrics": metrics(t)})
	},
}

func msgID(t turn) string {
	sum := sha256.Sum256([]byte(t.id + t.q.Text()))
	return fmt.Sprintf("msg_bdrk_%x", sum[:12])
}

var titanFamily = &family{
	req: func(in map[string]any) (aimodel.Request, error) {
		cfg := aiAmap(in["textGenerationConfig"])
		q, err := prompted(in, "inputText", "", "", "", "")
		q.MaxTokens, q.Temperature, q.TopP, q.Stop = int(aiNum(cfg["maxTokenCount"])), aiOptNum(cfg["temperature"]), aiOptNum(cfg["topP"]), aiStrList(cfg["stopSequences"])
		return q, err
	},
	resp: func(t turn) map[string]any {
		return map[string]any{"inputTextTokenCount": t.r.InputTokens, "results": []any{map[string]any{"tokenCount": t.r.OutputTokens,
			"outputText": t.r.Text, "completionReason": finish(t, "FINISH", "LENGTH")}}}
	},
	chunks: func(t turn) []map[string]any {
		ws := aiPieces(t.r.Text)
		var out []map[string]any
		for i, w := range ws {
			c := map[string]any{"outputText": w, "index": 0, "totalOutputTextTokenCount": nil, "completionReason": nil}
			if i == len(ws)-1 {
				c["totalOutputTextTokenCount"], c["completionReason"] = t.r.OutputTokens, finish(t, "FINISH", "LENGTH")
				c["inputTextTokenCount"], c["amazon-bedrock-invocationMetrics"] = t.r.InputTokens, metrics(t)
			}
			out = append(out, c)
		}
		return out
	},
}

// novaFamily is the Converse-shaped body of the Amazon Nova models.
var novaFamily = &family{
	req: func(in map[string]any) (aimodel.Request, error) {
		var q aimodel.Request
		q.System = blocksText(in["system"])
		for _, m := range asList(in["messages"]) {
			mm := aiAmap(m)
			q.Messages = append(q.Messages, aimodel.Message{Role: str(mm["role"]), Content: blocksText(mm["content"])})
		}
		if len(q.Messages) == 0 {
			return q, errMalformed
		}
		ic := aiAmap(in["inferenceConfig"])
		q.MaxTokens, q.Temperature, q.TopP, q.Stop = int(aiNum(ic["max_new_tokens"])), aiOptNum(ic["temperature"]), aiOptNum(ic["top_p"]), aiStrList(ic["stopSequences"])
		return q, nil
	},
	resp: func(t turn) map[string]any {
		return map[string]any{"output": map[string]any{"message": map[string]any{"role": "assistant", "content": []any{map[string]any{"text": t.r.Text}}}},
			"stopReason": t.r.Stop, "usage": usage(t.r)}
	},
	chunks: func(t turn) []map[string]any {
		out := []map[string]any{{"messageStart": map[string]any{"role": "assistant"}}}
		for _, w := range aiPieces(t.r.Text) {
			out = append(out, map[string]any{"contentBlockDelta": map[string]any{"contentBlockIndex": 0, "delta": map[string]any{"text": w}}})
		}
		return append(out, map[string]any{"contentBlockStop": map[string]any{"contentBlockIndex": 0}},
			map[string]any{"messageStop": map[string]any{"stopReason": t.r.Stop}},
			map[string]any{"amazon-bedrock-invocationMetrics": metrics(t)})
	},
}

var llamaFamily = &family{
	req: func(in map[string]any) (aimodel.Request, error) {
		return prompted(in, "prompt", "max_gen_len", "temperature", "top_p", "")
	},
	resp: func(t turn) map[string]any {
		return map[string]any{"generation": t.r.Text, "prompt_token_count": t.r.InputTokens, "generation_token_count": t.r.OutputTokens,
			"stop_reason": finish(t, "stop", "length")}
	},
	chunks: func(t turn) []map[string]any {
		ws := aiPieces(t.r.Text)
		var out []map[string]any
		for i, w := range ws {
			c := map[string]any{"generation": w, "prompt_token_count": nil, "generation_token_count": i + 1, "stop_reason": nil}
			if i == len(ws)-1 {
				c["prompt_token_count"], c["generation_token_count"], c["stop_reason"] = t.r.InputTokens, t.r.OutputTokens, finish(t, "stop", "length")
				c["amazon-bedrock-invocationMetrics"] = metrics(t)
			}
			out = append(out, c)
		}
		return out
	},
}

// mistralFamily: the prompt-style body, or the chat-style body (messages) of the newer models.
var mistralFamily = &family{
	req: func(in map[string]any) (aimodel.Request, error) {
		if in["messages"] == nil {
			return prompted(in, "prompt", "max_tokens", "temperature", "top_p", "stop")
		}
		q := aimodel.Request{MaxTokens: int(aiNum(in["max_tokens"])), Temperature: aiOptNum(in["temperature"]), TopP: aiOptNum(in["top_p"]), Stop: aiStrList(in["stop"])}
		for _, m := range asList(in["messages"]) {
			mm := aiAmap(m)
			if r := str(mm["role"]); r == "system" {
				q.System = blocksText(mm["content"])
			} else {
				q.Messages = append(q.Messages, aimodel.Message{Role: r, Content: blocksText(mm["content"])})
			}
		}
		if len(q.Messages) == 0 {
			return q, errMalformed
		}
		return q, nil
	},
	resp: func(t turn) map[string]any {
		if t.in["messages"] != nil {
			return mistralChat(t, t.r.Text)
		}
		return map[string]any{"outputs": []any{map[string]any{"text": t.r.Text, "stop_reason": finish(t, "stop", "length")}}}
	},
	chunks: func(t turn) []map[string]any {
		ws := aiPieces(t.r.Text)
		var out []map[string]any
		for i, w := range ws {
			var reason any
			if i == len(ws)-1 {
				reason = finish(t, "stop", "length")
			}
			out = append(out, map[string]any{"outputs": []any{map[string]any{"text": w, "stop_reason": reason}}})
		}
		return out
	},
}

func mistralChat(t turn, text string) map[string]any {
	return map[string]any{"choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": text},
		"stop_reason": finish(t, "stop", "length")}}}
}

// cohereFamily is Command (prompt body); commandRFamily is Command R (message body).
var cohereFamily = &family{
	req: func(in map[string]any) (aimodel.Request, error) {
		return prompted(in, "prompt", "max_tokens", "temperature", "p", "stop_sequences")
	},
	resp: func(t turn) map[string]any {
		return map[string]any{"id": msgID(t), "prompt": t.q.Messages[0].Content,
			"generations": []any{map[string]any{"id": msgID(t) + "-0", "text": t.r.Text, "finish_reason": finish(t, "COMPLETE", "MAX_TOKENS")}}}
	},
	chunks: func(t turn) []map[string]any {
		var out []map[string]any
		for _, w := range aiPieces(t.r.Text) {
			out = append(out, map[string]any{"event_type": "text-generation", "text": w, "is_finished": false})
		}
		return append(out, map[string]any{"is_finished": true, "finish_reason": finish(t, "COMPLETE", "MAX_TOKENS"),
			"response": map[string]any{"id": msgID(t), "prompt": t.q.Messages[0].Content, "generations": []any{map[string]any{"id": msgID(t) + "-0", "text": t.r.Text}}}})
	},
}

var commandRFamily = &family{
	req: func(in map[string]any) (aimodel.Request, error) {
		q, err := prompted(in, "message", "max_tokens", "temperature", "p", "stop_sequences")
		if err != nil {
			return q, err
		}
		q.System = str(in["preamble"])
		var hist []aimodel.Message
		for _, h := range asList(in["chat_history"]) {
			hm := aiAmap(h)
			role := "user"
			if strings.EqualFold(str(hm["role"]), "CHATBOT") {
				role = "assistant"
			}
			hist = append(hist, aimodel.Message{Role: role, Content: str(hm["message"])})
		}
		q.Messages = append(hist, q.Messages...)
		return q, nil
	},
	resp: func(t turn) map[string]any {
		return map[string]any{"response_id": msgID(t), "text": t.r.Text, "generation_id": msgID(t) + "-0", "finish_reason": finish(t, "COMPLETE", "MAX_TOKENS"),
			"chat_history": []any{}}
	},
	chunks: func(t turn) []map[string]any {
		var out []map[string]any
		for _, w := range aiPieces(t.r.Text) {
			out = append(out, map[string]any{"is_finished": false, "event_type": "text-generation", "text": w})
		}
		return append(out, map[string]any{"is_finished": true, "event_type": "stream-end", "finish_reason": finish(t, "COMPLETE", "MAX_TOKENS"),
			"response": map[string]any{"text": t.r.Text}})
	},
}
