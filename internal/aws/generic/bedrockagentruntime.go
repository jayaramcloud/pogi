package generic

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/nrndev/pogi/internal/aimodel"
)

// Bedrock Agents runtime: InvokeAgent answers through the model aiGateway as an aiEvent stream, using
// the agent's instruction and foundation model, and keeps the conversation of a session. Retrieve
// and RetrieveAndGenerate answer from deterministic passages.
//
// ponytail: no action groups or knowledge bases are called by the agent, and InvokeInlineAgent is
// not reachable (its route is the same as UpdateAgent's, which Bedrock Agents owns).

// agentControl is the Bedrock Agents service of the hub, which owns the agents and knowledge bases.
func agentControl(c *Call) *Service {
	if h := c.Hub(); h != nil {
		s, _ := h.Service("bedrock-agent").(*Service)
		return s
	}
	return nil
}

func init() {
	Override("bedrock-agent-runtime", "InvokeAgent", func(c *Call) (map[string]any, error) {
		ctl := agentControl(c)
		agentID, aliasID, sid := str(c.In["agentId"]), str(c.In["agentAliasId"]), str(c.In["sessionId"])
		var agent *Resource
		if ctl != nil {
			agent = ctl.Get(c.Account, c.Region, "Agent", agentID)
		}
		if agent == nil {
			return nil, c.NotFound("Agent %s not found", agentID)
		}
		if ctl.Get(c.Account, c.Region, "AgentAlias", agentID+"/"+aliasID) == nil && aliasID != "TSTALIASID" {
			return nil, c.NotFound("Agent alias %s not found", aliasID)
		}
		sess := c.Get("Session", agentID, aliasID, sid)
		if sess == nil {
			sess = &Resource{Type: "Session", Key: agentID + "/" + aliasID + "/" + sid, Name: sid, Account: c.Account, Region: c.Region, Data: map[string]any{}}
		}
		q := aimodel.Request{System: str(agent.Data["instruction"])}
		for _, t := range asList(sess.Data["turns"]) {
			tm := aiAmap(t)
			q.Messages = append(q.Messages, aimodel.Message{Role: "user", Content: str(tm["user"])}, aimodel.Message{Role: "assistant", Content: str(tm["assistant"])})
		}
		q.Messages = append(q.Messages, aimodel.Message{Role: "user", Content: str(c.In["inputText"])})
		r, err := ask(c, str(agent.Data["foundationModel"]), q)
		if err != nil {
			return nil, err
		}
		sess.Data["turns"] = append(asList(sess.Data["turns"]), map[string]any{"user": str(c.In["inputText"]), "assistant": r.Text})
		c.Put(sess)

		var evs []aiEvent
		for _, w := range aiPieces(r.Text) {
			evs = append(evs, aiEvent{"chunk", map[string]any{"bytes": []byte(w)}})
		}
		hdr := map[string]string{"x-amzn-bedrock-agent-content-type": "text/plain", "x-amz-bedrock-agent-session-id": sid}
		if m := str(c.In["memoryId"]); m != "" {
			hdr["x-amz-bedrock-agent-memory-id"] = m
		}
		c.Stream = func(w http.ResponseWriter) { aiWriteEvents(w, hdr, evs) }
		return nil, nil
	})

	Override("bedrock-agent-runtime", "Retrieve", func(c *Call) (map[string]any, error) {
		id := str(c.In["knowledgeBaseId"])
		if ctl := agentControl(c); ctl == nil || ctl.Get(c.Account, c.Region, "KnowledgeBase", id) == nil {
			return nil, c.NotFound("Knowledge base %s not found", id)
		}
		n := int(aiNum(aiAmap(aiAmap(c.In["retrievalConfiguration"])["vectorSearchConfiguration"])["numberOfResults"]))
		if n <= 0 {
			n = 5
		}
		return map[string]any{"retrievalResults": passages(id, str(aiAmap(c.In["retrievalQuery"])["text"]), n)}, nil
	})

	Override("bedrock-agent-runtime", "RetrieveAndGenerate", func(c *Call) (map[string]any, error) {
		text := str(aiAmap(c.In["input"])["text"])
		cfg := aiAmap(c.In["retrieveAndGenerateConfiguration"])
		kb := aiAmap(cfg["knowledgeBaseConfiguration"])
		id, model := str(kb["knowledgeBaseId"]), str(kb["modelArn"])
		if ctl := agentControl(c); id != "" && (ctl == nil || ctl.Get(c.Account, c.Region, "KnowledgeBase", id) == nil) {
			return nil, c.NotFound("Knowledge base %s not found", id)
		}
		var refs []any
		var ctx []string
		for _, p := range passages(id, text, 3) {
			pm := aiAmap(p)
			ctx = append(ctx, str(aiAmap(pm["content"])["text"]))
			refs = append(refs, map[string]any{"content": pm["content"], "location": pm["location"]})
		}
		r, err := ask(c, model, aimodel.Request{System: "Answer from these passages:\n" + strings.Join(ctx, "\n"), Messages: []aimodel.Message{{Role: "user", Content: text}}})
		if err != nil {
			return nil, err
		}
		sid := str(c.In["sessionId"])
		if sid == "" {
			sum := sha256.Sum256([]byte(text))
			sid = fmt.Sprintf("%x", sum[:8])
		}
		return map[string]any{"sessionId": sid, "output": map[string]any{"text": r.Text},
			"citations": []any{map[string]any{"generatedResponsePart": map[string]any{"textResponsePart": map[string]any{"text": r.Text}}, "retrievedReferences": refs}}}, nil
	})
}

// passages are n made-up knowledge base results for a query: stable for the same knowledge base
// and query, best score first.
func passages(kb, query string, n int) []any {
	type hit struct {
		text  string
		score float64
		uri   string
	}
	hits := make([]hit, n)
	for i := range hits {
		sum := sha256.Sum256(fmt.Appendf(nil, "%s\x00%s\x00%d", kb, query, i))
		hits[i] = hit{
			text:  fmt.Sprintf("[pogi canned passage %x] Passage %d of the knowledge base about %q.", sum[:4], i+1, query),
			score: float64(500+binary.BigEndian.Uint32(sum[8:])%500) / 1000,
			uri:   fmt.Sprintf("s3://%s-docs/document-%x.txt", strings.ToLower(kb), sum[4:6]),
		}
	}
	sort.SliceStable(hits, func(i, j int) bool { return hits[i].score > hits[j].score })
	out := make([]any, n)
	for i, h := range hits {
		out[i] = map[string]any{"content": map[string]any{"type": "TEXT", "text": h.text}, "score": h.score,
			"location": map[string]any{"type": "S3", "s3Location": map[string]any{"uri": h.uri}}, "metadata": map[string]any{}}
	}
	return out
}
