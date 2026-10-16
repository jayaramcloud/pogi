package generic

import (
	"context"
	"encoding/base64"
	"encoding/json"

	"github.com/nrndev/pogi/internal/aimodel"
	"github.com/nrndev/pogi/internal/core"
)

// SageMaker Runtime: InvokeEndpoint on an endpoint that exists answers from the model aiGateway. A
// JSON body gets the Hugging Face style answer [{"generated_text": "..."}]; any other body gets
// plain text. Nothing is deployed behind the endpoint.
func init() {
	Override("sagemaker-runtime", "InvokeEndpoint", func(c *Call) (map[string]any, error) {
		name := str(c.In["EndpointName"])
		sm, _ := c.Hub().Service("sagemaker").(*Service)
		if sm == nil || sm.Get(c.Account, c.Region, "Endpoint", name) == nil {
			return nil, core.Errorf(400, "ValidationError", "Endpoint %s of account %s not found.", name, c.Account)
		}
		body, _ := base64.StdEncoding.DecodeString(str(c.In["Body"]))
		text, isJSON := prompt(body)
		r, err := aiGateway(c).Chat(context.Background(), name, aimodel.Request{Messages: []aimodel.Message{{Role: "user", Content: text}}})
		if err != nil {
			return nil, core.Errorf(424, "ModelError", "%v", err)
		}
		out, ct := []byte(r.Text), "text/plain"
		if isJSON {
			out, _ = json.Marshal([]any{map[string]any{"generated_text": r.Text}})
			ct = "application/json"
		}
		return map[string]any{"Body": base64.StdEncoding.EncodeToString(out), "ContentType": ct, "InvokedProductionVariant": "AllTraffic"}, nil
	})
}

// prompt is the text to answer in a request body: the usual prompt member of a JSON body, else the body.
func prompt(body []byte) (string, bool) {
	var v any
	if json.Unmarshal(body, &v) != nil {
		return string(body), false
	}
	if m, ok := v.(map[string]any); ok {
		for _, k := range []string{"inputs", "prompt", "inputText", "text", "input"} {
			if s, ok := m[k].(string); ok {
				return s, true
			}
		}
	}
	return string(body), true
}
