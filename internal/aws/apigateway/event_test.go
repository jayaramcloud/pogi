package apigateway

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
)

// floci #3113: the REST event (and so the REQUEST authorizer event) names the API key that passed.
func TestRestEventAPIKey(t *testing.T) {
	r := httptest.NewRequest("GET", "/dev/k", nil)
	r.Header.Set("X-Api-Key", "secretvalue")
	var ev struct {
		RequestContext struct{ Identity map[string]any }
	}
	if err := json.Unmarshal(restEvent(&call{r: r, keyID: "key123"}, ""), &ev); err != nil {
		t.Fatal(err)
	}
	if id := ev.RequestContext.Identity; id["apiKeyId"] != "key123" || id["apiKey"] != "secretvalue" {
		t.Fatalf("identity = %v", id)
	}
	ev.RequestContext.Identity = nil
	if err := json.Unmarshal(restEvent(&call{r: r}, ""), &ev); err != nil {
		t.Fatal(err)
	}
	if _, has := ev.RequestContext.Identity["apiKeyId"]; has {
		t.Fatalf("no key passed, but identity = %v", ev.RequestContext.Identity)
	}
}
