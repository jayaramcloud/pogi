package web_test

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/core"
)

// TestExplorersFollowTheSwitcher checks that an explorer offers what is in the
// header switcher's account and region, as the resource tables do.
func TestExplorersFollowTheSwitcher(t *testing.T) {
	srv := pogi.Start(t, pogi.Config{Services: []string{"dynamodb"}})
	table := func(region, name string) {
		in := map[string]any{"TableName": name, "BillingMode": "PAY_PER_REQUEST",
			"AttributeDefinitions": []any{map[string]any{"AttributeName": "pk", "AttributeType": "S"}},
			"KeySchema":            []any{map[string]any{"AttributeName": "pk", "KeyType": "HASH"}}}
		if err := srv.Hub.Call(core.DefaultAccount, region, "DynamoDB_20120810", "CreateTable", in, nil); err != nil {
			t.Fatal(err)
		}
	}
	table(core.DefaultRegion, "Here")
	table("eu-west-1", "There")

	get := func(region string) string {
		req, _ := http.NewRequest("GET", srv.URL+"/_pogi/query/dynamodb", nil)
		if region != "" {
			req.AddCookie(&http.Cookie{Name: "pogi-region", Value: region})
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		b, _ := io.ReadAll(res.Body)
		return string(b)
	}
	p := get("")
	has(t, p, ">Here<")
	if strings.Contains(p, ">There<") {
		t.Error("the default scope offers a table of another region")
	}
	has(t, get("eu-west-1"), ">There<")
	p = get("*")
	has(t, p, ">Here<", ">There<")
}
