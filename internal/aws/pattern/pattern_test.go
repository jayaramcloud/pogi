package pattern

import "testing"

func TestMatch(t *testing.T) {
	event := `{"source":"orders","detail-type":"Order Placed","detail":{"total":120.5,"currency":"usd","items":["book","pen"],
		"customer":{"tier":"gold","ip":"10.0.0.7"},"coupon":null}}`
	cases := []struct {
		pattern string
		want    bool
	}{
		{`{"source":["orders"]}`, true},
		{`{"source":["billing"]}`, false},
		{`{"detail":{"total":[{"numeric":[">",100,"<=",200]}]}}`, true},
		{`{"detail":{"total":[{"numeric":[">",500]}]}}`, false},
		{`{"detail":{"currency":[{"equals-ignore-case":"USD"}]}}`, true},
		{`{"detail-type":[{"prefix":"Order"}]}`, true},
		{`{"detail-type":[{"suffix":{"equals-ignore-case":"PLACED"}}]}`, true},
		{`{"detail":{"items":["pen"]}}`, true}, // arrays match on any element
		{`{"detail":{"items":[{"anything-but":["book","pen"]}]}}`, false},
		{`{"detail":{"customer":{"tier":[{"anything-but":{"prefix":"bronze"}}]}}}`, true},
		{`{"detail":{"customer":{"ip":[{"cidr":"10.0.0.0/24"}]}}}`, true},
		{`{"detail":{"missing":[{"exists":false}]}}`, true},
		{`{"detail":{"currency":[{"exists":true}]}}`, true},
		{`{"detail":{"coupon":[null]}}`, true},
		{`{"source":[{"wildcard":"ord*s"}]}`, true},
		{`{"$or":[{"source":["nope"]},{"detail":{"customer":{"tier":["gold"]}}}]}`, true},
		{`{"$or":[{"source":["nope"]},{"detail-type":["nope"]}]}`, false},
	}
	for _, c := range cases {
		p, err := Compile(c.pattern)
		if err != nil {
			t.Fatalf("%s: %v", c.pattern, err)
		}
		if got := p.MatchJSON([]byte(event)); got != c.want {
			t.Errorf("%s: got %v, want %v", c.pattern, got, c.want)
		}
	}
	for _, bad := range []string{`{"source":"orders"}`, `{"a":[{"numeric":[">"]}]}`, `{"a":[{"nope":1}]}`, `[1]`} {
		if _, err := Compile(bad); err == nil {
			t.Errorf("%s should not compile", bad)
		}
	}
}
