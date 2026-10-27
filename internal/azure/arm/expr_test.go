package arm

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestExpressions(t *testing.T) {
	e := &exprEnv{fn: func(name string, args []any) (any, bool, error) {
		if name == "parameters" {
			return map[string]any{"name": "Web", "n": 3.0, "tags": map[string]any{"Env": "prod"}}[args[0].(string)], true, nil
		}
		return nil, false, nil
	}}
	for src, want := range map[string]string{
		`[concat('a', parameters('name'), '-', string(parameters('n')))]`: `"aWeb-3"`,
		`[toLower(format('{0}-{1}', parameters('name'), 'X'))]`:           `"web-x"`,
		`[parameters('tags').env]`:                                        `"prod"`,
		`[parameters('tags')['Env']]`:                                     `"prod"`,
		`[if(equals(parameters('n'), 3), 'three', fail())]`:               `"three"`,
		`[add(mul(parameters('n'), 2), -1)]`:                              `5`,
		`[length(createArray(1, 2, 3))]`:                                  `3`,
		`[union(createObject('a', 1), createObject('b', 2))]`:             `{"a":1,"b":2}`,
		`[split('a,b;c', createArray(',', ';'))[2]]`:                      `"c"`,
		`[substring('hello', 1, 3)]`:                                      `"ell"`,
		`[contains(createArray('x', 'y'), 'y')]`:                          `true`,
		`[and(true(), not(false()))]`:                                     `true`,
		`[replace('it''s', '''', '')]`:                                    `"its"`,
		`[[not an expression]`:                                            `"[not an expression]"`,
		`[take(skip(range(1, 10), 2), 3)]`:                                `[3,4,5]`,
		`[startsWith('Microsoft.Web', 'microsoft.')]`:                     `true`,
		`[coalesce(null(), 'x')]`:                                         `"x"`,
		`[length(uniqueString('a', 'b'))]`:                                `13`,
		`[equals(uniqueString('a', 'b'), uniqueString('a', 'b'))]`:        `true`,
		`[guid('a')]`:                  `"` + mustGuid("a") + `"`,
		`[padLeft(string(7), 3, '0')]`: `"007"`,
		`[last(split('a/b/c', '/'))]`:  `"c"`,
		`[indexOf('abcABC', 'C')]`:     `2`,
		`[intersection(createArray(1, 2, 3), createArray(2, 3, 4))]`: `[2,3]`,
		`[json('{"k":[1]}').k[0]]`:                                   `1`,
		`[greater('b', 'a')]`:                                        `true`,
		`[items(createObject('b', 2, 'a', 1))[0].key]`:               `"a"`,
		`[base64ToString(base64('pogi'))]`:                           `"pogi"`,
		`[empty(createObject())]`:                                    `true`,
		`[first('pogi')]`:                                            `"p"`,
	} {
		v, err := e.value(src)
		if err != nil {
			t.Errorf("%s: %v", src, err)
			continue
		}
		if got, _ := json.Marshal(normalize(v)); string(got) != want {
			t.Errorf("%s = %s, want %s", src, got, want)
		}
	}
	for src, msg := range map[string]string{
		`[concat('a']`:                       "not valid",
		`[nosuch()]`:                         "'nosuch' is not valid here",
		`[parameters('tags').missing]`:       "doesn't exist",
		`[createArray(1)[5]]`:                "out of range",
		`[div(1, 0)]`:                        "division by zero",
		`[substring('abc', 2, 5)]`:           "outside the string",
		`[if('yes', 1, 2)]`:                  "must be a boolean",
		`[format('{1}', 'only one')]`:        "not given",
		`[range(0, 100000)]`:                 "from 0 to 10000",
		`[concat(createArray(1), 'string')]`: "must be an array",
		`[bare]`:                             "must be a function call",
	} {
		if _, err := e.value(src); err == nil || !strings.Contains(err.Error(), msg) {
			t.Errorf("%s: %v, want an error with %q", src, err, msg)
		}
	}
}

func mustGuid(s string) string {
	v, err := exprFuncs["guid"]([]any{s})
	if err != nil {
		panic(err)
	}
	return v.(string)
}

func TestJSONC(t *testing.T) {
	for in, want := range map[string]string{
		`{"properties": {"mode": "Incremental", template:{"a": [1, 2,], // note
		  "b": "x // not a comment, /* nor this */", /* gone */ "c": true,}}}`: `{"properties":{"mode":"Incremental","template":{"a":[1,2],"b":"x // not a comment, /* nor this */","c":true}}}`,
		`{a: "\"q\"", b_2: [true, false, null], $c: {}}`: `{"$c":{},"a":"\"q\"","b_2":[true,false,null]}`,
	} {
		var v any
		if err := unmarshal([]byte(in), &v); err != nil {
			t.Errorf("%s: %v", in, err)
			continue
		}
		if got, _ := json.Marshal(v); string(got) != want {
			t.Errorf("%s = %s, want %s", in, got, want)
		}
	}
	var v any
	if unmarshal([]byte(`{"a": }`), &v) == nil {
		t.Error("bad JSON was read")
	}
}
