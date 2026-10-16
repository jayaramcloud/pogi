package apigateway

import (
	"net/http/httptest"
	"strings"
	"testing"
)

// run renders tmpl for a POST /pets/7?q=1 with the given body.
func run(t *testing.T, tmpl, body string) string {
	t.Helper()
	r := httptest.NewRequest("POST", "/pets/7?q=1&q=2", strings.NewReader(body))
	r.Header.Set("X-Repro-Header", "hello")
	r.Host = "abc.execute-api.localhost"
	x := &call{r: r, params: map[string]string{"id": "7"}, vars: map[string]string{"color": "blue"}, stage: "dev", apiID: "abc",
		auth: map[string]any{"principalId": "me", "claims": map[string]any{"sub": "u1"}}}
	vars := map[string]any{"input": &vinput{x: x, body: []byte(body)}, "util": vutil{}, "context": x.vcontext(), "stageVariables": fromGo(x.vars)}
	out, err := vtlRender(tmpl, vars)
	if err != nil {
		t.Fatalf("%q: %v", tmpl, err)
	}
	return out
}

func TestVTL(t *testing.T) {
	const body = `{"name":"Rex","tags":["a","b","c"],"n":3,"f":1.5,"nested":{"z":1,"y":[{"id":1},{"id":2}]},"s":"it's \"x\""}`
	for _, c := range []struct{ tmpl, want string }{
		// references, quiet and formal
		{`$input.params('id') $input.params('q') ${input.params('id')}x`, `7 1 7x`},
		{`[$nothing][$!nothing][$input.path('$.missing')]`, `[][][]`},
		{`cost: $5 and $`, `cost: $5 and $`},
		{`$context.stage $context.authorizer.principalId $context.authorizer.claims.sub $stageVariables.color`, `dev me u1 blue`},
		// $input
		{`$input.json('$.name')`, `"Rex"`},
		{`$input.json('$.tags')`, `["a","b","c"]`},
		{`$input.path('$.name')`, `Rex`},
		{`$input.path('$.nested')`, `{z=1, y=[{id=1}, {id=2}]}`},
		{`$input.path('$.tags').size() $input.path('$.tags').get(1) $input.path('$.tags')[2]`, `3 b c`},
		{`$input.path('$.nested.y[?(@.id == 2)]')`, `[{id=2}]`},
		{`$input.path('$..id')`, `[1, 2]`},
		{`$input.path('$.tags[*]').size()`, `3`},
		{`$input.path('$.tags[-1]') $input.path("$['name']")`, `c Rex`},
		{`$input.path('$.f') $input.path('$.n')`, `1.5 3`},
		{`$input.body`, body},
		// params(): header lookup ignores case, keySet is ordered
		{`$input.params().header.get('x-repro-header')`, `hello`},
		{`$input.params().querystring.get('q') $input.params().path.get('id') $input.params().path.id`, `1 7 7`},
		{`#foreach($h in $input.params().header.keySet())$h #end`, `X-Repro-Header `},
		// directives
		{`#set($x = 3)#set($y = $x * 2 + 1)$y`, `7`},
		{`#set($s = "n=$x")#set($x = 5)$s`, `n=`},
		{"#set($a = 'x')\n#if($a == 'x')yes#else no#end", `yes`},
		{`#if($input.path('$.n') > 5)big#elseif($input.path('$.n') >= 3)mid#{else}small#end`, `mid`},
		{`#if(!$nothing && true)ok#end#if($nothing)bad#end`, `ok`},
		{`#foreach($t in $input.path('$.tags'))$t#if($foreach.hasNext),#end#end`, `a,b,c`},
		{`#foreach($t in $input.path('$.tags'))$foreach.index/$foreach.count/$velocityCount$foreach.first$foreach.last #end`, `0/1/1truefalse 1/2/2falsefalse 2/3/3falsetrue `},
		{`#foreach($t in [1..4])#if($t == 3)#break#end$t#end`, `12`},
		{`#foreach($t in [3..1])$t#end`, `321`},
		{`a#foreach($t in [1,2])$t#stop#end b`, `a1`},
		{`#foreach($k in $input.path('$.nested').keySet())$k=$input.path('$.nested').get($k);#end`, `z=1;y=[{id=1}, {id=2}];`},
		{`#foreach($i in $input.path('$.nested.y'))$i.id #end`, `1 2 `},
		{`#foreach($v in {"a":1,"b":2})$v#end`, `12`},
		{"## a comment\nkept#* block *# text #[[ $raw #set ]]#", "kept text  $raw #set "},
		{`\$escaped \#set and #unknown #`, `$escaped #set and #unknown #`},
		// expressions
		{`#set($m = {"k": "v", "n": 2})$m.k $m.get('n') $m.size() $m.containsKey('k')`, `v 2 2 true`},
		{`#set($l = [])#set($ignore = $l.add('q'))$l $l.isEmpty()`, `[q] false`},
		{`#set($d = 7 / 2)#set($e = 7 % 4)#set($f = 7.0 / 2)$d $e $f`, `3 3 3.5`},
		{`#set($z = "a" + 1)$z`, `a1`},
		{`#if(1 == 1.0 && 'a' != 'b' && "3" == 3)t#end`, `t`},
		// strings
		{`$input.path('$.name').length() $input.path('$.name').toUpperCase() $input.path('$.name').substring(1) $input.path('$.name').substring(0,2)`, `3 REX ex Re`},
		{`$input.path('$.name').replace('e','3') $input.path('$.name').contains('ex') $input.path('$.name').startsWith('R')`, `R3x true true`},
		{`#set($str = 'a,b,,')#foreach($p in $str.split(','))[$p]#end`, `[a][b]`},
		// $util
		{`$util.escapeJavaScript($input.path('$.s'))`, `it\'s \"x\"`},
		{"$util.escapeJavaScript(\"a/b\n\")", `a\/b\n`},
		{`$util.urlEncode('a b&c~*') $util.urlDecode('a+b%26c')`, `a+b%26c%7E* a b&c`},
		{`$util.base64Encode('hello') $util.base64Decode('aGVsbG8=')`, `aGVsbG8= hello`},
		{`$util.parseJson('{"a":{"b":4}}').a.b`, `4`},
		{`#set($o = $util.parseJson('{"a":[1,2]}'))$o.a.size()`, `2`},
		{`$util.quiet($input.path('$.tags').add('d'))$input.path('$.tags').size()`, `4`},
		// the usual JSON template
		{`{"id": "$input.params('id')", "body": $input.json('$.nested'), "tags": [#foreach($t in $input.path('$.tags'))"$t"#if($foreach.hasNext),#end#end]}`,
			`{"id": "7", "body": {"z":1,"y":[{"id":1},{"id":2}]}, "tags": ["a","b","c"]}`},
	} {
		if got := run(t, c.tmpl, body); got != c.want {
			t.Errorf("%s\n got  %q\n want %q", c.tmpl, got, c.want)
		}
	}
	if got := run(t, `$input.json('$')`, ``); got != `{}` {
		t.Errorf("empty body: %q", got)
	}
}

func TestVTLSyntaxErrors(t *testing.T) {
	for _, tmpl := range []string{`#if($a)open`, `#end`, `#foreach($x in)#end`, `#set($x 3)`, `${open`, `#set($x = "unclosed)`} {
		if _, err := vtlRender(tmpl, map[string]any{}); err == nil {
			t.Errorf("%q: want a syntax error", tmpl)
		}
	}
}

func TestStageOps(t *testing.T) {
	st := &Stage{MethodSettings: map[string]any{"have/GET": map[string]any{}}}
	ops := stageOps(st, []patchOp{
		{Op: "replace", Path: "/~1pets/GET/metrics/enabled", Value: "true"},
		{Op: "replace", Path: "/~1pets~0promo/GET/metrics/enabled", Value: "true"},
		{Op: "replace", Path: "/*/*/throttling/rateLimit", Value: "5"},
		{Op: "replace", Path: "/have~1GET/caching/ttlInSeconds", Value: "9"},
		{Op: "replace", Path: "/variables/x", Value: "y"},
		{Op: "remove", Path: "/pets~1GET"},
	})
	var paths []string
	for _, op := range ops {
		paths = append(paths, op.Path)
	}
	want := "/methodSettings/pets~1GET /methodSettings/pets~1GET/metricsEnabled /methodSettings/pets~0promo~1GET " +
		"/methodSettings/pets~0promo~1GET/metricsEnabled /methodSettings/*~1* /methodSettings/*~1*/throttlingRateLimit " +
		"/methodSettings/have~1GET/cacheTtlInSeconds /variables/x /methodSettings/pets~1GET"
	if got := strings.Join(paths, " "); got != want {
		t.Errorf("paths\n got  %s\n want %s", got, want)
	}
}
