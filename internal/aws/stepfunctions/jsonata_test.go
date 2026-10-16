package stepfunctions

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

const acct = `{"Account":{"Name":"Firefly","Order":[
 {"OrderID":"order103","Product":[{"Name":"Bowler Hat","Price":34.45,"Quantity":2},{"Name":"Trilby hat","Price":21.67,"Quantity":1}]},
 {"OrderID":"order104","Product":[{"Name":"Bowler Hat","Price":34.45,"Quantity":4},{"Name":"Cloak","Price":107.99,"Quantity":1}]}]}}`

// want is JSON; "undefined" means no value; "!CODE" means an error with that code.
type jcase struct{ expr, in, want string }

func runJSONata(t *testing.T, cases []jcase) {
	t.Helper()
	for _, c := range cases {
		in := any(nil)
		if c.in != "" {
			if err := json.Unmarshal([]byte(c.in), &in); err != nil {
				t.Fatalf("bad input %s: %v", c.in, err)
			}
		}
		got, ok, err := jsonataRun(c.expr, in, nil)
		switch {
		case strings.HasPrefix(c.want, "!"):
			if je, isJ := err.(*jerr); !isJ || !strings.HasPrefix(je.code, c.want[1:]) {
				t.Errorf("%s: want error %s, got %v (%v)", c.expr, c.want, err, got)
			}
		case err != nil:
			t.Errorf("%s: unexpected error %v", c.expr, err)
		case c.want == "undefined":
			if ok {
				t.Errorf("%s: want undefined, got %s", c.expr, js(got))
			}
		case !ok:
			t.Errorf("%s: want %s, got undefined", c.expr, c.want)
		default:
			var want any
			if err := json.Unmarshal([]byte(c.want), &want); err != nil {
				t.Fatalf("bad want %s: %v", c.want, err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("%s: got %s, want %s", c.expr, js(got), c.want)
			}
		}
	}
}

func TestJSONataPaths(t *testing.T) {
	runJSONata(t, []jcase{
		{`Account.Name`, acct, `"Firefly"`},
		{`Account.Order.OrderID`, acct, `["order103","order104"]`},
		{`Account.Order[0].OrderID`, acct, `"order103"`},
		{`Account.Order[-1].OrderID`, acct, `"order104"`},
		{`Account.Order.Product.Name`, acct, `["Bowler Hat","Trilby hat","Bowler Hat","Cloak"]`},
		{`Account.Order[0].Product[1].Name`, acct, `"Trilby hat"`},
		{`Account.Order.Product[Price>100].Name`, acct, `"Cloak"`},
		{`Account.Order.Product[Price>30].Name`, acct, `["Bowler Hat","Bowler Hat","Cloak"]`},
		{`Account.Order[OrderID="order104"].Product[0].Name`, acct, `"Bowler Hat"`},
		{`Account.Order.Product[Name="Cloak"].Price`, acct, `107.99`},
		{`$count(Account.Order)`, acct, `2`},
		{`$count(Account.Order.Product)`, acct, `4`},
		{`$round($sum(Account.Order.Product.(Price*Quantity)), 2)`, acct, `336.36`},
		{`Account.Order.Product.Quantity ~> $sum`, acct, `8`},
		{`Account.Order.{OrderID: $round($sum(Product.(Price*Quantity)), 2)}`, acct, `[{"order103":90.57},{"order104":245.79}]`},
		{`Account.Order.Product{Name: $sum(Quantity)}`, acct, `{"Bowler Hat":6,"Trilby hat":1,"Cloak":1}`},
		{`Account.Order.Product^(Price).Name`, acct, `["Trilby hat","Bowler Hat","Bowler Hat","Cloak"]`},
		{`Account.Order.Product^(>Price).Name`, acct, `["Cloak","Bowler Hat","Bowler Hat","Trilby hat"]`},
		{`**.Price`, acct, `[34.45,21.67,34.45,107.99]`},
		{`Account.Order.[OrderID]`, acct, `[["order103"],["order104"]]`},
		{`$.[a,b]`, `{"a":1,"b":2}`, `[1,2]`},
		{`a.b.c`, `{"a":{"b":{"c":[1,2]}}}`, `[1,2]`},
		{`a.b.c`, `{"a":{"b":{}}}`, `undefined`},
		{`a.b`, `{"a":[{"b":1},{"b":2},{"x":3}]}`, `[1,2]`},
		{`a.b`, `{"a":{"b":[1]}}`, `[1]`},
		{`a[0].b`, `{"a":[{"b":1},{"b":2}]}`, `1`},
		{`a.b[]`, `{"a":{"b":1}}`, `[1]`},
		{`items[0].name`, `{"items":[{"name":"a"},{"name":"b"}]}`, `"a"`},
		{`items[name="b"]`, `{"items":[{"name":"a"},{"name":"b"}]}`, `{"name":"b"}`},
		{`items[price > 10 and qty = 1].name`, `{"items":[{"name":"a","price":20,"qty":1},{"name":"b","price":5,"qty":1},{"name":"c","price":20,"qty":2}]}`, `"a"`},
		{`items[[0,2]].n`, `{"items":[{"n":1},{"n":2},{"n":3}]}`, `[1,3]`},
		{`$`, `5`, `5`},
		{`$$`, `5`, `5`},
		{`$.a`, `{"a":3}`, `3`},
		{"`a b`", `{"a b":1}`, `1`},
		{`a.*`, `{"a":{"x":1,"y":2}}`, `[1,2]`},
		{`*.x`, `{"a":{"x":1},"b":{"x":2}}`, `[1,2]`},
		{`-a`, `{"a":2}`, `-2`},
		{`missing.x`, `{}`, `undefined`},
		{`a`, `{"a":null}`, `null`},
		{`[1,2,3][1]`, ``, `2`},
		{`[1,2,3][-1]`, ``, `3`},
		{`[1,2,3][$ > 1]`, ``, `[2,3]`},
		{`[1,[2,3]]`, ``, `[1,[2,3]]`},
		{`($x := [1,2]; [$x, 3])`, ``, `[1,2,3]`},
		{`[missing, 1]`, ``, `[1]`},
		{`{"a": missing, "b": 1}`, ``, `{"b":1}`},
		{`{"n": $count(items), "first": items[0]}`, `{"items":[5,6]}`, `{"n":2,"first":5}`},
		{`1 /* a comment */ + 2`, ``, `3`},
	})
}

func TestJSONataOperators(t *testing.T) {
	runJSONata(t, []jcase{
		{`1+2`, ``, `3`}, {`10/4`, ``, `2.5`}, {`7%3`, ``, `1`}, {`2*3+4`, ``, `10`}, {`2+3*4`, ``, `14`},
		{`(2+3)*4`, ``, `20`}, {`-5+2`, ``, `-3`}, {`1.5e2`, ``, `150`},
		{`"a"&"b"`, ``, `"ab"`}, {`"n"&1`, ``, `"n1"`}, {`"x"&missing`, ``, `"x"`}, {`"a"&[1,2]`, ``, `"a[1,2]"`},
		{`1=1`, ``, `true`}, {`"a"!="b"`, ``, `true`}, {`1<2`, ``, `true`}, {`"a"<"b"`, ``, `true`}, {`2>=2`, ``, `true`},
		{`[1,{"a":2}]=[1,{"a":2}]`, ``, `true`}, {`1="1"`, ``, `false`},
		{`true and false`, ``, `false`}, {`true or false`, ``, `true`}, {`false or missing`, ``, `false`},
		{`1 in [1,2]`, ``, `true`}, {`5 in [1,2]`, ``, `false`}, {`"a" in "a"`, ``, `true`},
		{`[1..5]`, ``, `[1,2,3,4,5]`}, {`[5..1]`, ``, `[]`}, {`([1..3]).($*2)`, ``, `[2,4,6]`},
		{`x ? "y" : "n"`, `{"x":true}`, `"y"`}, {`x ? "y" : "n"`, `{}`, `"n"`}, {`false ? 1`, ``, `undefined`},
		{`x ?: "d"`, `{"x":""}`, `"d"`}, {`x ?? "d"`, `{}`, `"d"`}, {`x ?? "d"`, `{"x":null}`, `null`},
		{`missing + 1`, ``, `undefined`}, {`missing = 1`, ``, `false`}, {`missing = missing`, ``, `false`},
		{`"abc" + 1`, ``, `!T2001`}, {`1 + "abc"`, ``, `!T2002`}, {`1/0`, ``, `!D1001`},
		{`"a" < 1`, ``, `!T2009`}, {`true < false`, ``, `!T2010`}, {`[1..2000000]`, ``, `!D2014`},
		{`$count([1..100000])`, ``, `100000`},
		{`$string(1e21)`, ``, `"1e+21"`}, {`$string(0.000001)`, ``, `"0.000001"`}, {`$string(1e-7)`, ``, `"1e-7"`},
		{`$string(123456789012345678)`, ``, `"123456789012346000"`}, {`$string(0.1+0.2)`, ``, `"0.3"`},
		{`"a\nb"`, ``, `"a\nb"`}, {`"é"`, ``, `"é"`}, {`'it\'s'`, ``, `"it's"`}, {`"😀"`, ``, `"😀"`},
	})
}

func TestJSONataFunctionValues(t *testing.T) {
	runJSONata(t, []jcase{
		{`($x := 2; $y := 3; $x * $y)`, ``, `6`},
		{`($f := function($a,$b){$a+$b}; $f(1,2))`, ``, `3`},
		{`function($x){$x*2}(4)`, ``, `8`},
		{`λ($x){$x+1}(1)`, ``, `2`},
		{`($fact := function($n){ $n <= 1 ? 1 : $n * $fact($n-1) }; $fact(10))`, ``, `3628800`},
		{`($add := function($x){ function($y){ $x+$y } }; $add(1)(2))`, ``, `3`},
		{`($add := function($a,$b){$a+$b}; $inc := $add(?, 1); $inc(5))`, ``, `6`},
		{`($x := 1; ($x := 2; $x) + $x)`, ``, `3`},
		{`($f := function($v)<n:n>{$v+1}; $f(1))`, ``, `2`},
		{`"abc" ~> $uppercase()`, ``, `"ABC"`},
		{`[1,2,3] ~> $sum`, ``, `6`},
		{`"a,b" ~> $split(",")`, ``, `["a","b"]`},
		{`($f := $uppercase ~> $trim; $f("  a  "))`, ``, `"A"`},
		{`$ ~> |a|{"b":2}|`, `{"a":{"x":1}}`, `{"a":{"x":1,"b":2}}`},
		{`$ ~> |a|{},["x"]|`, `{"a":{"x":1,"y":2}}`, `{"a":{"y":2}}`},
		{`name ~> $uppercase()`, `{"name":"pogi"}`, `"POGI"`},
		{`name.$uppercase()`, `{"name":"pogi"}`, `"POGI"`},
		{`name.$substring(1, 2)`, `{"name":"pogi"}`, `"og"`},
		{`$eval("1+1")`, ``, `!T1006`},
		{`$nope(1)`, ``, `!T1006`},
		{`nope(1)`, ``, `!T1005`},
		{`($f := function($n){ $f($n+1) }; $f(1))`, ``, `!U1001`},
		{`$1`, ``, `undefined`},
	})
}

func TestJSONataStrings(t *testing.T) {
	runJSONata(t, []jcase{
		{`$substring("hello",1,3)`, ``, `"ell"`}, {`$substring("hello",-3)`, ``, `"llo"`}, {`$substring("hello",1)`, ``, `"ello"`},
		{`$substring("hello",1,0)`, ``, `""`}, {`$substring("héllo",1,2)`, ``, `"él"`},
		{`$substringBefore("a-b-c","-")`, ``, `"a"`}, {`$substringAfter("a-b-c","-")`, ``, `"b-c"`}, {`$substringAfter("abc","x")`, ``, `"abc"`},
		{`$uppercase("a")`, ``, `"A"`}, {`$lowercase("A")`, ``, `"a"`}, {`$trim("  a   b  ")`, ``, `"a b"`},
		{`$pad("a",3,"-")`, ``, `"a--"`}, {`$pad("a",-3,"0")`, ``, `"00a"`}, {`$pad("abc",2)`, ``, `"abc"`},
		{`$contains("hello","ell")`, ``, `true`}, {`$contains("hello",/l+/)`, ``, `true`}, {`$contains("ABC",/b/i)`, ``, `true`},
		{`$contains(missing,"a")`, ``, `undefined`},
		{`$split("a,b,c",",")`, ``, `["a","b","c"]`}, {`$split("a1b2c",/\d/)`, ``, `["a","b","c"]`}, {`$split("abc","")`, ``, `["a","b","c"]`},
		{`$split("a,b,c",",",2)`, ``, `["a","b"]`},
		{`$join(["a","b"],"-")`, ``, `"a-b"`}, {`$join(["a","b"])`, ``, `"ab"`}, {`$join([1,2])`, ``, `!T0412`},
		{`$length("héllo")`, ``, `5`}, {`$length(1)`, ``, `!T0410`},
		{`$replace("hello","l","L")`, ``, `"heLLo"`}, {`$replace("hello","l","L",1)`, ``, `"heLlo"`},
		{`$replace("abc",/(b)/,"[$1]")`, ``, `"a[b]c"`}, {`$replace("abc",/b/,"$$")`, ``, `"a$c"`},
		{`$replace("john smith",/(\w+)\s(\w+)/,"$2 $1")`, ``, `"smith john"`},
		{`$replace("abc",/b/,function($m){ $uppercase($m.match) })`, ``, `"aBc"`},
		{`$replace("abc","","x")`, ``, `!D3010`},
		{`$match("abc",/b/).index`, ``, `1`}, {`$match("ab12cd34",/\d+/).match`, ``, `["12","34"]`},
		{`$match("ab12",/([a-z]+)(\d+)/).groups`, ``, `["ab","12"]`},
		{`$string(1.0)`, ``, `"1"`}, {`$string([1,"a",{"b":null}])`, ``, `"[1,\"a\",{\"b\":null}]"`}, {`$string(null)`, ``, `"null"`},
		{`$string("x")`, ``, `"x"`}, {`$string(true)`, ``, `"true"`}, {`$string(missing)`, ``, `undefined`},
		{`$string({"a":1},true)`, ``, "\"{\\n  \\\"a\\\": 1\\n}\""},
		{`$base64encode("hello")`, ``, `"aGVsbG8="`}, {`$base64decode("aGVsbG8=")`, ``, `"hello"`}, {`$base64decode("aGVsbG8")`, ``, `"hello"`},
		{`$encodeUrlComponent("a b&c")`, ``, `"a%20b%26c"`}, {`$encodeUrl("http://x.com/a b?q=1")`, ``, `"http://x.com/a%20b?q=1"`},
		{`$decodeUrlComponent("a%20b%26c")`, ``, `"a b&c"`}, {`$decodeUrl("a%20b%26c")`, ``, `"a b%26c"`},
	})
}

func TestJSONataNumbers(t *testing.T) {
	runJSONata(t, []jcase{
		{`$number("42")`, ``, `42`}, {`$number("0x1F")`, ``, `31`}, {`$number(" 5 ")`, ``, `5`}, {`$number("1e2")`, ``, `100`},
		{`$number("abc")`, ``, `!D3030`}, {`$number("Infinity")`, ``, `!D3030`}, {`$number(true)`, ``, `!T0410`},
		{`$abs(-2)`, ``, `2`}, {`$floor(2.7)`, ``, `2`}, {`$ceil(2.1)`, ``, `3`}, {`$floor(-2.1)`, ``, `-3`},
		{`$round(2.5)`, ``, `2`}, {`$round(3.5)`, ``, `4`}, {`$round(-2.5)`, ``, `-2`}, {`$round(123.456,2)`, ``, `123.46`}, {`$round(1234.5678,-2)`, ``, `1200`},
		{`$power(2,10)`, ``, `1024`}, {`$power(-1,0.5)`, ``, `!D3061`}, {`$sqrt(16)`, ``, `4`}, {`$sqrt(-1)`, ``, `!D3060`},
		{`$sum([1,2,3])`, ``, `6`}, {`$sum([])`, ``, `0`}, {`$sum(5)`, ``, `5`}, {`$max([1,5,3])`, ``, `5`}, {`$min([4,2])`, ``, `2`},
		{`$average([1,2,3,4])`, ``, `2.5`}, {`$sum("a")`, ``, `!T0412`}, {`$max([])`, ``, `undefined`}, {`$sum(missing)`, ``, `undefined`},
		{`$formatNumber(1234.5678,"#,##0.00")`, ``, `"1,234.57"`}, {`$formatNumber(0.5,"0%")`, ``, `"50%"`}, {`$formatNumber(12,"000")`, ``, `"012"`},
		{`$formatNumber(-5,"#.0")`, ``, `"-5.0"`}, {`$formatNumber(1234,"#,###")`, ``, `"1,234"`}, {`$formatNumber(2.5,"#.##")`, ``, `"2.5"`},
		{`$formatBase(255,16)`, ``, `"ff"`}, {`$formatBase(10,2)`, ``, `"1010"`}, {`$formatBase(10,1)`, ``, `!D3100`},
		{`$random() < 1`, ``, `true`}, {`$random(5) = $random(5)`, ``, `true`},
	})
}

func TestJSONataCollectionsAndLogic(t *testing.T) {
	runJSONata(t, []jcase{
		{`$count([1,2])`, ``, `2`}, {`$count(1)`, ``, `1`}, {`$count(missing)`, ``, `0`}, {`$exists(a)`, `{"a":null}`, `true`}, {`$exists(b)`, `{"a":1}`, `false`},
		{`$append([1],[2,3])`, ``, `[1,2,3]`}, {`$append(1,2)`, ``, `[1,2]`}, {`$append(missing,2)`, ``, `2`},
		{`$reverse([1,2,3])`, ``, `[3,2,1]`}, {`$sort([3,1,2])`, ``, `[1,2,3]`}, {`$sort(["b","a"])`, ``, `["a","b"]`},
		{`$sort([3,1,2],function($l,$r){$l<$r})`, ``, `[3,2,1]`}, {`$sort([1,"a"])`, ``, `!D3070`},
		{`$sort(items,function($l,$r){$l.n>$r.n})`, `{"items":[{"n":2},{"n":1}]}`, `[{"n":1},{"n":2}]`},
		{`$distinct([1,2,1,3,2])`, ``, `[1,2,3]`}, {`$zip([1,2],["a","b"])`, ``, `[[1,"a"],[2,"b"]]`},
		{`$keys({"a":1,"b":2})`, ``, `["a","b"]`}, {`$keys({"a":1})`, ``, `"a"`}, {`$lookup({"a":1},"a")`, ``, `1`},
		{`$spread({"a":1,"b":2})`, ``, `[{"a":1},{"b":2}]`}, {`$merge([{"a":1},{"b":2}])`, ``, `{"a":1,"b":2}`},
		{`$each({"a":1,"b":2},function($v,$k){$k & $v})`, ``, `["a1","b2"]`},
		{`$sift({"a":1,"b":2},function($v){$v>1})`, ``, `{"b":2}`},
		{`$type(1)`, ``, `"number"`}, {`$type([])`, ``, `"array"`}, {`$type(null)`, ``, `"null"`}, {`$type(missing)`, ``, `undefined`},
		{`$type({})`, ``, `"object"`}, {`$type($sum)`, ``, `"function"`},
		{`$error("x")`, ``, `!D3137`}, {`$assert(false,"boom")`, ``, `!D3141`}, {`$assert(true)`, ``, `undefined`},
		{`$map([1,2,3],function($v){$v*2})`, ``, `[2,4,6]`}, {`$map(["a","b"],function($v,$i){$v & $i})`, ``, `["a0","b1"]`},
		{`$map([1,2],$string)`, ``, `["1","2"]`},
		{`$filter([1,2,3,4],function($v){$v%2=0})`, ``, `[2,4]`}, {`$filter([1,2,3],function($v){$v>2})`, ``, `3`},
		{`$reduce([1,2,3,4],function($a,$b){$a+$b})`, ``, `10`}, {`$reduce([1,2,3],function($a,$v){$a+$v},10)`, ``, `16`},
		{`$single([1,2,3],function($v){$v>2})`, ``, `3`}, {`$single([1,2],function($v){$v>0})`, ``, `!D3138`}, {`$single([1,2],function($v){$v>5})`, ``, `!D3139`},
		{`$boolean("")`, ``, `false`}, {`$boolean("a")`, ``, `true`}, {`$boolean([0,1])`, ``, `true`}, {`$boolean([])`, ``, `false`},
		{`$boolean(0)`, ``, `false`}, {`$boolean({})`, ``, `false`}, {`$not(true)`, ``, `false`}, {`$not(missing)`, ``, `undefined`},
	})
}

func TestJSONataTimeAndStepFunctions(t *testing.T) {
	runJSONata(t, []jcase{
		{`$fromMillis(1510067557121)`, ``, `"2017-11-07T15:12:37.121Z"`},
		{`$fromMillis(1510067557121,"[Y0001]-[M01]-[D01]")`, ``, `"2017-11-07"`},
		{`$fromMillis(1510067557121,"[H01]:[m]:[s]")`, ``, `"15:12:37"`},
		{`$fromMillis(1510067557121,"[FNn], [D1] [MNn] [Y]")`, ``, `"Tuesday, 7 November 2017"`},
		{`$fromMillis(1510067557121,"[h]:[m] [P]")`, ``, `"3:12 pm"`},
		{`$fromMillis(1510067557121,"[H01]:[m]","+0530")`, ``, `"20:42"`},
		{`$fromMillis(1510067557121,"[Q]")`, ``, `!D3132`},
		{`$toMillis("2017-11-07T15:12:37.121Z")`, ``, `1510067557121`}, {`$toMillis("2017-11-07")`, ``, `1510012800000`},
		{`$toMillis("nope")`, ``, `!D3110`},
		{`$type($now())`, ``, `"string"`}, {`$millis() > 0`, ``, `true`}, {`$now() = $now()`, ``, `true`},
		{`$partition([1,2,3,4,5],2)`, ``, `[[1,2],[3,4],[5]]`}, {`$partition([1],0)`, ``, `!D3050`},
		{`$range(1,5)`, ``, `[1,2,3,4,5]`}, {`$range(0,10,5)`, ``, `[0,5,10]`}, {`$range(5,1,-2)`, ``, `[5,3,1]`}, {`$range(1,2,0)`, ``, `!D3050`},
		{`$hash("abc","MD5")`, ``, `"900150983cd24fb0d6963f7d28e17f72"`},
		{`$hash("abc","SHA-256")`, ``, `"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"`},
		{`$hash("abc","CRC")`, ``, `!D3050`},
		{`$length($uuid())`, ``, `36`}, {`$parse('{"a":1}')`, ``, `{"a":1}`}, {`$parse("{")`, ``, `!D3140`},
	})
}

func TestJSONataSyntaxErrors(t *testing.T) {
	for expr, code := range map[string]string{
		`1 +`: "S0207", `(1`: "S0202", `"abc`: "S0101", `$x :=`: "S0207", `a.1`: "S0213", `1 := 2`: "S0212",
		`a % 2 + %`: "S0", `a@$x`: "S0", `a#$i`: "S0", `[1,`: "S0207", `{"a" 1}`: "S0202", `/*`: "S0106", `/(/`: "S0302", `/(a/`: "S0302", `/[/`: "S0302",
		`"\q"`: "S0301", `function($a { $a }`: "S0202", `)`: "S0211", `a.`: "S0207", "`abc": "S0105",
	} {
		_, err := jsonataParse(expr)
		if je, ok := err.(*jerr); !ok || !strings.HasPrefix(je.code, code) {
			t.Errorf("%q: want %s, got %v", expr, code, err)
		}
	}
}

// FuzzJSONata checks that no expression makes the parser or the evaluator panic.
func FuzzJSONata(f *testing.F) {
	for _, s := range []string{`a.b[0]`, `$map([1,2], function($v){$v*2})`, `{"a": 1}.a ~> $string()`, `x ? y : z`, `[1..3].($ * 2)`, `$replace("a", /a/, "$1")`,
		`Account.Order^(>Price).{"a": b}`, `($f := function($n){$n}; $f(1))`, `|a|{"b":1}|`, `$formatNumber(1234.5, "#,##0.00")`, `$fromMillis(1, "[Y]-[M01]")`} {
		f.Add(s)
	}
	in := map[string]any{"a": []any{map[string]any{"b": 1.0}}, "x": true}
	f.Fuzz(func(t *testing.T, src string) {
		if n, err := jsonataParse(src); err == nil {
			jsonataEval(n, in, nil)
		}
	})
}
