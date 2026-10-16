package dynamodb

import (
	"bytes"
	"encoding/json"
	"math/big"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// Value is one attribute value in DynamoDB JSON. Exactly one field is set;
// nil means unset, so empty maps, lists and binaries survive a round trip.
type Value struct {
	S    *string
	N    *string
	B    []byte
	BOOL *bool
	NULL bool
	M    map[string]*Value
	L    []*Value
	SS   []string
	NS   []string
	BS   [][]byte
}

// Item is an item or a key: attribute name → value. Stored items are never
// changed in place (updates work on copies), so responses may share them.
type Item map[string]*Value

func (v *Value) kind() string {
	switch {
	case v == nil:
		return ""
	case v.S != nil:
		return "S"
	case v.N != nil:
		return "N"
	case v.B != nil:
		return "B"
	case v.BOOL != nil:
		return "BOOL"
	case v.NULL:
		return "NULL"
	case v.M != nil:
		return "M"
	case v.L != nil:
		return "L"
	case v.SS != nil:
		return "SS"
	case v.NS != nil:
		return "NS"
	case v.BS != nil:
		return "BS"
	}
	return ""
}

func (v Value) MarshalJSON() ([]byte, error) {
	var x any
	switch v.kind() {
	case "S":
		x = *v.S
	case "N":
		x = *v.N
	case "B":
		x = v.B
	case "BOOL":
		x = *v.BOOL
	case "NULL":
		x = true
	case "M":
		x = v.M
	case "L":
		x = v.L
	case "SS":
		x = v.SS
	case "NS":
		x = v.NS
	case "BS":
		x = v.BS
	}
	return json.Marshal(map[string]any{v.kind(): x})
}

func errVal(format string, a ...any) error {
	return core.Errorf(400, "ValidationException", format, a...)
}

// check validates a value from a request and canonicalizes its numbers.
func (v *Value) check() error {
	n := 0
	if v != nil {
		for _, set := range [...]bool{v.S != nil, v.N != nil, v.B != nil, v.BOOL != nil, v.NULL, v.M != nil, v.L != nil, v.SS != nil, v.NS != nil, v.BS != nil} {
			if set {
				n++
			}
		}
	}
	switch {
	case n == 0:
		return errVal("Supplied AttributeValue is empty, must contain exactly one of the supported datatypes")
	case n > 1:
		return errVal("Supplied AttributeValue has more than one datatypes set, must contain exactly one of the supported datatypes")
	case v.N != nil:
		c, err := canon(*v.N)
		v.N = &c
		return err
	case v.NS != nil:
		for i, x := range v.NS {
			var err error
			if v.NS[i], err = canon(x); err != nil {
				return err
			}
		}
		return checkSet("number", v.NS)
	case v.SS != nil:
		return checkSet("string", v.SS)
	case v.BS != nil:
		return checkSet("binary", members(v))
	}
	for _, x := range v.M {
		if err := x.check(); err != nil {
			return err
		}
	}
	for _, x := range v.L {
		if err := x.check(); err != nil {
			return err
		}
	}
	return nil
}

func checkSet(kind string, xs []string) error {
	if len(xs) == 0 {
		return errVal("One or more parameter values were invalid: An %s set  may not be empty", kind)
	}
	seen := map[string]bool{}
	for _, x := range xs {
		if seen[x] {
			return errVal("One or more parameter values were invalid: Input collection [%s] contains duplicates.", strings.Join(xs, ", "))
		}
		seen[x] = true
	}
	return nil
}

func checkItem(it Item) error {
	for _, v := range it {
		if err := v.check(); err != nil {
			return err
		}
	}
	return nil
}

var numRe = regexp.MustCompile(`^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d{1,4})?$`)

// canon returns a number the way DynamoDB stores it: "01.50" → "1.5", "1e2" → "100".
func canon(n string) (string, error) {
	if !numRe.MatchString(n) { // before SetString: the regexp bounds the exponent
		return "", errVal("A value provided cannot be converted into a number")
	}
	r, _ := new(big.Rat).SetString(n)
	return ratNum(r)
}

// ratNum formats an exact decimal within DynamoDB's limits: 38 significant
// digits and magnitudes from 1E-130 to under 1E+126.
func ratNum(r *big.Rat) (string, error) {
	s := r.RatString()
	if !r.IsInt() {
		x, digits := new(big.Rat).Set(r), 0
		for ; !x.IsInt() && digits <= 200; digits++ { // past 200 places it fails a check below anyway
			x.Mul(x, big.NewRat(10, 1))
		}
		s = r.FloatString(digits)
	}
	whole, frac, _ := strings.Cut(strings.TrimPrefix(s, "-"), ".")
	switch {
	case len(whole) > 126:
		return "", errVal("Number overflow. Attempting to store a number with magnitude larger than supported range")
	case whole == "0" && len(frac)-len(strings.TrimLeft(frac, "0")) >= 130:
		return "", errVal("Number underflow. Attempting to store a number with magnitude smaller than supported range")
	case len(strings.Trim(whole+frac, "0")) > 38:
		return "", errVal("Attempting to store more than 38 significant digits in a Number")
	}
	return s, nil
}

func num(n int) *Value {
	s := strconv.Itoa(n)
	return &Value{N: &s}
}

func numCmp(a, b string) int {
	x, _ := new(big.Rat).SetString(a)
	y, _ := new(big.Rat).SetString(b)
	return x.Cmp(y)
}

// scalar returns an S, N or B value as a string.
func scalar(v *Value) string {
	switch {
	case v.S != nil:
		return *v.S
	case v.N != nil:
		return *v.N
	}
	return string(v.B)
}

// members returns a set's elements as strings (binary elements as raw bytes).
func members(v *Value) []string {
	switch {
	case v.SS != nil:
		return v.SS
	case v.NS != nil:
		return v.NS
	case v.BS != nil:
		out := make([]string, len(v.BS))
		for i, b := range v.BS {
			out[i] = string(b)
		}
		return out
	}
	return nil
}

func setOf(kind string, xs []string) *Value {
	switch kind {
	case "SS":
		return &Value{SS: xs}
	case "NS":
		return &Value{NS: xs}
	}
	bs := make([][]byte, len(xs))
	for i, x := range xs {
		bs[i] = []byte(x)
	}
	return &Value{BS: bs}
}

// compare orders two S, N or B values of the same type; ok is false otherwise.
func compare(a, b *Value) (c int, ok bool) {
	switch {
	case a == nil || b == nil:
		return 0, false
	case a.S != nil && b.S != nil:
		return strings.Compare(*a.S, *b.S), true
	case a.N != nil && b.N != nil:
		return numCmp(*a.N, *b.N), true
	case a.B != nil && b.B != nil:
		return bytes.Compare(a.B, b.B), true
	}
	return 0, false
}

func equal(a, b *Value) bool {
	if a == nil || b == nil || a.kind() != b.kind() {
		return a == b
	}
	switch a.kind() {
	case "BOOL":
		return *a.BOOL == *b.BOOL
	case "NULL":
		return true
	case "M":
		return equalItems(a.M, b.M)
	case "L":
		return slices.EqualFunc(a.L, b.L, equal)
	case "SS", "NS", "BS":
		x, y := members(a), members(b)
		return len(x) == len(y) && !slices.ContainsFunc(x, func(e string) bool { return !slices.Contains(y, e) })
	}
	return scalar(a) == scalar(b) // numbers are canonical
}

func equalItems(a, b Item) bool {
	if (a == nil) != (b == nil) || len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if !equal(v, b[k]) {
			return false
		}
	}
	return true
}

// size approximates DynamoDB's storage size of a value in bytes.
func size(v *Value) int {
	n := 0
	switch {
	case v.S != nil, v.B != nil:
		return len(scalar(v))
	case v.N != nil:
		return len(*v.N)/2 + 1
	case v.BOOL != nil, v.NULL:
		return 1
	case v.M != nil:
		n = 3 + itemSize(v.M) + len(v.M)
	case v.L != nil:
		n = 3 + len(v.L)
		for _, x := range v.L {
			n += size(x)
		}
	}
	for _, m := range members(v) {
		n += len(m)
	}
	return n
}

func itemSize(it Item) int {
	n := 0
	for k, v := range it {
		n += len(k) + size(v)
	}
	return n
}

func (v *Value) clone() *Value {
	c := *v
	if v.M != nil {
		c.M = Item(v.M).clone()
	}
	if v.L != nil {
		c.L = make([]*Value, len(v.L))
		for i, x := range v.L {
			c.L[i] = x.clone()
		}
	}
	return &c
}

func (it Item) clone() Item {
	c := make(Item, len(it))
	for k, v := range it {
		c[k] = v.clone()
	}
	return c
}

// pick copies the named top-level attributes of it.
func pick(it Item, names ...string) Item {
	out := Item{}
	for _, n := range names {
		if v := it[n]; v != nil {
			out[n] = v
		}
	}
	return out
}

// ---- document paths ----

// path is a document path: string steps are attribute or map keys, int steps list indexes.
type path []any

func (p path) String() string {
	parts := make([]string, len(p))
	for i, s := range p {
		if n, ok := s.(int); ok {
			parts[i] = "[" + strconv.Itoa(n) + "]"
		} else {
			parts[i] = s.(string)
		}
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func (p path) get(it Item) *Value {
	v := it[p[0].(string)]
	for _, s := range p[1:] {
		switch s := s.(type) {
		case string:
			if v == nil || v.M == nil {
				return nil
			}
			v = v.M[s]
		case int:
			if v == nil || s >= len(v.L) {
				return nil
			}
			v = v.L[s]
		}
	}
	return v
}

var errPath = errVal("The document path provided in the update expression is invalid for update")

// set stores v at p in it, which must be a private copy. An index past the
// end of a list appends.
func (p path) set(it Item, v *Value) error {
	if len(p) == 1 {
		it[p[0].(string)] = v
		return nil
	}
	parent := p[:len(p)-1].get(it)
	switch s := p[len(p)-1].(type) {
	case string:
		if parent == nil || parent.M == nil {
			return errPath
		}
		parent.M[s] = v
	case int:
		if parent == nil || parent.L == nil {
			return errPath
		}
		if s >= len(parent.L) {
			parent.L = append(parent.L, v)
		} else {
			parent.L[s] = v
		}
	}
	return nil
}

func (p path) remove(it Item) error {
	if len(p) == 1 {
		delete(it, p[0].(string))
		return nil
	}
	parent := p[:len(p)-1].get(it)
	switch s := p[len(p)-1].(type) {
	case string:
		if parent == nil || parent.M == nil {
			return errPath
		}
		delete(parent.M, s)
	case int:
		if parent == nil || parent.L == nil {
			return errPath
		}
		if s < len(parent.L) {
			parent.L = slices.Delete(parent.L, s, s+1)
		}
	}
	return nil
}

// mask selects parts of an item, for projections.
type mask struct {
	all  bool
	kids map[any]*mask // string or int step → sub-mask
}

// project keeps only the given paths of it (ProjectionExpression semantics:
// selected list elements are compacted, in index order).
func project(it Item, paths []path) Item {
	if it == nil {
		return nil
	}
	root := &mask{}
	for _, p := range paths {
		m := root
		for _, s := range p {
			if m.kids == nil {
				m.kids = map[any]*mask{}
			}
			if m.kids[s] == nil {
				m.kids[s] = &mask{}
			}
			m = m.kids[s]
		}
		m.all = true
	}
	if v := root.apply(&Value{M: it}); v != nil {
		return v.M
	}
	return Item{}
}

func (m *mask) apply(v *Value) *Value {
	if m.all {
		return v
	}
	switch {
	case v.M != nil:
		out := Item{}
		for k, sub := range m.kids {
			if k, ok := k.(string); ok && v.M[k] != nil {
				if x := sub.apply(v.M[k]); x != nil {
					out[k] = x
				}
			}
		}
		if len(out) > 0 {
			return &Value{M: out}
		}
	case v.L != nil:
		var idx []int
		for k := range m.kids {
			if i, ok := k.(int); ok && i < len(v.L) {
				idx = append(idx, i)
			}
		}
		slices.Sort(idx)
		var out []*Value
		for _, i := range idx {
			if x := m.kids[i].apply(v.L[i]); x != nil {
				out = append(out, x)
			}
		}
		if len(out) > 0 {
			return &Value{L: out}
		}
	}
	return nil
}

// ---- expressions ----

// exprs holds a request's placeholders and records which ones were used.
type exprs struct {
	names  map[string]string
	values map[string]*Value
	used   map[string]bool
}

func newExprs(names map[string]string, values map[string]*Value) (*exprs, error) {
	switch {
	case names != nil && len(names) == 0:
		return nil, errVal("ExpressionAttributeNames must not be empty")
	case values != nil && len(values) == 0:
		return nil, errVal("ExpressionAttributeValues must not be empty")
	}
	for k, v := range values {
		if err := v.check(); err != nil {
			return nil, errVal("ExpressionAttributeValues contains invalid value: %s for key %s", core.AsError(err).Msg, k)
		}
	}
	return &exprs{names, values, map[string]bool{}}, nil
}

// unused rejects placeholders that no expression used, as DynamoDB does.
func (x *exprs) unused() error {
	var names, values []string
	for k := range x.names {
		if !x.used[k] {
			names = append(names, k)
		}
	}
	for k := range x.values {
		if !x.used[k] {
			values = append(values, k)
		}
	}
	slices.Sort(names)
	slices.Sort(values)
	switch {
	case len(values) > 0:
		return errVal("Value provided in ExpressionAttributeValues unused in expressions: keys: {%s}", strings.Join(values, ", "))
	case len(names) > 0:
		return errVal("Value provided in ExpressionAttributeNames unused in expressions: keys: {%s}", strings.Join(names, ", "))
	}
	return nil
}

// exprError carries an error out of the recursive parser and evaluator.
type exprError struct{ err error }

func fail(err error) { panic(exprError{err}) }

// catch turns an exprError panic back into an error.
func catch(err *error) {
	if r := recover(); r != nil {
		e, ok := r.(exprError)
		if !ok {
			panic(r)
		}
		*err = e.err
	}
}

func lex(src string) []string {
	var toks []string
	for i := 0; i < len(src); {
		j := i + 1
		switch c := src[i]; {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
			continue
		case c == '<' && j < len(src) && (src[j] == '=' || src[j] == '>'), c == '>' && j < len(src) && src[j] == '=':
			j++
		case c == '#' || c == ':' || word(c):
			for j < len(src) && word(src[j]) {
				j++
			}
		}
		toks = append(toks, src[i:j])
		i = j
	}
	return toks
}

func word(c byte) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
}

// uses says whether the expression names the top-level attribute name.
func (n *node) uses(name string) bool {
	if n == nil {
		return false
	}
	if n.op == "path" && n.path[0] == name {
		return true
	}
	return slices.ContainsFunc(n.args, func(a *node) bool { return a.uses(name) })
}

// node is a parsed condition, operand or update value.
type node struct {
	op   string // or and not = <> < <= > >= between in, a function name, path, value, + -
	args []*node
	path path
	val  *Value
}

type parser struct {
	toks  []string
	pos   int
	x     *exprs
	what  string          // expression kind, for error messages
	funcs map[string]bool // functions allowed as operands
}

func (x *exprs) parser(src, what string) *parser {
	funcs := map[string]bool{"size": true}
	if what == "UpdateExpression" {
		funcs = map[string]bool{"if_not_exists": true, "list_append": true}
	}
	return &parser{toks: lex(src), x: x, what: what, funcs: funcs}
}

func (p *parser) peek(i int) string {
	if p.pos+i < len(p.toks) {
		return p.toks[p.pos+i]
	}
	return ""
}

func (p *parser) next() string {
	t := p.peek(0)
	p.pos++
	return t
}

func (p *parser) is(kw string) bool { return strings.EqualFold(p.peek(0), kw) }

func (p *parser) expect(kw string) {
	if !p.is(kw) {
		p.syntax()
	}
	p.pos++
}

func (p *parser) fail(format string, a ...any) {
	fail(errVal("Invalid "+p.what+": "+format, a...))
}

func (p *parser) syntax() {
	tok := p.peek(0)
	if tok == "" {
		tok = "<EOF>"
	}
	lo, hi := min(max(p.pos-1, 0), len(p.toks)), min(p.pos+2, len(p.toks))
	p.fail("Syntax error; token: \"%s\", near: \"%s\"", tok, strings.Join(p.toks[lo:hi], " "))
}

func (p *parser) end() {
	if p.pos < len(p.toks) {
		p.syntax()
	}
}

// name reads an attribute name: a #placeholder or a plain, non-reserved word.
func (p *parser) name() string {
	t := p.next()
	switch {
	case strings.HasPrefix(t, "#"):
		n, ok := p.x.names[t]
		if !ok {
			p.fail("An expression attribute name used in the document path is not defined; attribute name: %s", t)
		}
		p.x.used[t] = true
		return n
	case t == "" || !word(t[0]) || t[0] >= '0' && t[0] <= '9':
		p.pos--
		p.syntax()
	case reserved[strings.ToUpper(t)]:
		p.fail("Attribute name is a reserved keyword; reserved keyword: %s", t)
	}
	return t
}

func (p *parser) path() path {
	out := path{p.name()}
	for {
		switch p.peek(0) {
		case ".":
			p.pos++
			out = append(out, p.name())
		case "[":
			p.pos++
			i, err := strconv.Atoi(p.peek(0))
			if err != nil || i < 0 {
				p.syntax()
			}
			p.pos++
			p.expect("]")
			out = append(out, i)
		default:
			return out
		}
	}
}

func (p *parser) operand() *node {
	switch t := p.peek(0); {
	case strings.HasPrefix(t, ":"):
		p.pos++
		v, ok := p.x.values[t]
		if !ok {
			p.fail("An expression attribute value used in expression is not defined; attribute value: %s", t)
		}
		p.x.used[t] = true
		return &node{op: "value", val: v}
	case p.funcs[t] && p.peek(1) == "(":
		return p.call()
	}
	return &node{op: "path", path: p.path()}
}

// list reads comma-separated operands.
func (p *parser) list() []*node {
	out := []*node{p.operand()}
	for p.peek(0) == "," {
		p.pos++
		out = append(out, p.operand())
	}
	return out
}

var arity = map[string]int{"attribute_exists": 1, "attribute_not_exists": 1, "attribute_type": 2, "begins_with": 2,
	"contains": 2, "size": 1, "if_not_exists": 2, "list_append": 2}

var types = []string{"S", "N", "B", "BOOL", "NULL", "M", "L", "SS", "NS", "BS"}

func (p *parser) call() *node {
	n := &node{op: p.next()}
	p.expect("(")
	n.args = p.list()
	p.expect(")")
	switch t := n.args[len(n.args)-1].val; {
	case len(n.args) != arity[n.op]:
		p.fail("Incorrect number of operands for operator or function; operator or function: %s, number of operands: %d", n.op, len(n.args))
	case n.op != "list_append" && n.args[0].op != "path":
		p.fail("Operator or function requires a document path; operator or function: %s", n.op)
	case n.op == "attribute_type" && t != nil && (t.S == nil || !slices.Contains(types, *t.S)):
		p.fail("Invalid attribute type name found; type: %s, valid types: { B,NULL,SS,BOOL,L,BS,N,NS,S,M }", scalar(t))
	}
	return n
}

var condFuncs = map[string]bool{"attribute_exists": true, "attribute_not_exists": true, "attribute_type": true, "begins_with": true, "contains": true}

// Precedence, lowest first: OR, AND, NOT, then comparisons and functions.
func (p *parser) or() *node {
	n := p.and()
	for p.is("OR") {
		p.pos++
		n = &node{op: "or", args: []*node{n, p.and()}}
	}
	return n
}

func (p *parser) and() *node {
	n := p.not()
	for p.is("AND") {
		p.pos++
		n = &node{op: "and", args: []*node{n, p.not()}}
	}
	return n
}

func (p *parser) not() *node {
	if p.is("NOT") {
		p.pos++
		return &node{op: "not", args: []*node{p.not()}}
	}
	return p.cond()
}

func (p *parser) cond() *node {
	if p.peek(0) == "(" {
		p.pos++
		n := p.or()
		p.expect(")")
		return n
	}
	if condFuncs[p.peek(0)] && p.peek(1) == "(" {
		return p.call()
	}
	n := &node{args: []*node{p.operand()}}
	switch t := p.peek(0); {
	case t == "=" || t == "<>" || t == "<" || t == "<=" || t == ">" || t == ">=":
		n.op = p.next()
		n.args = append(n.args, p.operand())
	case p.is("BETWEEN"):
		n.op = "between"
		p.pos++
		lo := p.operand()
		p.expect("AND")
		n.args = append(n.args, lo, p.operand())
		if lo, hi := n.args[1].val, n.args[2].val; lo != nil && hi != nil {
			if c, ok := compare(lo, hi); ok && c > 0 {
				p.fail("The BETWEEN operator requires upper bound to be greater than or equal to lower bound; lower bound operand: AttributeValue: {%s:%s}, upper bound operand: AttributeValue: {%s:%s}",
					lo.kind(), scalar(lo), hi.kind(), scalar(hi))
			}
		}
	case p.is("IN"):
		n.op = "in"
		p.pos++
		p.expect("(")
		n.args = append(n.args, p.list()...)
		p.expect(")")
	default:
		p.syntax()
	}
	return n
}

// cond parses a ConditionExpression, FilterExpression or KeyConditionExpression.
func (x *exprs) cond(src, what string) (n *node, err error) {
	if src == "" {
		return nil, nil
	}
	defer catch(&err)
	p := x.parser(src, what)
	n = p.or()
	p.end()
	return n, nil
}

func (x *exprs) projection(src string) (ps []path, err error) {
	if src == "" {
		return nil, nil
	}
	defer catch(&err)
	p := x.parser(src, "ProjectionExpression")
	ps = append(ps, p.path())
	for p.peek(0) == "," {
		p.pos++
		ps = append(ps, p.path())
	}
	p.end()
	p.overlap(ps)
	return ps, nil
}

// action is one clause of an update expression.
type action struct {
	op   string // SET, REMOVE, ADD, DELETE
	path path
	val  *node
}

func (x *exprs) update(src string) (acts []action, err error) {
	if src == "" {
		return nil, nil
	}
	defer catch(&err)
	p := x.parser(src, "UpdateExpression")
	seen := map[string]bool{}
	for len(acts) == 0 || p.pos < len(p.toks) {
		kw := strings.ToUpper(p.peek(0))
		if kw != "SET" && kw != "REMOVE" && kw != "ADD" && kw != "DELETE" {
			p.syntax()
		}
		if seen[kw] {
			p.fail("The \"%s\" section can only be used once in an update expression;", kw)
		}
		seen[kw] = true
		p.pos++
		for {
			a := action{op: kw, path: p.path()}
			switch kw {
			case "SET":
				p.expect("=")
				a.val = p.operand()
				if t := p.peek(0); t == "+" || t == "-" {
					p.pos++
					a.val = &node{op: t, args: []*node{a.val, p.operand()}}
				}
			case "ADD", "DELETE":
				a.val = p.operand()
			}
			acts = append(acts, a)
			if p.peek(0) != "," {
				break
			}
			p.pos++
		}
	}
	var ps []path
	for _, a := range acts {
		ps = append(ps, a.path)
	}
	p.overlap(ps)
	return acts, nil
}

// overlap rejects paths that are equal or nested in one another.
func (p *parser) overlap(ps []path) {
	for i, a := range ps {
		for _, b := range ps[:i] {
			if n := min(len(a), len(b)); slices.Equal(a[:n], b[:n]) {
				kind := "overlap"
				if len(a) == len(b) {
					kind = "conflict"
				}
				p.fail("Two document paths %s with each other; must remove or rewrite one of these paths; path one: %s, path two: %s", kind, b, a)
			}
		}
	}
}

// test evaluates a condition against it (nil when the item does not exist).
func (n *node) test(it Item) bool {
	arg := func(i int) *Value { return n.args[i].value(it) }
	switch n.op {
	case "or":
		return n.args[0].test(it) || n.args[1].test(it)
	case "and":
		return n.args[0].test(it) && n.args[1].test(it)
	case "not":
		return !n.args[0].test(it)
	case "=":
		a, b := arg(0), arg(1)
		return a != nil && b != nil && equal(a, b)
	case "<>": // a missing attribute differs from everything
		a, b := arg(0), arg(1)
		return a == nil || b == nil || !equal(a, b)
	case "<", "<=", ">", ">=":
		c, ok := compare(arg(0), arg(1))
		return ok && (n.op == "<" && c < 0 || n.op == "<=" && c <= 0 || n.op == ">" && c > 0 || n.op == ">=" && c >= 0)
	case "between":
		lo, ok1 := compare(arg(0), arg(1))
		hi, ok2 := compare(arg(0), arg(2))
		return ok1 && ok2 && lo >= 0 && hi <= 0
	case "in":
		a := arg(0)
		for i := 1; i < len(n.args); i++ {
			if a != nil && equal(a, arg(i)) {
				return true
			}
		}
	case "attribute_exists":
		return arg(0) != nil
	case "attribute_not_exists":
		return arg(0) == nil
	case "attribute_type":
		a, t := arg(0), arg(1)
		return a != nil && t != nil && t.S != nil && a.kind() == *t.S
	case "begins_with":
		a, b := arg(0), arg(1)
		switch {
		case a == nil || b == nil:
		case a.S != nil && b.S != nil:
			return strings.HasPrefix(*a.S, *b.S)
		case a.B != nil && b.B != nil:
			return bytes.HasPrefix(a.B, b.B)
		}
	case "contains":
		a, b := arg(0), arg(1)
		switch {
		case a == nil || b == nil:
		case a.S != nil && b.S != nil:
			return strings.Contains(*a.S, *b.S)
		case a.L != nil:
			return slices.ContainsFunc(a.L, func(e *Value) bool { return equal(e, b) })
		case a.kind() == b.kind()+"S":
			return slices.Contains(members(a), scalar(b))
		}
	}
	return false
}

// value evaluates an operand; nil means a missing attribute.
func (n *node) value(it Item) *Value {
	switch n.op {
	case "value":
		return n.val
	case "path":
		return n.path.get(it)
	case "size":
		switch v := n.args[0].value(it); {
		case v == nil:
		case v.S != nil, v.B != nil:
			return num(len(scalar(v)))
		case v.M != nil:
			return num(len(v.M))
		case v.L != nil:
			return num(len(v.L))
		case members(v) != nil:
			return num(len(members(v)))
		}
	case "if_not_exists":
		if v := n.args[0].value(it); v != nil {
			return v
		}
		return n.args[1].value(it)
	case "list_append":
		a, b := n.args[0].must(it), n.args[1].must(it)
		if a.L == nil || b.L == nil {
			bad := a
			if a.L != nil {
				bad = b
			}
			fail(errVal("Invalid UpdateExpression: Incorrect operand type for operator or function; operator or function: list_append, operand type: %s", bad.kind()))
		}
		return &Value{L: slices.Concat(a.L, b.L)}
	case "+", "-":
		return arith(n.op, n.args[0].must(it), n.args[1].must(it))
	}
	return nil
}

// must evaluates an update operand that has to exist.
func (n *node) must(it Item) *Value {
	v := n.value(it)
	if v == nil {
		fail(errVal("The provided expression refers to an attribute that does not exist in the item"))
	}
	return v
}

var errOperand = errVal("An operand in the update expression has an incorrect data type")

func arith(op string, a, b *Value) *Value {
	if a.N == nil || b.N == nil {
		fail(errOperand)
	}
	x, _ := new(big.Rat).SetString(*a.N)
	y, _ := new(big.Rat).SetString(*b.N)
	if op == "+" {
		x.Add(x, y)
	} else {
		x.Sub(x, y)
	}
	s, err := ratNum(x)
	if err != nil {
		fail(err)
	}
	return &Value{N: &s}
}

// apply runs update actions on a copy of old (just the key for a new item).
// SET values are computed from old, so "SET a = b, b = a" swaps.
func apply(old Item, acts []action) (it Item, err error) {
	defer catch(&err)
	vals := make([]*Value, len(acts))
	for i, a := range acts {
		if a.val != nil {
			vals[i] = a.val.must(old)
		}
	}
	// SET, then REMOVE (list elements from the highest index down), ADD, DELETE.
	rank := map[string]int{"SET": 0, "REMOVE": 1, "ADD": 2, "DELETE": 3}
	order := make([]int, len(acts))
	for i := range order {
		order[i] = i
	}
	slices.SortStableFunc(order, func(i, j int) int {
		a, b := acts[i], acts[j]
		if c := rank[a.op] - rank[b.op]; c != 0 || a.op != "REMOVE" {
			return c
		}
		return comparePaths(b.path, a.path)
	})
	it = old.clone()
	for _, i := range order {
		a := acts[i]
		switch a.op {
		case "SET":
			err = a.path.set(it, vals[i])
		case "REMOVE":
			err = a.path.remove(it)
		case "ADD":
			err = add(it, a.path, vals[i])
		case "DELETE":
			err = del(it, a.path, vals[i])
		}
		if err != nil {
			return nil, err
		}
	}
	return it, nil
}

func comparePaths(a, b path) int {
	for i := range min(len(a), len(b)) {
		c := 0
		switch x := a[i].(type) {
		case string:
			y, _ := b[i].(string)
			c = strings.Compare(x, y)
		case int:
			y, _ := b[i].(int)
			c = x - y
		}
		if c != 0 {
			return c
		}
	}
	return len(a) - len(b)
}

// add is ADD: numbers are summed (a missing attribute counts as 0), sets united.
func add(it Item, p path, v *Value) error {
	cur := p.get(it)
	switch {
	case cur == nil && (v.N != nil || members(v) != nil):
		return p.set(it, v)
	case v.N != nil && cur.N != nil:
		return p.set(it, arith("+", cur, v))
	case members(v) != nil && cur.kind() == v.kind():
		out := slices.Clone(members(cur))
		for _, m := range members(v) {
			if !slices.Contains(out, m) {
				out = append(out, m)
			}
		}
		return p.set(it, setOf(v.kind(), out))
	}
	return errOperand
}

// del is DELETE: it removes elements from a set, and the attribute once empty.
func del(it Item, p path, v *Value) error {
	cur, drop := p.get(it), members(v)
	switch {
	case drop == nil || cur != nil && cur.kind() != v.kind():
		return errOperand
	case cur == nil:
		return nil
	}
	out := slices.DeleteFunc(slices.Clone(members(cur)), func(m string) bool { return slices.Contains(drop, m) })
	if len(out) == 0 {
		return p.remove(it)
	}
	return p.set(it, setOf(v.kind(), out))
}

// reserved holds DynamoDB's reserved words, which expressions may only use through #names.
var reserved = func() map[string]bool {
	m := map[string]bool{}
	for _, w := range strings.Fields(reservedWords) {
		m[w] = true
	}
	return m
}()

const reservedWords = `ABORT ABSOLUTE ACTION ADD AFTER AGENT AGGREGATE ALL ALLOCATE ALTER ANALYZE AND ANY ARCHIVE ARE ARRAY AS
ASC ASCII ASENSITIVE ASSERTION ASYMMETRIC AT ATOMIC ATTACH ATTRIBUTE AUTH AUTHORIZATION AUTHORIZE AUTO AVG BACK
BACKUP BASE BATCH BEFORE BEGIN BETWEEN BIGINT BINARY BIT BLOB BLOCK BOOLEAN BOTH BREADTH BUCKET BULK BY BYTE CALL
CALLED CALLING CAPACITY CASCADE CASCADED CASE CAST CATALOG CHAR CHARACTER CHECK CLASS CLOB CLOSE CLUSTER CLUSTERED
CLUSTERING CLUSTERS COALESCE COLLATE COLLATION COLLECTION COLUMN COLUMNS COMBINE COMMENT COMMIT COMPACT COMPILE
COMPRESS CONDITION CONFLICT CONNECT CONNECTION CONSISTENCY CONSISTENT CONSTRAINT CONSTRAINTS CONSTRUCTOR CONSUMED
CONTINUE CONVERT COPY CORRESPONDING COUNT COUNTER CREATE CROSS CUBE CURRENT CURSOR CYCLE DATA DATABASE DATE
DATETIME DAY DEALLOCATE DEC DECIMAL DECLARE DEFAULT DEFERRABLE DEFERRED DEFINE DEFINED DEFINITION DELETE DELIMITED
DEPTH DEREF DESC DESCRIBE DESCRIPTOR DETACH DETERMINISTIC DIAGNOSTICS DIRECTORIES DISABLE DISCONNECT DISTINCT
DISTRIBUTE DO DOMAIN DOUBLE DROP DUMP DURATION DYNAMIC EACH ELEMENT ELSE ELSEIF EMPTY ENABLE END EQUAL EQUALS ERROR
ESCAPE ESCAPED EVAL EVALUATE EXCEEDED EXCEPT EXCEPTION EXCEPTIONS EXCLUSIVE EXEC EXECUTE EXISTS EXIT EXPLAIN EXPLODE
EXPORT EXPRESSION EXTENDED EXTERNAL EXTRACT FAIL FALSE FAMILY FETCH FIELDS FILE FILTER FILTERING FINAL FINISH FIRST
FIXED FLATTERN FLOAT FOR FORCE FOREIGN FORMAT FORWARD FOUND FREE FROM FULL FUNCTION FUNCTIONS GENERAL GENERATE GET
GLOB GLOBAL GO GOTO GRANT GREATER GROUP GROUPING HANDLER HASH HAVE HAVING HEAP HIDDEN HOLD HOUR IDENTIFIED IDENTITY
IF IGNORE IMMEDIATE IMPORT IN INCLUDING INCLUSIVE INCREMENT INCREMENTAL INDEX INDEXED INDEXES INDICATOR INFINITE
INITIALLY INLINE INNER INNTER INOUT INPUT INSENSITIVE INSERT INSTEAD INT INTEGER INTERSECT INTERVAL INTO INVALIDATE
IS ISOLATION ITEM ITEMS ITERATE JOIN KEY KEYS LAG LANGUAGE LARGE LAST LATERAL LEAD LEADING LEAVE LEFT LENGTH LESS
LEVEL LIKE LIMIT LIMITED LINES LIST LOAD LOCAL LOCALTIME LOCALTIMESTAMP LOCATION LOCATOR LOCK LOCKS LOG LOGED LONG
LOOP LOWER MAP MATCH MATERIALIZED MAX MAXLEN MEMBER MERGE METHOD METRICS MIN MINUS MINUTE MISSING MOD MODE MODIFIES
MODIFY MODULE MONTH MULTI MULTISET NAME NAMES NATIONAL NATURAL NCHAR NCLOB NEW NEXT NO NONE NOT NULL NULLIF NUMBER
NUMERIC OBJECT OF OFFLINE OFFSET OLD ON ONLINE ONLY OPAQUE OPEN OPERATOR OPTION OR ORDER ORDINALITY OTHER OTHERS OUT
OUTER OUTPUT OVER OVERLAPS OVERRIDE OWNER PAD PARALLEL PARAMETER PARAMETERS PARTIAL PARTITION PARTITIONED PARTITIONS
PATH PERCENT PERCENTILE PERMISSION PERMISSIONS PIPE PIPELINED PLAN POOL POSITION PRECISION PREPARE PRESERVE PRIMARY
PRIOR PRIVATE PRIVILEGES PROCEDURE PROCESSED PROJECT PROJECTION PROPERTY PROVISIONING PUBLIC PUT QUERY QUIT QUORUM
RAISE RANDOM RANGE RANK RAW READ READS REAL REBUILD RECORD RECURSIVE REDUCE REF REFERENCE REFERENCES REFERENCING
REGEXP REGION REINDEX RELATIVE RELEASE REMAINDER RENAME REPEAT REPLACE REQUEST RESET RESIGNAL RESOURCE RESPONSE
RESTORE RESTRICT RESULT RETURN RETURNING RETURNS REVERSE REVOKE RIGHT ROLE ROLES ROLLBACK ROLLUP ROUTINE ROW ROWS
RULE RULES SAMPLE SATISFIES SAVE SAVEPOINT SCAN SCHEMA SCOPE SCROLL SEARCH SECOND SECTION SEGMENT SEGMENTS SELECT
SELF SEMI SENSITIVE SEPARATE SEQUENCE SERIALIZABLE SESSION SET SETS SHARD SHARE SHARED SHORT SHOW SIGNAL SIMILAR
SIZE SKEWED SMALLINT SNAPSHOT SOME SOURCE SPACE SPACES SPARSE SPECIFIC SPECIFICTYPE SPLIT SQL SQLCODE SQLERROR
SQLEXCEPTION SQLSTATE SQLWARNING START STATE STATIC STATUS STORAGE STORE STORED STREAM STRING STRUCT STYLE SUB
SUBMULTISET SUBPARTITION SUBSTRING SUBTYPE SUM SUPER SYMMETRIC SYNONYM SYSTEM TABLE TABLESAMPLE TEMP TEMPORARY
TERMINATED TEXT THAN THEN THROUGHPUT TIME TIMESTAMP TIMEZONE TINYINT TO TOKEN TOTAL TOUCH TRAILING TRANSACTION
TRANSFORM TRANSLATE TRANSLATION TREAT TRIGGER TRIM TRUE TRUNCATE TTL TUPLE TYPE UNDER UNDO UNION UNIQUE UNIT UNKNOWN
UNLOGGED UNNEST UNPROCESSED UNSIGNED UNTIL UPDATE UPPER URL USAGE USE USER USERS USING UUID VACUUM VALUE VALUED
VALUES VARCHAR VARIABLE VARIANCE VARINT VARYING VIEW VIEWS VIRTUAL VOID WAIT WHEN WHENEVER WHERE WHILE WINDOW WITH
WITHIN WITHOUT WORK WRAPPED WRITE YEAR ZONE`
