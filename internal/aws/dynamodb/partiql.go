package dynamodb

// PartiQL: ExecuteStatement, BatchExecuteStatement and ExecuteTransaction.
// A small parser turns the statement into the same nodes, paths and update
// actions that expressions use, so the existing evaluator, planner and
// reader do the work.

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/nrndev/pogi/internal/core"
)

const stmtPrefix = "Statement wasn't well formed, can't be processed: "

// stmt is a parsed statement.
type stmt struct {
	kind         string // SELECT, INSERT, UPDATE, DELETE, or EXISTS (inside a transaction)
	table, index string
	paths        []path // SELECT list; nil means *
	where        *node
	orderBy      string
	desc         bool
	item         Item     // INSERT
	acts         []action // UPDATE
	returning    string   // ALL OLD, ALL NEW, MODIFIED OLD, MODIFIED NEW
}

// ---- lexer ----

type ptok struct {
	k byte // i identifier, q "quoted identifier", s 'string', n number, p punctuation
	s string
}

func plex(src string) ([]ptok, error) {
	var toks []ptok
	for i := 0; i < len(src); {
		c := src[i]
		j := i + 1
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i = j
			continue
		case word(c) && (c < '0' || c > '9'):
			for j < len(src) && word(src[j]) {
				j++
			}
			toks = append(toks, ptok{'i', src[i:j]})
		case c >= '0' && c <= '9':
			for j < len(src) && (src[j] >= '0' && src[j] <= '9' || src[j] == '.' && j+1 < len(src) && src[j+1] >= '0' && src[j+1] <= '9') {
				j++
			}
			if j < len(src) && (src[j] == 'e' || src[j] == 'E') {
				k := j + 1
				if k < len(src) && (src[k] == '+' || src[k] == '-') {
					k++
				}
				if k < len(src) && src[k] >= '0' && src[k] <= '9' {
					for k < len(src) && src[k] >= '0' && src[k] <= '9' {
						k++
					}
					j = k
				}
			}
			toks = append(toks, ptok{'n', src[i:j]})
		case c == '"' || c == '\'':
			var b strings.Builder
			for {
				if j >= len(src) {
					return nil, errVal(stmtPrefix+"unterminated %c at position %d", c, i)
				}
				if src[j] == c {
					if j+1 < len(src) && src[j+1] == c { // doubled quote
						b.WriteByte(c)
						j += 2
						continue
					}
					j++
					break
				}
				b.WriteByte(src[j])
				j++
			}
			toks = append(toks, ptok{map[byte]byte{'"': 'q', '\'': 's'}[c], b.String()})
		case strings.Contains("<>!", string(c)) && j < len(src) && slices.Contains([]string{"<>", "!=", "<=", ">=", "<<", ">>"}, src[i:j+1]):
			toks = append(toks, ptok{'p', src[i : j+1]})
			j++
		case strings.Contains("=<>+-()[]{},.:*?", string(c)):
			toks = append(toks, ptok{'p', string(c)})
		default:
			return nil, errVal(stmtPrefix+"unexpected character %q at position %d", c, i)
		}
		i = j
	}
	return toks, nil
}

// ---- parser ----

type pp struct {
	t      []ptok
	i      int
	params []*Value
	pi     int
}

func (p *pp) cur() ptok {
	if p.i < len(p.t) {
		return p.t[p.i]
	}
	return ptok{'e', "<EOF>"}
}

func (p *pp) bad() {
	fail(errVal(stmtPrefix+"unexpected token %q at position %d", p.cur().s, p.i+1))
}

// kw takes the keyword when it is next.
func (p *pp) kw(w string) bool {
	if t := p.cur(); t.k == 'i' && strings.EqualFold(t.s, w) {
		p.i++
		return true
	}
	return false
}

func (p *pp) need(w string) {
	if !p.kw(w) {
		p.bad()
	}
}

func (p *pp) pun(s string) bool {
	if t := p.cur(); t.k == 'p' && t.s == s {
		p.i++
		return true
	}
	return false
}

func (p *pp) needPun(s string) {
	if !p.pun(s) {
		p.bad()
	}
}

// callNext reports whether a ( follows the current token.
func (p *pp) callNext() bool {
	return p.i+1 < len(p.t) && p.t[p.i+1].k == 'p' && p.t[p.i+1].s == "("
}

func (p *pp) ident() string {
	t := p.cur()
	if t.k != 'i' && t.k != 'q' {
		p.bad()
	}
	p.i++
	return t.s
}

func (p *pp) number() string {
	t := p.cur()
	if t.k != 'n' {
		p.bad()
	}
	p.i++
	return t.s
}

func (p *pp) path() path {
	pa := path{p.ident()}
	for {
		switch {
		case p.pun("."):
			pa = append(pa, p.ident())
		case p.pun("["):
			n := 0
			for _, d := range p.number() {
				if d < '0' || d > '9' || n > 1<<24 {
					p.i--
					p.bad()
				}
				n = n*10 + int(d-'0')
			}
			p.needPun("]")
			pa = append(pa, n)
		default:
			return pa
		}
	}
}

func (p *pp) tref() (table, index string) {
	table = p.ident()
	if p.pun(".") {
		index = p.ident()
	}
	return
}

// value parses a literal: a scalar, a ?, a list, a map or a set.
func (p *pp) value() *Value {
	v := p.value0()
	if err := v.check(); err != nil {
		fail(err)
	}
	return v
}

func (p *pp) value0() *Value {
	t := p.cur()
	switch {
	case t.k == 's':
		p.i++
		return &Value{S: &t.s}
	case t.k == 'n':
		p.i++
		return p.num(t.s)
	case t.k == 'p' && t.s == "-" && p.i+1 < len(p.t) && p.t[p.i+1].k == 'n':
		p.i += 2
		return p.num("-" + p.t[p.i-1].s)
	case t.k == 'i' && strings.EqualFold(t.s, "TRUE"), t.k == 'i' && strings.EqualFold(t.s, "FALSE"):
		p.i++
		b := strings.EqualFold(t.s, "TRUE")
		return &Value{BOOL: &b}
	case p.kw("NULL"):
		return &Value{NULL: true}
	case p.pun("?"):
		if p.pi >= len(p.params) {
			fail(errParams)
		}
		p.pi++
		return p.params[p.pi-1]
	case p.pun("["):
		l := []*Value{}
		for !p.pun("]") {
			if len(l) > 0 {
				p.needPun(",")
			}
			l = append(l, p.value())
		}
		return &Value{L: l}
	case p.pun("{"):
		m := map[string]*Value{}
		for !p.pun("}") {
			if len(m) > 0 {
				p.needPun(",")
			}
			k := p.cur()
			if k.k != 's' && k.k != 'q' {
				p.bad()
			}
			p.i++
			p.needPun(":")
			m[k.s] = p.value()
		}
		return &Value{M: m}
	case p.pun("<<"):
		var ss, ns []string
		for !p.pun(">>") {
			if len(ss)+len(ns) > 0 {
				p.needPun(",")
			}
			switch e := p.value(); {
			case e.S != nil:
				ss = append(ss, *e.S)
			case e.N != nil:
				ns = append(ns, *e.N)
			default:
				fail(errVal(stmtPrefix + "a set holds only strings or only numbers"))
			}
		}
		switch {
		case len(ss) > 0 && len(ns) == 0:
			return &Value{SS: ss}
		case len(ns) > 0 && len(ss) == 0:
			return &Value{NS: ns}
		}
		fail(errVal(stmtPrefix + "a set holds only strings or only numbers, and at least one"))
	}
	p.bad()
	return nil
}

func (p *pp) num(s string) *Value {
	n, err := canon(s)
	if err != nil {
		fail(err)
	}
	return &Value{N: &n}
}

// startsValue reports whether a literal starts here.
func (p *pp) startsValue() bool {
	t := p.cur()
	switch t.k {
	case 's', 'n':
		return true
	case 'i':
		return slices.ContainsFunc([]string{"TRUE", "FALSE", "NULL"}, func(w string) bool { return strings.EqualFold(t.s, w) }) && !p.callNext()
	case 'p':
		return slices.Contains([]string{"?", "[", "{", "<<", "-"}, t.s)
	}
	return false
}

var stmtFuncs = map[string]int{"begins_with": 2, "contains": 2, "attribute_type": 2, "size": 1}

// call parses name(args) for the functions a condition may use.
func (p *pp) call() *node {
	name := strings.ToLower(p.ident())
	n, ok := stmtFuncs[name]
	if !ok {
		p.i--
		p.bad()
	}
	p.needPun("(")
	c := &node{op: name}
	for i := 0; i < n; i++ {
		if i > 0 {
			p.needPun(",")
		}
		c.args = append(c.args, p.operand())
	}
	p.needPun(")")
	return c
}

func (p *pp) operand() *node {
	switch {
	case p.startsValue():
		return &node{op: "value", val: p.value()}
	case p.cur().k == 'i' && p.callNext():
		return p.call()
	}
	return &node{op: "path", path: p.path()}
}

func (p *pp) or() *node {
	n := p.and()
	for p.kw("OR") {
		n = &node{op: "or", args: []*node{n, p.and()}}
	}
	return n
}

func (p *pp) and() *node {
	n := p.not()
	for p.kw("AND") {
		n = &node{op: "and", args: []*node{n, p.not()}}
	}
	return n
}

func (p *pp) not() *node {
	if p.kw("NOT") {
		return &node{op: "not", args: []*node{p.not()}}
	}
	return p.pred()
}

func (p *pp) pred() *node {
	if p.pun("(") {
		n := p.or()
		p.needPun(")")
		return n
	}
	if t := p.cur(); t.k == 'i' && p.callNext() && t.s != "" && stmtFuncs[strings.ToLower(t.s)] == 2 {
		return p.call()
	}
	a := p.operand()
	for _, op := range []string{"=", "<>", "!=", "<=", ">=", "<", ">"} {
		if p.pun(op) {
			return &node{op: strings.Replace(op, "!=", "<>", 1), args: []*node{a, p.operand()}}
		}
	}
	switch {
	case p.kw("BETWEEN"):
		lo := p.operand()
		p.need("AND")
		return &node{op: "between", args: []*node{a, lo, p.operand()}}
	case p.kw("IN"):
		n := &node{op: "in", args: []*node{a}}
		end := "]"
		if !p.pun("[") {
			p.needPun("<<")
			end = ">>"
		}
		for !p.pun(end) {
			if len(n.args) > 1 {
				p.needPun(",")
			}
			n.args = append(n.args, p.operand())
		}
		if len(n.args) == 1 {
			p.bad()
		}
		return n
	case p.kw("IS"):
		not := p.kw("NOT")
		var n *node
		switch {
		case p.kw("MISSING"):
			n = &node{op: "attribute_not_exists", args: []*node{a}}
		case p.kw("NULL"): // NULL or missing, as in PartiQL
			n = &node{op: "or", args: []*node{{op: "attribute_not_exists", args: []*node{a}},
				{op: "=", args: []*node{a, {op: "value", val: &Value{NULL: true}}}}}}
		default:
			p.bad()
		}
		if not {
			n = &node{op: "not", args: []*node{n}}
		}
		return n
	}
	p.bad()
	return nil
}

func (p *pp) sel() *stmt {
	p.need("SELECT")
	st := &stmt{kind: "SELECT"}
	if !p.pun("*") {
		for {
			st.paths = append(st.paths, p.path())
			if !p.pun(",") {
				break
			}
		}
	}
	p.need("FROM")
	st.table, st.index = p.tref()
	if p.kw("WHERE") {
		st.where = p.or()
	}
	if p.kw("ORDER") {
		p.need("BY")
		st.orderBy = p.ident()
		st.desc = p.kw("DESC")
		if !st.desc {
			p.kw("ASC")
		}
	}
	return st
}

// setValue parses the right side of SET path = ...
func (p *pp) setValue(target path) action {
	if t := p.cur(); t.k == 'i' && p.callNext() {
		switch name := strings.ToLower(t.s); name {
		case "list_append":
			p.i++
			p.needPun("(")
			a := p.operand()
			p.needPun(",")
			b := p.operand()
			p.needPun(")")
			return action{op: "SET", path: target, val: &node{op: name, args: []*node{a, b}}}
		case "set_add", "set_delete":
			p.i++
			p.needPun("(")
			if !slices.Equal(p.path(), target) {
				fail(errVal("%s%s must name the attribute it sets", stmtPrefix, name))
			}
			p.needPun(",")
			v := p.operand()
			p.needPun(")")
			if v.op == "value" && !slices.Contains([]string{"SS", "NS", "BS"}, v.val.kind()) {
				fail(errVal("%s%s takes a set", stmtPrefix, name))
			}
			return action{op: map[string]string{"set_add": "ADD", "set_delete": "DELETE"}[name], path: target, val: v}
		}
	}
	v := p.operand()
	if t := p.cur(); t.k == 'p' && (t.s == "+" || t.s == "-") {
		p.i++
		v = &node{op: t.s, args: []*node{v, p.operand()}}
	}
	return action{op: "SET", path: target, val: v}
}

func (p *pp) update() *stmt {
	st := &stmt{kind: "UPDATE"}
	var idx string
	if st.table, idx = p.tref(); idx != "" {
		p.i--
		p.bad()
	}
	for {
		set := p.kw("SET")
		if !set && !p.kw("REMOVE") {
			break
		}
		for {
			target := p.path()
			if set {
				p.needPun("=")
				st.acts = append(st.acts, p.setValue(target))
			} else {
				st.acts = append(st.acts, action{op: "REMOVE", path: target})
			}
			if !p.pun(",") {
				break
			}
		}
	}
	if len(st.acts) == 0 {
		p.bad()
	}
	var ps []path
	for _, a := range st.acts {
		ps = append(ps, a.path)
	}
	(&parser{what: "statement"}).overlap(ps)
	return st
}

var errParams = errVal("Number of parameters in request and statement don't match.")

func parseStmt(src string, params []*Value) (st *stmt, err error) {
	switch n := utf8.RuneCountInString(src); {
	case n < 1:
		return nil, errVal("1 validation error detected: Value at 'statement' failed to satisfy constraint: Member must have length greater than or equal to 1")
	case n > 8192:
		return nil, errVal("1 validation error detected: Value at 'statement' failed to satisfy constraint: Member must have length less than or equal to 8192")
	case params != nil && len(params) == 0:
		return nil, errVal("1 validation error detected: Value at 'parameters' failed to satisfy constraint: Member must have length greater than or equal to 1")
	}
	defer catch(&err)
	toks, err := plex(src)
	if err != nil {
		return nil, err
	}
	p := &pp{t: toks, params: params}
	switch {
	case p.cur().k == 'i' && strings.EqualFold(p.cur().s, "SELECT"):
		st = p.sel()
	case p.kw("EXISTS"):
		p.needPun("(")
		st = p.sel()
		p.needPun(")")
		if st.kind = "EXISTS"; st.index != "" || st.paths != nil || st.orderBy != "" {
			fail(errVal(stmtPrefix + "EXISTS takes SELECT * FROM table WHERE ..."))
		}
	case p.kw("INSERT"):
		p.need("INTO")
		st = &stmt{kind: "INSERT", table: p.ident()}
		p.need("VALUE")
		if st.item = p.value().M; st.item == nil {
			fail(errVal(stmtPrefix + "VALUE must be a map"))
		}
	case p.kw("UPDATE"):
		st = p.update()
	case p.kw("DELETE"):
		p.need("FROM")
		st = &stmt{kind: "DELETE"}
		var idx string
		if st.table, idx = p.tref(); idx != "" {
			p.i--
			p.bad()
		}
	default:
		p.bad()
	}
	if st.kind == "UPDATE" || st.kind == "DELETE" {
		if p.kw("WHERE") {
			st.where = p.or()
		}
		if p.kw("RETURNING") {
			a, b := p.ident(), p.ident()
			st.returning = strings.ToUpper(a + " " + b)
			p.needPun("*")
			if ok := st.returning == "ALL OLD" || st.kind == "UPDATE" && slices.Contains([]string{"ALL NEW", "MODIFIED OLD", "MODIFIED NEW"}, st.returning); !ok {
				fail(errVal(stmtPrefix+"RETURNING %s * is not supported for %s", st.returning, st.kind))
			}
		}
	}
	if p.i < len(p.t) {
		p.bad()
	}
	if p.pi != len(params) {
		return nil, errParams
	}
	return st, nil
}

// ---- running statements ----

// splitKey takes the equality on each key attribute out of a WHERE; the other
// conjuncts come back as a condition.
func splitKey(t *Table, where *node) (Item, *node, error) {
	key := Item{}
	var rest *node
	var walk func(n *node)
	walk = func(n *node) {
		if n.op == "and" {
			walk(n.args[0])
			walk(n.args[1])
			return
		}
		if n.op == "=" && n.args[0].op == "path" && len(n.args[0].path) == 1 && n.args[1].op == "value" {
			name, _ := n.args[0].path[0].(string)
			if slices.Contains(keyNames(t.Keys), name) && key[name] == nil {
				key[name] = n.args[1].val
				return
			}
		}
		rest = join(rest, n, "")
	}
	if where != nil {
		walk(where)
	}
	if len(key) != len(keyNames(t.Keys)) {
		return nil, nil, errVal("Where clause does not contain a mandatory equality on all key attributes")
	}
	return key, rest, nil
}

// pin finds the partitions a PartiQL SELECT reads: those named by = or IN on
// every partition key attribute. The test is nil when it must scan. It also
// checks ORDER BY, which needs one.
func (in *readInput) pin(keys []keyElem) (func(Item) bool, error) {
	names, nh := keyNames(keys), hashCount(keys)
	sets := make([][]*Value, nh)
	var walk func(n *node)
	walk = func(n *node) {
		if n.op == "and" {
			walk(n.args[0])
			walk(n.args[1])
			return
		}
		if (n.op != "=" && n.op != "in") || n.args[0].op != "path" || len(n.args[0].path) != 1 {
			return
		}
		name, _ := n.args[0].path[0].(string)
		i := slices.Index(names[:nh], name)
		if i < 0 || sets[i] != nil || slices.ContainsFunc(n.args[1:], func(a *node) bool { return a.op != "value" }) {
			return
		}
		for _, a := range n.args[1:] {
			sets[i] = append(sets[i], a.val)
		}
	}
	if in.where != nil {
		walk(in.where)
	}
	pinned := !slices.ContainsFunc(sets, func(v []*Value) bool { return v == nil })
	if in.orderBy != "" {
		switch {
		case in.where == nil:
			return nil, errVal("Must have WHERE clause in the statement when using ORDER BY clause.")
		case !pinned:
			return nil, errVal("Must have at least one non-optional hash key condition in the statement when using ORDER BY clause.")
		case !slices.Contains(names[nh:], in.orderBy):
			return nil, errVal("ORDER BY must name a sort key attribute of the table or index")
		}
	}
	if !pinned {
		return nil, nil
	}
	return func(it Item) bool {
		for i, h := range names[:nh] {
			if !slices.ContainsFunc(sets[i], func(v *Value) bool { return equal(it[h], v) }) {
				return false
			}
		}
		return true
	}, nil
}

// stmtReq is a statement as the three operations receive it.
type stmtReq struct {
	Statement                           string
	Parameters                          []*Value
	ConsistentRead                      bool
	Limit                               *int
	NextToken                           string
	ReturnConsumedCapacity              string
	ReturnValuesOnConditionCheckFailure string
}

func (s *Service) executeStatement(c *core.Ctx, in *stmtReq) (any, error) {
	st, err := parseStmt(in.Statement, in.Parameters)
	if err != nil {
		return nil, err
	}
	if st.kind == "EXISTS" {
		return nil, errVal(stmtPrefix + "EXISTS can only be used in ExecuteTransaction")
	}
	if in.Limit != nil && *in.Limit < 1 {
		return nil, errVal("1 validation error detected: Value '%d' at 'limit' failed to satisfy constraint: Member must have value greater than or equal to 1", *in.Limit)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if st.kind == "SELECT" {
		return s.stmtSelect(c, st, in)
	}
	ch, err := s.applyWrite(c, st, in.ReturnValuesOnConditionCheckFailure)
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	var it Item
	switch st.returning {
	case "ALL OLD":
		it = ch.old
	case "ALL NEW":
		it = ch.new
	case "MODIFIED OLD":
		it = project(ch.old, ch.paths)
	case "MODIFIED NEW":
		it = project(ch.new, ch.paths)
	}
	if len(it) > 0 {
		out["Items"] = []Item{it}
	}
	if cc := consumed(in.ReturnConsumedCapacity, ch.t.Name, writeUnits(ch)); cc != nil {
		out["ConsumedCapacity"] = cc
	}
	return out, nil
}

// stmtSelect runs a SELECT as a Query or Scan; lock held.
func (s *Service) stmtSelect(c *core.Ctx, st *stmt, in *stmtReq) (any, error) {
	ri := &readInput{TableName: st.table, IndexName: st.index, ConsistentRead: in.ConsistentRead, ReturnConsumedCapacity: in.ReturnConsumedCapacity,
		where: st.where, proj: st.paths, orderBy: st.orderBy, desc: st.desc}
	if in.Limit != nil {
		ri.Limit = *in.Limit
	}
	pj, _ := json.Marshal(in.Parameters)
	sum := sha256.Sum256(slices.Concat([]byte(in.Statement), []byte{0}, pj))
	sig := hex.EncodeToString(sum[:8])
	if in.NextToken != "" {
		var tok struct {
			Sig string
			Key Item
		}
		b, err := base64.RawURLEncoding.DecodeString(in.NextToken)
		if err == nil {
			err = json.Unmarshal(b, &tok)
		}
		if err != nil || tok.Sig != sig || len(tok.Key) == 0 {
			return nil, errVal("The provided NextToken is invalid for this statement")
		}
		ri.ExclusiveStartKey = tok.Key
	}
	got, err := s.search(c, ri, false)
	if err != nil {
		return nil, err
	}
	out := map[string]any{"Items": got["Items"]}
	if k, ok := got["LastEvaluatedKey"].(Item); ok {
		b, _ := json.Marshal(map[string]any{"Sig": sig, "Key": k})
		out["NextToken"], out["LastEvaluatedKey"] = base64.RawURLEncoding.EncodeToString(b), k
	}
	if cc, ok := got["ConsumedCapacity"]; ok {
		out["ConsumedCapacity"] = cc
	}
	return out, nil
}

// stmtOp turns a write statement into a planned operation; lock held.
func (s *Service) stmtOp(c *core.Ctx, st *stmt, onFail string) (string, *writeInput, error) {
	t, err := s.table(c, st.table)
	if err != nil {
		return "", nil, err
	}
	w := &writeInput{TableName: st.table, ReturnValuesOnConditionCheckFailure: onFail}
	hash := []*node{{op: "path", path: path{keyNames(t.Keys)[0]}}}
	if st.kind == "INSERT" {
		w.Item, w.insert, w.cond = st.item, true, &node{op: "attribute_not_exists", args: hash}
		return "Put", w, nil
	}
	var rest *node
	if w.Key, rest, err = splitKey(t, st.where); err != nil {
		return "", nil, err
	}
	w.cond = rest
	op := "Delete"
	if st.kind != "DELETE" { // the item has to exist
		w.cond = &node{op: "attribute_exists", args: hash}
		if rest != nil {
			w.cond = join(w.cond, rest, "")
		}
		op, w.acts = "Update", st.acts
		if st.kind == "EXISTS" {
			op = "ConditionCheck"
		}
	}
	return op, w, nil
}

// applyWrite plans and commits one statement; lock held. Deleting an item
// that is not there changes nothing.
func (s *Service) applyWrite(c *core.Ctx, st *stmt, onFail string) (*change, error) {
	op, w, err := s.stmtOp(c, st, onFail)
	if err != nil {
		return nil, err
	}
	ch, err := s.plan(c, op, w)
	if err != nil {
		return nil, err
	}
	if ch.fail != nil {
		return nil, ch.failure(onFail)
	}
	if ch.old != nil || ch.new != nil {
		s.commit(ch)
	}
	return ch, nil
}

// keyedRead serves a SELECT of one item by its full key; lock held.
func (s *Service) keyedRead(c *core.Ctx, st *stmt) (*Table, Item, error) {
	if st.kind != "SELECT" || st.index != "" || st.orderBy != "" {
		return nil, nil, errVal("Select statements within a batch or transaction must read one item of a table by its full primary key")
	}
	t, err := s.table(c, st.table)
	if err != nil {
		return nil, nil, err
	}
	key, rest, err := splitKey(t, st.where)
	if err == nil && rest != nil {
		err = errVal("Select statements within a batch or transaction must read one item of a table by its full primary key")
	}
	if err == nil {
		err = t.checkKey(key)
	}
	if err != nil {
		return nil, nil, err
	}
	it := t.Items[t.key(key)]
	if it != nil && st.paths != nil {
		it = project(it, st.paths)
	}
	return t, it, nil
}

// parseAll parses a list of statements; a bad one leaves its error in errs.
func parseAll(reqs []stmtReq, max int) (sts []*stmt, errs []error, err error) {
	if len(reqs) < 1 || len(reqs) > max {
		return nil, nil, errVal("1 validation error detected: Value at 'statements' failed to satisfy constraint: Member must have length less than or equal to %d and greater than or equal to 1", max)
	}
	sts, errs = make([]*stmt, len(reqs)), make([]error, len(reqs))
	for i, r := range reqs {
		sts[i], errs[i] = parseStmt(r.Statement, r.Parameters)
	}
	return
}

// reads says whether the statements are all reads (true) or all writes.
func reads(sts []*stmt) (bool, error) {
	var r, w int
	for _, st := range sts {
		if st != nil && st.kind == "SELECT" {
			r++
		} else if st != nil {
			w++
		}
	}
	if r > 0 && w > 0 {
		return false, errVal("The statements must be all reads or all writes")
	}
	return r > 0, nil
}

// batchErr renders an error as a BatchStatementError.
func batchErr(err error) map[string]any {
	e := core.AsError(err)
	code := strings.TrimSuffix(e.Code, "Exception")
	if code == "Validation" {
		code = "ValidationError"
	}
	out := map[string]any{"Code": code, "Message": e.Msg}
	if ae, ok := err.(*apiError); ok && ae.extra["Item"] != nil {
		out["Item"] = ae.extra["Item"]
	}
	return out
}

func (s *Service) batchExecuteStatement(c *core.Ctx, in *struct {
	Statements             []stmtReq
	ReturnConsumedCapacity string
}) (any, error) {
	sts, errs, err := parseAll(in.Statements, 25)
	if err != nil {
		return nil, err
	}
	isRead, err := reads(sts)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	resp, units := make([]map[string]any, len(sts)), map[string]float64{}
	for i, st := range sts {
		resp[i] = map[string]any{}
		if errs[i] != nil {
			resp[i]["Error"] = batchErr(errs[i])
			continue
		}
		resp[i]["TableName"] = st.table
		if isRead {
			t, it, err := s.keyedRead(c, st)
			if err != nil {
				resp[i]["Error"] = batchErr(err)
				continue
			}
			if it != nil {
				resp[i]["Item"] = it
			}
			units[t.Name] += readUnits(itemSize(it), in.Statements[i].ConsistentRead)
			continue
		}
		if st.kind == "EXISTS" {
			errs[i] = errVal(stmtPrefix + "EXISTS can only be used in ExecuteTransaction")
			resp[i]["Error"] = batchErr(errs[i])
			continue
		}
		ch, err := s.applyWrite(c, st, in.Statements[i].ReturnValuesOnConditionCheckFailure)
		if err != nil {
			resp[i]["Error"] = batchErr(err)
			continue
		}
		units[ch.t.Name] += writeUnits(ch)
	}
	out := map[string]any{"Responses": resp}
	if caps := capacities(in.ReturnConsumedCapacity, units); caps != nil {
		out["ConsumedCapacity"] = caps
	}
	return out, nil
}

func (s *Service) executeTransaction(c *core.Ctx, in *struct {
	TransactStatements     []stmtReq
	ClientRequestToken     string
	ReturnConsumedCapacity string
}) (any, error) {
	sts, errs, err := parseAll(in.TransactStatements, 100)
	if err != nil {
		return nil, err
	}
	for _, e := range errs {
		if e != nil {
			return nil, e
		}
	}
	isRead, err := reads(sts)
	if err != nil {
		return nil, err
	}
	if isRead {
		s.mu.Lock()
		defer s.mu.Unlock()
		resp, units := make([]map[string]any, len(sts)), map[string]float64{}
		for i, st := range sts {
			t, it, err := s.keyedRead(c, st)
			if err != nil {
				return nil, err
			}
			resp[i] = map[string]any{"TableName": t.Name}
			if it != nil {
				resp[i]["Item"] = it
			}
			units[t.Name] += 2 * readUnits(itemSize(it), true) // transactions cost double
		}
		out := map[string]any{"Responses": resp}
		if caps := capacities(in.ReturnConsumedCapacity, units); caps != nil {
			out["ConsumedCapacity"] = caps
		}
		return out, nil
	}
	// The planning below needs the lock, but transact takes it itself, so
	// the statements become operations first; the key checks come in plan.
	ops := make([]txOp, len(sts))
	s.mu.Lock()
	for i, st := range sts {
		if ops[i].op, ops[i].w, err = s.stmtOp(c, st, in.TransactStatements[i].ReturnValuesOnConditionCheckFailure); err != nil {
			s.mu.Unlock()
			return nil, err
		}
	}
	s.mu.Unlock()
	b, _ := json.Marshal(in.TransactStatements)
	out, err := s.transact(c, ops, in.ClientRequestToken, b, in.ReturnConsumedCapacity)
	if err != nil || out != nil {
		return out, err
	}
	return map[string]any{"Responses": []any{}}, nil
}
