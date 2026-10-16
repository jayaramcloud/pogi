package servicebus

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/nrndev/pogi/internal/amqp"
	"github.com/nrndev/pogi/internal/core"
)

// The Service Bus SQL filter and action language: comparisons, AND/OR/NOT,
// IN, LIKE, IS [NOT] NULL, EXISTS, arithmetic, sys.* and user properties,
// @parameters; SET and REMOVE for actions. Expressions compile once into
// closures. nil is "unknown": a filter matches only when it comes out true.

// view is the message a filter reads and an action edits.
type view struct {
	m        *amqp.Message
	count    uint32
	enqueued time.Time
	seq      int64
}

type compiled func(v *view, p map[string]any) any

// sysProps maps sys.<name> (lower-cased) to its getter and, when writable, setter.
var sysProps = map[string]struct {
	get func(*view) any
	set func(*view, any)
}{
	"messageid":        {func(v *view) any { return str(v.props().MessageID) }, func(v *view, x any) { v.props().MessageID = fmt.Sprint(x) }},
	"correlationid":    {func(v *view) any { return str(v.props().CorrelationID) }, func(v *view, x any) { v.props().CorrelationID = fmt.Sprint(x) }},
	"label":            {func(v *view) any { return nz(v.props().Subject) }, func(v *view, x any) { v.props().Subject = fmt.Sprint(x) }},
	"subject":          {func(v *view) any { return nz(v.props().Subject) }, func(v *view, x any) { v.props().Subject = fmt.Sprint(x) }},
	"to":               {func(v *view) any { return nz(v.props().To) }, func(v *view, x any) { v.props().To = fmt.Sprint(x) }},
	"replyto":          {func(v *view) any { return nz(v.props().ReplyTo) }, func(v *view, x any) { v.props().ReplyTo = fmt.Sprint(x) }},
	"replytosessionid": {func(v *view) any { return nz(v.props().ReplyToGroupID) }, func(v *view, x any) { v.props().ReplyToGroupID = fmt.Sprint(x) }},
	"sessionid":        {func(v *view) any { return nz(v.props().GroupID) }, func(v *view, x any) { v.props().GroupID = fmt.Sprint(x) }},
	"contenttype":      {func(v *view) any { return nz(v.props().ContentType) }, func(v *view, x any) { v.props().ContentType = fmt.Sprint(x) }},
	"partitionkey": {func(v *view) any { return v.m.MessageAnnotations["x-opt-partition-key"] }, func(v *view, x any) {
		if v.m.MessageAnnotations == nil {
			v.m.MessageAnnotations = map[string]any{}
		}
		v.m.MessageAnnotations["x-opt-partition-key"] = fmt.Sprint(x)
	}},
	"timetolive": {func(v *view) any {
		if v.m.Header == nil || v.m.Header.TTL == 0 {
			return nil
		}
		return v.m.Header.TTL.Seconds()
	}, func(v *view, x any) {
		if f, ok := num(x); ok {
			if v.m.Header == nil {
				v.m.Header = &amqp.Header{}
			}
			v.m.Header.TTL = time.Duration(f * float64(time.Second))
		}
	}},
	"deliverycount":  {func(v *view) any { return int64(v.count) }, nil},
	"sequencenumber": {func(v *view) any { return v.seq }, nil},
	"enqueuedtimeutc": {func(v *view) any {
		if v.enqueued.IsZero() {
			return nil
		}
		return v.enqueued
	}, nil},
}

func (v *view) props() *amqp.Properties {
	if v.m.Properties == nil {
		v.m.Properties = &amqp.Properties{}
	}
	return v.m.Properties
}

// str renders an id (string, UUID, uint64, []byte) as text; nil and "" are null.
func str(x any) any {
	switch t := x.(type) {
	case nil:
		return nil
	case string:
		return nz(t)
	case []byte:
		return string(t)
	}
	return fmt.Sprint(x)
}

func nz(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// norm turns AMQP numeric types into int64 and float64.
func norm(x any) any {
	switch t := x.(type) {
	case int:
		return int64(t)
	case int8:
		return int64(t)
	case int16:
		return int64(t)
	case int32:
		return int64(t)
	case uint8:
		return int64(t)
	case uint16:
		return int64(t)
	case uint32:
		return int64(t)
	case uint64:
		return int64(t)
	case uint:
		return int64(t)
	case float32:
		return float64(t)
	case amqp.Symbol:
		return string(t)
	}
	return x
}

func (v *view) get(name string) any {
	if rest, ok := cutFold(name, "sys."); ok {
		if p, ok := sysProps[strings.ToLower(rest)]; ok {
			return p.get(v)
		}
		return nil
	}
	name = strings.TrimPrefix(name, "user.")
	if x, ok := v.m.ApplicationProperties[name]; ok {
		return norm(x)
	}
	for k, x := range v.m.ApplicationProperties {
		if strings.EqualFold(k, name) {
			return norm(x)
		}
	}
	return nil
}

func (v *view) has(name string) bool {
	if rest, ok := cutFold(name, "sys."); ok {
		p, ok := sysProps[strings.ToLower(rest)]
		return ok && p.get(v) != nil
	}
	name = strings.TrimPrefix(name, "user.")
	for k := range v.m.ApplicationProperties {
		if strings.EqualFold(k, name) {
			return true
		}
	}
	return false
}

func cutFold(s, prefix string) (string, bool) {
	if len(s) >= len(prefix) && strings.EqualFold(s[:len(prefix)], prefix) {
		return s[len(prefix):], true
	}
	return s, false
}

// ---- lexer ----

type token struct {
	kind byte // 'i' ident, 'b' [bracketed ident], 's' string, 'n' number, 'p' @param, 'o' operator, 0 end
	text string
}

func lex(src string) ([]token, error) {
	var out []token
	for i := 0; i < len(src); {
		c := rune(src[i])
		switch {
		case unicode.IsSpace(c):
			i++
		case c == '\'':
			var b strings.Builder
			i++
			for {
				if i >= len(src) {
					return nil, fmt.Errorf("unterminated string literal")
				}
				if src[i] == '\'' {
					if i+1 < len(src) && src[i+1] == '\'' {
						b.WriteByte('\'')
						i += 2
						continue
					}
					i++
					break
				}
				b.WriteByte(src[i])
				i++
			}
			out = append(out, token{'s', b.String()})
		case c == '[':
			j := strings.IndexByte(src[i:], ']')
			if j < 0 {
				return nil, fmt.Errorf("unterminated [property]")
			}
			out = append(out, token{'b', src[i+1 : i+j]})
			i += j + 1
		case unicode.IsDigit(c) || c == '.' && i+1 < len(src) && unicode.IsDigit(rune(src[i+1])):
			j := i
			for j < len(src) && (unicode.IsDigit(rune(src[j])) || src[j] == '.') {
				j++
			}
			if j < len(src) && (src[j] == 'e' || src[j] == 'E') {
				k := j + 1
				if k < len(src) && (src[k] == '+' || src[k] == '-') {
					k++
				}
				if k < len(src) && unicode.IsDigit(rune(src[k])) {
					for k < len(src) && unicode.IsDigit(rune(src[k])) {
						k++
					}
					j = k
				}
			}
			out = append(out, token{'n', src[i:j]})
			i = j
		case c == '@':
			j := i + 1
			for j < len(src) && (unicode.IsLetter(rune(src[j])) || unicode.IsDigit(rune(src[j])) || src[j] == '_') {
				j++
			}
			out = append(out, token{'p', src[i+1 : j]})
			i = j
		case unicode.IsLetter(c) || c == '_' || c == '$':
			j := i
			for j < len(src) && (unicode.IsLetter(rune(src[j])) || unicode.IsDigit(rune(src[j])) || src[j] == '_' || src[j] == '.' || src[j] == '$') {
				j++
			}
			out = append(out, token{'i', src[i:j]})
			i = j
		default:
			if i+1 < len(src) {
				switch two := src[i : i+2]; two {
				case "<=", ">=", "<>", "!=", "==":
					out = append(out, token{'o', two})
					i += 2
					continue
				}
			}
			if strings.ContainsRune("=<>+-*/%(),;", c) {
				out = append(out, token{'o', string(c)})
				i++
				continue
			}
			return nil, fmt.Errorf("unexpected character %q", c)
		}
	}
	return append(out, token{}), nil
}

// ---- parser ----

type parser struct {
	toks  []token
	pos   int
	depth int // how deep the parser is nested right now
}

// maxDepth stops a filter like 1,000,000 nested brackets from overflowing the Go stack, which would kill
// the whole process. Real filters nest a few levels.
const maxDepth = 1000

// enter counts one more nesting level. Callers must `defer p.leave()` when it returns nil.
func (p *parser) enter() error {
	if p.depth++; p.depth > maxDepth {
		p.depth--
		return fmt.Errorf("the expression is nested more than %d levels deep", maxDepth)
	}
	return nil
}
func (p *parser) leave() { p.depth-- }

func (p *parser) peek() token { return p.toks[p.pos] }
func (p *parser) next() token {
	t := p.toks[p.pos]
	if t.kind != 0 {
		p.pos++
	}
	return t
}

func (p *parser) kw(w string) bool {
	if t := p.peek(); t.kind == 'i' && strings.EqualFold(t.text, w) {
		p.pos++
		return true
	}
	return false
}

func (p *parser) op(o string) bool {
	if t := p.peek(); t.kind == 'o' && t.text == o {
		p.pos++
		return true
	}
	return false
}

// compileFilter compiles a SQL filter expression.
func compileFilter(src string) (compiled, error) {
	toks, err := lex(src)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks}
	e, err := p.or()
	if err != nil {
		return nil, err
	}
	if t := p.peek(); t.kind != 0 {
		return nil, fmt.Errorf("unexpected %q", t.text)
	}
	return e, nil
}

func (p *parser) or() (compiled, error) {
	if err := p.enter(); err != nil {
		return nil, err
	}
	defer p.leave()
	l, err := p.and()
	for err == nil && p.kw("OR") {
		var r compiled
		if r, err = p.and(); err == nil {
			a, b := l, r
			l = func(v *view, ps map[string]any) any { return or3(a(v, ps), b(v, ps)) }
		}
	}
	return l, err
}

func (p *parser) and() (compiled, error) {
	l, err := p.not()
	for err == nil && p.kw("AND") {
		var r compiled
		if r, err = p.not(); err == nil {
			a, b := l, r
			l = func(v *view, ps map[string]any) any { return and3(a(v, ps), b(v, ps)) }
		}
	}
	return l, err
}

func (p *parser) not() (compiled, error) {
	if err := p.enter(); err != nil {
		return nil, err
	}
	defer p.leave()
	if p.kw("NOT") {
		e, err := p.not()
		if err != nil {
			return nil, err
		}
		return func(v *view, ps map[string]any) any {
			if b, ok := e(v, ps).(bool); ok {
				return !b
			}
			return nil
		}, nil
	}
	return p.cmp()
}

func and3(a, b any) any {
	x, xok := a.(bool)
	y, yok := b.(bool)
	switch {
	case xok && !x, yok && !y:
		return false
	case xok && yok:
		return true
	}
	return nil
}

func or3(a, b any) any {
	x, xok := a.(bool)
	y, yok := b.(bool)
	switch {
	case xok && x, yok && y:
		return true
	case xok && yok:
		return false
	}
	return nil
}

func (p *parser) cmp() (compiled, error) {
	l, err := p.add()
	if err != nil {
		return nil, err
	}
	t := p.peek()
	switch {
	case t.kind == 'o' && strings.Contains(" = == <> != < > <= >= ", " "+t.text+" "):
		p.next()
		r, err := p.add()
		if err != nil {
			return nil, err
		}
		op := t.text
		return func(v *view, ps map[string]any) any { return compare(op, l(v, ps), r(v, ps)) }, nil
	case t.kind == 'i' && strings.EqualFold(t.text, "IS"):
		p.next()
		neg := p.kw("NOT")
		if !p.kw("NULL") {
			return nil, fmt.Errorf("expected NULL after IS")
		}
		return func(v *view, ps map[string]any) any { return (l(v, ps) == nil) != neg }, nil
	}
	neg := false
	if t.kind == 'i' && strings.EqualFold(t.text, "NOT") {
		next := p.toks[p.pos+1]
		if next.kind != 'i' || !strings.EqualFold(next.text, "IN") && !strings.EqualFold(next.text, "LIKE") {
			return l, nil
		}
		p.next()
		neg = true
	}
	switch {
	case p.kw("IN"):
		if !p.op("(") {
			return nil, fmt.Errorf("expected ( after IN")
		}
		var items []compiled
		for {
			e, err := p.add()
			if err != nil {
				return nil, err
			}
			items = append(items, e)
			if p.op(",") {
				continue
			}
			if p.op(")") {
				break
			}
			return nil, fmt.Errorf("expected , or ) in IN list")
		}
		return func(v *view, ps map[string]any) any {
			x := l(v, ps)
			if x == nil {
				return nil
			}
			for _, it := range items {
				if compare("=", x, it(v, ps)) == true {
					return !neg
				}
			}
			return neg
		}, nil
	case p.kw("LIKE"):
		pat, err := p.add()
		if err != nil {
			return nil, err
		}
		var esc compiled
		if p.kw("ESCAPE") {
			if esc, err = p.add(); err != nil {
				return nil, err
			}
		}
		return func(v *view, ps map[string]any) any {
			s, ok1 := l(v, ps).(string)
			pt, ok2 := pat(v, ps).(string)
			if !ok1 || !ok2 {
				return nil
			}
			e := rune(0)
			if esc != nil {
				if es, ok := esc(v, ps).(string); ok && len(es) > 0 {
					e = []rune(es)[0]
				}
			}
			return like([]rune(s), []rune(pt), e) != neg
		}, nil
	}
	return l, nil
}

func like(s, p []rune, esc rune) bool {
	if len(p) == 0 {
		return len(s) == 0
	}
	switch {
	case esc != 0 && p[0] == esc && len(p) > 1:
		return len(s) > 0 && s[0] == p[1] && like(s[1:], p[2:], esc)
	case p[0] == '%':
		for i := 0; i <= len(s); i++ {
			if like(s[i:], p[1:], esc) {
				return true
			}
		}
		return false
	case p[0] == '_':
		return len(s) > 0 && like(s[1:], p[1:], esc)
	}
	return len(s) > 0 && s[0] == p[0] && like(s[1:], p[1:], esc)
}

func num(x any) (float64, bool) {
	switch t := norm(x).(type) {
	case int64:
		return float64(t), true
	case float64:
		return t, true
	}
	return 0, false
}

// compare returns a bool, or nil when an operand is null (or, for ordering, the types differ).
func compare(op string, a, b any) any {
	a, b = norm(a), norm(b)
	if a == nil || b == nil {
		return nil
	}
	c, ok := 0, true
	switch x := a.(type) {
	case int64:
		if y, isInt := b.(int64); isInt {
			c = cmpOrd(x, y)
		} else if f, isNum := num(b); isNum {
			c = cmpOrd(float64(x), f)
		} else {
			ok = false
		}
	case float64:
		if f, isNum := num(b); isNum {
			c = cmpOrd(x, f)
		} else {
			ok = false
		}
	case string:
		if y, is := b.(string); is {
			c = strings.Compare(x, y)
		} else {
			ok = false
		}
	case bool:
		if y, is := b.(bool); is {
			if x != y {
				c = 1
			}
		} else {
			ok = false
		}
	case time.Time:
		if y, is := b.(time.Time); is {
			c = x.Compare(y)
		} else {
			ok = false
		}
	default:
		if fmt.Sprint(a) != fmt.Sprint(b) {
			c = 1
		}
	}
	if !ok {
		switch op {
		case "=", "==":
			return false
		case "<>", "!=":
			return true
		}
		return nil
	}
	switch op {
	case "=", "==":
		return c == 0
	case "<>", "!=":
		return c != 0
	case "<":
		return c < 0
	case ">":
		return c > 0
	case "<=":
		return c <= 0
	}
	return c >= 0
}

func cmpOrd[T int64 | float64](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func (p *parser) add() (compiled, error) {
	l, err := p.mul()
	for err == nil {
		op := ""
		if p.op("+") {
			op = "+"
		} else if p.op("-") {
			op = "-"
		} else {
			break
		}
		var r compiled
		if r, err = p.mul(); err == nil {
			a, b := l, r
			l = func(v *view, ps map[string]any) any { return arith(op, a(v, ps), b(v, ps)) }
		}
	}
	return l, err
}

func (p *parser) mul() (compiled, error) {
	l, err := p.unary()
	for err == nil {
		op := ""
		for _, o := range []string{"*", "/", "%"} {
			if p.op(o) {
				op = o
				break
			}
		}
		if op == "" {
			break
		}
		var r compiled
		if r, err = p.unary(); err == nil {
			a, b := l, r
			l = func(v *view, ps map[string]any) any { return arith(op, a(v, ps), b(v, ps)) }
		}
	}
	return l, err
}

func arith(op string, a, b any) any {
	a, b = norm(a), norm(b)
	if s, ok := a.(string); ok && op == "+" {
		if t, ok := b.(string); ok {
			return s + t
		}
	}
	ai, aInt := a.(int64)
	bi, bInt := b.(int64)
	if aInt && bInt {
		switch {
		case op == "+":
			return ai + bi
		case op == "-":
			return ai - bi
		case op == "*":
			return ai * bi
		case bi == 0:
			return nil
		case op == "/":
			return ai / bi
		}
		return ai % bi
	}
	af, ok1 := num(a)
	bf, ok2 := num(b)
	if !ok1 || !ok2 {
		return nil
	}
	switch op {
	case "+":
		return af + bf
	case "-":
		return af - bf
	case "*":
		return af * bf
	}
	if bf == 0 {
		return nil
	}
	if op == "/" {
		return af / bf
	}
	return math.Mod(af, bf)
}

func (p *parser) unary() (compiled, error) {
	if err := p.enter(); err != nil {
		return nil, err
	}
	defer p.leave()
	if p.op("-") {
		e, err := p.unary()
		if err != nil {
			return nil, err
		}
		return func(v *view, ps map[string]any) any { return arith("-", int64(0), e(v, ps)) }, nil
	}
	if p.op("+") {
		return p.unary()
	}
	return p.primary()
}

func lit(x any) compiled { return func(*view, map[string]any) any { return x } }

var keywords = map[string]bool{"AND": true, "OR": true, "NOT": true, "IN": true, "LIKE": true, "ESCAPE": true, "IS": true, "SET": true, "REMOVE": true}

func (p *parser) primary() (compiled, error) {
	t := p.next()
	switch t.kind {
	case 's':
		return lit(t.text), nil
	case 'n':
		if n, err := strconv.ParseInt(t.text, 10, 64); err == nil {
			return lit(n), nil
		}
		f, err := strconv.ParseFloat(t.text, 64)
		if err != nil {
			return nil, fmt.Errorf("bad number %q", t.text)
		}
		return lit(f), nil
	case 'p':
		name := t.text
		return func(_ *view, ps map[string]any) any { return norm(ps[name]) }, nil
	case 'b':
		name := t.text
		return func(v *view, _ map[string]any) any { return v.get(name) }, nil
	case 'o':
		if t.text == "(" {
			e, err := p.or()
			if err != nil {
				return nil, err
			}
			if !p.op(")") {
				return nil, fmt.Errorf("expected )")
			}
			return e, nil
		}
	case 'i':
		fn := strings.ToUpper(t.text)
		switch fn {
		case "TRUE":
			return lit(true), nil
		case "FALSE":
			return lit(false), nil
		case "NULL":
			return lit(nil), nil
		case "EXISTS", "PROPERTY", "P", "NEWID":
			if !p.op("(") {
				break
			}
			var arg compiled
			switch {
			case fn == "EXISTS": // names a property, not its value
				name := p.next()
				if name.kind != 'i' && name.kind != 'b' && name.kind != 's' {
					return nil, fmt.Errorf("expected a property name in EXISTS()")
				}
				arg = lit(name.text)
			case fn != "NEWID":
				var err error
				if arg, err = p.or(); err != nil {
					return nil, err
				}
			}
			if !p.op(")") {
				return nil, fmt.Errorf("expected ) after %s(", t.text)
			}
			return func(v *view, ps map[string]any) any {
				switch fn {
				case "NEWID":
					return core.UUID()
				case "EXISTS":
					return v.has(fmt.Sprint(arg(v, ps)))
				}
				return v.get(fmt.Sprint(arg(v, ps)))
			}, nil
		}
		if keywords[fn] {
			return nil, fmt.Errorf("unexpected keyword %s", t.text)
		}
		name := t.text
		return func(v *view, _ map[string]any) any { return v.get(name) }, nil
	}
	if t.kind == 0 {
		return nil, fmt.Errorf("unexpected end of expression")
	}
	return nil, fmt.Errorf("unexpected %q", t.text)
}

// ---- actions ----

// action is a compiled SET/REMOVE script.
type action []func(v *view, p map[string]any)

func compileAction(src string) (action, error) {
	toks, err := lex(src)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks}
	var out action
	for p.peek().kind != 0 {
		switch {
		case p.kw("SET"):
			name := p.next()
			if name.kind != 'i' && name.kind != 'b' {
				return nil, fmt.Errorf("expected a property name after SET")
			}
			if !p.op("=") {
				return nil, fmt.Errorf("expected = after SET %s", name.text)
			}
			e, err := p.or()
			if err != nil {
				return nil, err
			}
			out = append(out, func(v *view, ps map[string]any) { v.set(name.text, e(v, ps)) })
		case p.kw("REMOVE"):
			name := p.next()
			if name.kind != 'i' && name.kind != 'b' {
				return nil, fmt.Errorf("expected a property name after REMOVE")
			}
			out = append(out, func(v *view, _ map[string]any) { v.remove(name.text) })
		default:
			return nil, fmt.Errorf("expected SET or REMOVE, got %q", p.peek().text)
		}
		for p.op(";") || p.op(",") {
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("empty action")
	}
	return out, nil
}

func (a action) apply(v *view, p map[string]any) {
	for _, f := range a {
		f(v, p)
	}
}

func (v *view) set(name string, x any) {
	if rest, ok := cutFold(name, "sys."); ok {
		if p, ok := sysProps[strings.ToLower(rest)]; ok && p.set != nil && x != nil {
			p.set(v, x)
		}
		return
	}
	name = strings.TrimPrefix(name, "user.")
	if x == nil {
		v.remove(name)
		return
	}
	if v.m.ApplicationProperties == nil {
		v.m.ApplicationProperties = map[string]any{}
	}
	v.m.ApplicationProperties[name] = x
}

func (v *view) remove(name string) {
	if _, ok := cutFold(name, "sys."); ok {
		return
	}
	delete(v.m.ApplicationProperties, strings.TrimPrefix(name, "user."))
}
