package generic

import (
	"crypto/rand"
	"encoding/hex"
	"math/big"
	"regexp"
	"regexp/syntax"
	"slices"
	"strings"
	"unicode"
)

type kind int

const (
	kOther kind = iota
	kCreate
	kRead
	kList
	kUpdate
	kDelete
	kTag
	kUntag
	kListTags
)

// rtype is a resource type inferred from the operation names.
type rtype struct {
	name string // singular, as in CreateX
	n    string // normalized name
	ids  []string
	own  string // the identifier minted when a create call lacks it
	ops  map[kind][]*operation
	// maybe are operations named with a preposition (GetXFromTemplate) that may be X's own.
	maybe []maybeOp
}

type maybeOp struct {
	op *operation
	k  kind
}

// verbs that begin a resource operation, longest first.
var verbs = []string{"Deregister", "Unregister", "Deprecate", "Register", "Describe", "Create", "Update", "Modify", "Delete", "List", "Get", "Put", "Import"}

// importCreates are the services whose Import<X> operations create an X (Translate terminologies).
var importCreates = map[string]bool{"translate": true}

var verbKind = map[string]kind{"Create": kCreate, "Put": kCreate, "Register": kCreate, "Describe": kRead, "Get": kRead,
	"List": kList, "Update": kUpdate, "Modify": kUpdate, "Delete": kDelete, "Deregister": kDelete, "Unregister": kDelete, "Deprecate": kDelete}

var (
	tagOps    = map[string]kind{"Tag": kTag, "Untag": kUntag, "GetTags": kListTags, "TagResource": kTag, "AddTags": kTag, "AddTagsToResource": kTag, "TagResources": kTag, "UntagResource": kUntag, "RemoveTags": kUntag, "DeleteTags": kUntag, "RemoveTagsFromResource": kUntag, "UntagResources": kUntag, "ListTagsForResource": kListTags, "ListTags": kListTags, "ListTagsOfResource": kListTags}
	prepWords = []string{"In", "Into", "For", "By", "On", "From", "Of", "To", "With"}
)

func splitVerb(name string) (verb, rest string) {
	for _, v := range verbs {
		if r, ok := strings.CutPrefix(name, v); ok && r != "" && unicode.IsUpper(rune(r[0])) {
			return v, r
		}
	}
	return "", name
}

func words(s string) []string {
	var out []string
	start := 0
	rs := []rune(s)
	for i := 1; i < len(rs); i++ {
		if unicode.IsUpper(rs[i]) && (unicode.IsLower(rs[i-1]) || unicode.IsDigit(rs[i-1]) || (i+1 < len(rs) && unicode.IsLower(rs[i+1]))) {
			out = append(out, string(rs[start:i]))
			start = i
		}
	}
	return append(out, string(rs[start:]))
}

func kebab(s string) string {
	ws := words(s)
	for i := range ws {
		ws[i] = strings.ToLower(ws[i])
	}
	return strings.Join(ws, "-")
}

// singular drops a plural ending from the last word of a CamelCase name.
func singular(s string) string {
	switch {
	case strings.HasSuffix(s, "ies") && len(s) > 4:
		return s[:len(s)-3] + "y"
	case strings.HasSuffix(s, "Aliases"): // Alias is not a plural of Alia
		return s[:len(s)-2]
	case strings.HasSuffix(s, "Alias"):
		return s
	case strings.HasSuffix(s, "sses"), strings.HasSuffix(s, "xes"), strings.HasSuffix(s, "ches"), strings.HasSuffix(s, "shes"):
		return s[:len(s)-2]
	case strings.HasSuffix(s, "ss"), strings.HasSuffix(s, "us"), strings.HasSuffix(s, "is"), strings.HasSuffix(s, "ics"):
		return s
	case strings.HasSuffix(s, "s") && len(s) > 2:
		return s[:len(s)-1]
	}
	return s
}

// cutPrep drops a trailing "In<Parent>" or "For<Parent>" from a list name.
func cutPrep(x string) string {
	ws := words(x)
	for i := 1; i < len(ws); i++ {
		if slices.Contains(prepWords, ws[i]) {
			return strings.Join(ws[:i], "")
		}
	}
	return x
}

func (m *model) typeFor(x string) *rtype {
	if t := m.types[norm(x)]; t != nil {
		return t
	}
	return m.types[norm(singular(x))]
}

func (m *model) inferTypes() {
	m.types = map[string]*rtype{}
	ops := make([]*operation, 0, len(m.ops))
	for _, op := range m.ops {
		ops = append(ops, op)
	}
	slices.SortFunc(ops, func(a, b *operation) int { return strings.Compare(a.Name, b.Name) })
	for _, op := range ops {
		if k, ok := tagOps[op.Name]; ok {
			op.kind = k
			continue
		}
		if v, x := splitVerb(op.Name); verbKind[v] == kCreate || v == "Import" && importCreates[m.meta.Name] {
			name := singular(cutPrep(x))
			t := m.types[norm(name)]
			if t == nil {
				t = &rtype{name: name, n: norm(name), ops: map[kind][]*operation{}}
				m.types[t.n] = t
			}
			op.kind, op.typ = kCreate, t
			t.ops[kCreate] = append(t.ops[kCreate], op)
		}
	}
	for _, op := range ops {
		if op.kind != kOther {
			continue
		}
		v, x := splitVerb(op.Name)
		k := verbKind[v]
		if k == kList {
			x = cutPrep(x)
		}
		t := m.typeFor(x)
		if t == nil && k != kList && k != kCreate {
			// GetQuotaFromTemplate reads a Quota; GetPlanFromTemplate does not read a Plan: decided once the ids are known
			if mt := m.typeFor(cutPrep(x)); mt != nil && cutPrep(x) != x {
				mt.maybe = append(mt.maybe, maybeOp{op, k})
			}
		}
		if t == nil {
			continue
		}
		plural := singular(x) != x
		switch {
		case k == kRead && plural && hasList(op.out):
			k = kList
		case k == kList:
		case k == kRead, k == kUpdate, k == kDelete:
		default:
			continue
		}
		op.kind, op.typ = k, t
		t.ops[k] = append(t.ops[k], op)
	}
	for _, t := range m.types {
		m.inferIDs(t)
	}
	for _, t := range m.types {
		for _, mo := range t.maybe {
			if t.fits(mo.op) {
				mo.op.kind, mo.op.typ = mo.k, t
				t.ops[mo.k] = append(t.ops[mo.k], mo.op)
			}
		}
	}
}

// fits says whether an operation's input names every identifier of the type.
func (t *rtype) fits(op *operation) bool {
	if op.in == nil {
		return false
	}
	have := map[string]any{}
	for _, n := range op.in.names {
		have[norm(n)] = true
	}
	for _, id := range t.ids {
		if _, ok := lookupIn(have, id, t); !ok {
			return false
		}
	}
	return true
}

func hasList(sh *shape) bool {
	if sh == nil {
		return false
	}
	for _, r := range sh.Members {
		if r.sh != nil && r.sh.Type == "list" {
			return true
		}
	}
	return false
}

// ownNames are the member names (normalized) that identify a resource of type t by themselves.
func (t *rtype) ownNames() []string {
	return []string{t.n, t.n + "name", t.n + "id", t.n + "arn", "name", "id", "arn"}
}

func (m *model) inferIDs(t *rtype) {
	own := t.ownNames()
	reqStrings := func(op *operation) []string {
		var out []string
		if op.in == nil {
			return nil
		}
		for _, n := range op.in.Required {
			if r := op.in.Members[n]; r != nil && r.sh != nil && r.sh.Type == "string" && r.Location != "header" {
				out = append(out, n)
			}
		}
		return out
	}
	for _, k := range []kind{kRead, kDelete, kUpdate} {
		ops := slices.Clone(t.ops[k]) // the plain Get<X> before the variants
		slices.SortFunc(ops, func(a, b *operation) int { return len(a.Name) - len(b.Name) })
		for _, op := range ops {
			if ids := reqStrings(op); len(ids) > 0 {
				t.ids = ids
				break
			}
		}
		if t.ids != nil {
			break
		}
	}
	for _, mo := range t.maybe { // a reader with a preposition in its name, when there is no plain one
		if t.ids == nil && mo.k != kList {
			t.ids = reqStrings(mo.op)
		}
	}
	if t.ids == nil { // no reader: whatever the create call names
		for _, op := range t.ops[kCreate] {
			if op.in == nil {
				continue
			}
			for _, n := range op.in.names {
				if slices.Contains(own, norm(n)) && op.in.Members[n].sh.Type == "string" {
					t.ids = []string{n}
					break
				}
			}
			if t.ids != nil {
				break
			}
		}
	}
	if t.ids == nil {
		t.ids = []string{t.name + "Id"} // minted, not part of any shape
	}
	for _, id := range t.ids {
		if slices.Contains(own, norm(id)) {
			t.own = id
		}
	}
	if t.own == "" {
		t.own = t.ids[len(t.ids)-1]
	}
}

// idShape returns the shape of identifier id of t, taken from the operations that have it. One that is not
// an id-or-ARN shape wins (UpdateAlias takes an AliasIdOrArn, CreateAlias makes an AliasId); the order is fixed so
// that a resource's ids do not change from run to run.
func (t *rtype) idShape(id string) *shape {
	var first *shape
	for k := kOther; k <= kListTags; k++ {
		for _, op := range t.ops[k] {
			if op.in == nil {
				continue
			}
			if r := op.in.Members[id]; r != nil && r.sh != nil {
				if !strings.Contains(r.sh.Pattern, "arn:") {
					return r.sh
				}
				if first == nil {
					first = r.sh
				}
			}
		}
	}
	return first
}

// role says what an identifier is: "arn", "id" or "name".
func role(member string) string {
	n := norm(member)
	switch {
	case strings.HasSuffix(n, "arn"):
		return "arn"
	case strings.HasSuffix(n, "id"):
		return "id"
	}
	return "name"
}

// stem is the part of an identifier's name before Id, Name or Arn ("DetectorId" is "Detector").
func stem(member string) string {
	for _, suf := range []string{"Name", "Id", "ID", "Arn", "ARN"} {
		if s, ok := strings.CutSuffix(member, suf); ok && s != "" {
			return s
		}
	}
	return ""
}

// prefix is a short name for minted ids: initials of several words, else the first letters.
func prefix(name string) string {
	ws := words(name)
	if len(ws) == 1 {
		return strings.ToLower(ws[0][:min(3, len(ws[0]))])
	}
	var b strings.Builder
	for _, w := range ws {
		b.WriteString(strings.ToLower(w[:1]))
	}
	return b.String()
}

// mintID makes an identifier for a member: from its pattern when it has one, else <prefix>-<hex>.
func mintID(sh *shape, typ string) string {
	lo, hi := 1, 255
	if sh != nil {
		if sh.Min != nil {
			lo = int(*sh.Min)
		}
		if sh.Max != nil {
			hi = int(*sh.Max)
		}
		if sh.Pattern != "" {
			if s, ok := fromPattern(sh.Pattern, lo, hi); ok && s != "" { // a pattern that allows anything can give an empty id
				return s
			}
		}
	}
	s := prefix(typ) + "-" + randHex(17)
	if len(s) > hi {
		s = s[:hi]
	}
	for len(s) < lo {
		s += randHex(1)
	}
	return s
}

func randHex(n int) string {
	b := make([]byte, (n+1)/2)
	rand.Read(b)
	return hex.EncodeToString(b)[:n]
}

func randInt(n int) int {
	v, _ := rand.Int(rand.Reader, big.NewInt(int64(n)))
	return int(v.Int64())
}

// fromPattern produces a string that matches a model pattern and its length bounds.
func fromPattern(pat string, lo, hi int) (string, bool) {
	re, err := syntax.Parse(pat, syntax.Perl)
	if err != nil {
		return "", false
	}
	cre, err := regexp.Compile(pat)
	if err != nil {
		return "", false
	}
	arn := ""
	for _, reps := range []int{17, 12, 8, 4, 1, 32} {
		for range 8 { // a pattern that takes an id or an ARN (alias-x|arn:...) makes an id, unless it keeps coming out as an ARN
			var b strings.Builder
			gen(re, &b, reps)
			s := b.String()
			switch {
			case len(s) < lo || len(s) > hi || !cre.MatchString(s):
			case strings.HasPrefix(s, "arn:"):
				arn = s
			default:
				return s, true
			}
		}
	}
	return arn, arn != ""
}

func gen(re *syntax.Regexp, b *strings.Builder, reps int) {
	rep := func(n int) {
		for range n {
			gen(re.Sub[0], b, reps)
		}
	}
	switch re.Op {
	case syntax.OpLiteral:
		b.WriteString(string(re.Rune))
	case syntax.OpCharClass:
		var pool []rune
		for i := 0; i+1 < len(re.Rune); i += 2 {
			for r := re.Rune[i]; r <= re.Rune[i+1] && len(pool) < 200; r++ {
				if r < 128 && (r >= '0' && r <= '9' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z') {
					pool = append(pool, r)
				}
			}
		}
		if len(pool) == 0 {
			if len(re.Rune) > 0 {
				b.WriteRune(re.Rune[0])
			}
			return
		}
		if slices.Contains(pool, '0') && slices.Contains(pool, 'a') && !slices.Contains(pool, 'g') { // hex digits
			pool = slices.DeleteFunc(pool, func(r rune) bool { return r > '9' && r < 'a' })
		}
		b.WriteRune(pool[randInt(len(pool))])
	case syntax.OpAnyChar, syntax.OpAnyCharNotNL:
		b.WriteByte('a')
	case syntax.OpCapture:
		gen(re.Sub[0], b, reps)
	case syntax.OpConcat:
		for _, s := range re.Sub {
			gen(s, b, reps)
		}
	case syntax.OpAlternate:
		var ok []*syntax.Regexp
		for _, s := range re.Sub {
			if !strings.HasPrefix(strings.TrimLeft(s.String(), "(?:"), "arn:") { // also when the branch is a group
				ok = append(ok, s)
			}
		}
		if len(ok) == 0 {
			ok = re.Sub
		}
		gen(ok[randInt(len(ok))], b, reps)
	case syntax.OpStar:
		rep(0)
	case syntax.OpPlus:
		rep(reps)
	case syntax.OpQuest:
		rep(1)
	case syntax.OpRepeat:
		n := reps
		if re.Max >= 0 {
			n = min(n, re.Max)
		}
		rep(max(n, re.Min))
	}
}
