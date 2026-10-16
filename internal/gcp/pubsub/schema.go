package pubsub

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
)

// Schema checks for topics with schema settings. A message must match the schema in the
// topic's encoding or the publish is refused, as Pub/Sub does.
//
// ponytail: a schema is parsed again on every publish. Cache the parsed form per
// (schema name, revision) if a topic with a schema takes heavy traffic.
// ponytail: protobuf definitions are parsed by a small reader (messages, enums, maps, oneofs, nested
// types, proto2 or proto3; no imports, extensions or groups). Avro JSON ignores fields the schema does
// not name.

// errInvalidMessage is what Pub/Sub says when data does not match the topic's schema.
func errInvalidMessage() error {
	return invalid("Invalid data in message: Message failed schema validation.")
}

// checkSchema parses a schema definition, as ValidateSchema and CreateSchema do.
func checkSchema(sc *pb.Schema) error {
	_, err := parseSchema(sc)
	return err
}

// messageMatches reports whether data is a valid message of the schema in the encoding.
func messageMatches(sc *pb.Schema, enc pb.Encoding, data []byte) error {
	check, err := parseSchema(sc)
	if err != nil {
		return err
	}
	if !check(enc, data) {
		return errInvalidMessage()
	}
	return nil
}

// parseSchema returns the check for a schema: it reports whether data fits in the encoding
// (unspecified means JSON).
func parseSchema(sc *pb.Schema) (func(pb.Encoding, []byte) bool, error) {
	switch sc.GetType() {
	case pb.Schema_AVRO:
		t, err := parseAvro(sc.Definition)
		if err != nil {
			return nil, invalid("Invalid schema definition: %v", err)
		}
		return func(enc pb.Encoding, data []byte) bool {
			if enc == pb.Encoding_BINARY {
				rest, ok := t.binary(data)
				return ok && len(rest) == 0
			}
			var v any
			return json.Unmarshal(data, &v) == nil && t.json(v)
		}, nil
	case pb.Schema_PROTOCOL_BUFFER:
		md, err := parseProto(sc.Definition)
		if err != nil {
			return nil, invalid("Invalid schema definition: %v", err)
		}
		return func(enc pb.Encoding, data []byte) bool {
			m := dynamicpb.NewMessage(md)
			if enc == pb.Encoding_BINARY {
				return proto.Unmarshal(data, m) == nil
			}
			return protojson.Unmarshal(data, m) == nil
		}, nil
	}
	return nil, invalid("A schema needs a type and a definition")
}

// ---- Avro ----

type avroType struct {
	kind     string // null boolean int long float double bytes string record enum fixed array map union
	name     string // full name of a record, enum or fixed type
	fields   []avroField
	symbols  []string
	size     int       // fixed
	elem     *avroType // array items, map values
	branches []*avroType
}

type avroField struct {
	name     string
	typ      *avroType
	optional bool // has a default
}

func parseAvro(def string) (*avroType, error) {
	var v any
	if err := json.Unmarshal([]byte(def), &v); err != nil {
		return nil, fmt.Errorf("not JSON: %w", err)
	}
	return parseAvroType(v, "", map[string]*avroType{})
}

func parseAvroType(v any, ns string, named map[string]*avroType) (*avroType, error) {
	switch x := v.(type) {
	case string:
		switch x {
		case "null", "boolean", "int", "long", "float", "double", "bytes", "string":
			return &avroType{kind: x}, nil
		}
		if t := named[ns+"."+x]; t != nil {
			return t, nil
		}
		if t := named[x]; t != nil {
			return t, nil
		}
		return nil, fmt.Errorf("unknown Avro type %q", x)
	case []any:
		u := &avroType{kind: "union"}
		for _, b := range x {
			t, err := parseAvroType(b, ns, named)
			if err != nil {
				return nil, err
			}
			u.branches = append(u.branches, t)
		}
		return u, nil
	case map[string]any:
		kind, _ := x["type"].(string)
		switch kind {
		case "record", "error", "enum", "fixed":
			name, _ := x["name"].(string)
			if name == "" {
				return nil, fmt.Errorf("a %s needs a name", kind)
			}
			if i := strings.LastIndexByte(name, '.'); i >= 0 {
				ns = name[:i]
			} else if n, ok := x["namespace"].(string); ok {
				ns = n
			}
			if !strings.Contains(name, ".") && ns != "" {
				name = ns + "." + name
			}
			t := &avroType{kind: kind, name: name}
			if kind == "error" {
				t.kind = "record"
			}
			named[name] = t // before the fields, so a record can contain itself
			switch kind {
			case "enum":
				for _, s := range anySlice(x["symbols"]) {
					sym, ok := s.(string)
					if !ok {
						return nil, errors.New("enum symbols must be strings")
					}
					t.symbols = append(t.symbols, sym)
				}
				if len(t.symbols) == 0 {
					return nil, errors.New("an enum needs symbols")
				}
			case "fixed":
				size, ok := x["size"].(float64)
				if !ok || size < 0 {
					return nil, errors.New("fixed needs a size")
				}
				t.size = int(size)
			default:
				fs, ok := x["fields"].([]any)
				if !ok {
					return nil, errors.New("a record needs fields")
				}
				for _, f := range fs {
					fm, _ := f.(map[string]any)
					fname, _ := fm["name"].(string)
					if fname == "" {
						return nil, errors.New("a record field needs a name")
					}
					ft, err := parseAvroType(fm["type"], ns, named)
					if err != nil {
						return nil, err
					}
					_, hasDefault := fm["default"]
					t.fields = append(t.fields, avroField{fname, ft, hasDefault})
				}
			}
			return t, nil
		case "array", "map":
			key := "items"
			if kind == "map" {
				key = "values"
			}
			e, err := parseAvroType(x[key], ns, named)
			if err != nil {
				return nil, err
			}
			return &avroType{kind: kind, elem: e}, nil
		}
		if x["type"] == nil {
			return nil, errors.New(`an Avro type object needs a "type"`)
		}
		return parseAvroType(x["type"], ns, named) // {"type": "string", "logicalType": ...}
	}
	return nil, fmt.Errorf("invalid Avro schema: %v", v)
}

func anySlice(v any) []any { s, _ := v.([]any); return s }

// branchName is how Avro's JSON encoding names a union branch.
func (t *avroType) branchName() string {
	if t.name != "" {
		return t.name
	}
	return t.kind
}

func isLatin1(s string) bool {
	for _, r := range s {
		if r > 0xFF {
			return false
		}
	}
	return true
}

func isInt(v any, bits float64) bool {
	f, ok := v.(float64)
	return ok && f == math.Trunc(f) && f >= -math.Pow(2, bits-1) && f < math.Pow(2, bits-1)
}

// json reports whether v, decoded from JSON, is a value of the type in Avro's JSON encoding.
func (t *avroType) json(v any) bool {
	switch t.kind {
	case "null":
		return v == nil
	case "boolean":
		_, ok := v.(bool)
		return ok
	case "int":
		return isInt(v, 32)
	case "long":
		return isInt(v, 64)
	case "float", "double":
		_, ok := v.(float64)
		return ok
	case "string":
		_, ok := v.(string)
		return ok
	case "bytes":
		s, ok := v.(string)
		return ok && isLatin1(s)
	case "fixed":
		s, ok := v.(string)
		return ok && isLatin1(s) && utf8.RuneCountInString(s) == t.size
	case "enum":
		s, _ := v.(string)
		return slices.Contains(t.symbols, s)
	case "array":
		a, ok := v.([]any)
		return ok && !slices.ContainsFunc(a, func(e any) bool { return !t.elem.json(e) })
	case "map":
		m, ok := v.(map[string]any)
		if !ok {
			return false
		}
		for _, e := range m {
			if !t.elem.json(e) {
				return false
			}
		}
		return true
	case "record":
		m, ok := v.(map[string]any)
		if !ok {
			return false
		}
		for _, f := range t.fields {
			val, present := m[f.name]
			if present && !f.typ.json(val) || !present && !f.optional {
				return false
			}
		}
		return true
	case "union":
		if v == nil {
			return slices.ContainsFunc(t.branches, func(b *avroType) bool { return b.kind == "null" })
		}
		m, ok := v.(map[string]any)
		if !ok || len(m) != 1 {
			return false
		}
		for k, val := range m {
			for _, b := range t.branches {
				if n := b.branchName(); (k == n || strings.HasSuffix(n, "."+k)) && b.kind != "null" {
					return b.json(val)
				}
			}
		}
	}
	return false
}

// binary reads one value of the type from b in Avro's binary encoding and returns what is left.
func (t *avroType) binary(b []byte) ([]byte, bool) {
	varint := func() (int64, bool) {
		n, w := binary.Varint(b)
		if w <= 0 {
			return 0, false
		}
		b = b[w:]
		return n, true
	}
	take := func(n int) bool {
		if n < 0 || n > len(b) {
			return false
		}
		b = b[n:]
		return true
	}
	switch t.kind {
	case "null":
		return b, true
	case "boolean":
		ok := len(b) > 0 && b[0] <= 1
		return b[min(1, len(b)):], ok
	case "int", "long":
		n, ok := varint()
		return b, ok && (t.kind == "long" || n >= math.MinInt32 && n <= math.MaxInt32)
	case "float":
		return b[min(4, len(b)):], take(4)
	case "double":
		return b[min(8, len(b)):], take(8)
	case "string", "bytes":
		n, ok := varint()
		if !ok || n < 0 || n > int64(len(b)) {
			return b, false
		}
		return b[n:], t.kind == "bytes" || utf8.Valid(b[:n])
	case "fixed":
		return b[min(t.size, len(b)):], take(t.size)
	case "enum":
		n, ok := varint()
		return b, ok && n >= 0 && n < int64(len(t.symbols))
	case "array", "map":
		for {
			n, ok := varint()
			if !ok {
				return b, false
			}
			if n == 0 {
				return b, true
			}
			if n < 0 { // a negative count is followed by the block's size in bytes
				n = -n
				if _, ok = varint(); !ok {
					return b, false
				}
			}
			if n > 1<<20 {
				return b, false
			}
			for range n {
				if t.kind == "map" {
					if b, ok = (&avroType{kind: "string"}).binary(b); !ok {
						return b, false
					}
				}
				if b, ok = t.elem.binary(b); !ok {
					return b, false
				}
			}
		}
	case "record":
		for _, f := range t.fields {
			var ok bool
			if b, ok = f.typ.binary(b); !ok {
				return b, false
			}
		}
		return b, true
	case "union":
		n, ok := varint()
		if !ok || n < 0 || n >= int64(len(t.branches)) {
			return b, false
		}
		return t.branches[n].binary(b)
	}
	return b, false
}

// ---- Protocol Buffers ----

var protoScalars = map[string]descriptorpb.FieldDescriptorProto_Type{
	"double": descriptorpb.FieldDescriptorProto_TYPE_DOUBLE, "float": descriptorpb.FieldDescriptorProto_TYPE_FLOAT,
	"int32": descriptorpb.FieldDescriptorProto_TYPE_INT32, "int64": descriptorpb.FieldDescriptorProto_TYPE_INT64,
	"uint32": descriptorpb.FieldDescriptorProto_TYPE_UINT32, "uint64": descriptorpb.FieldDescriptorProto_TYPE_UINT64,
	"sint32": descriptorpb.FieldDescriptorProto_TYPE_SINT32, "sint64": descriptorpb.FieldDescriptorProto_TYPE_SINT64,
	"fixed32": descriptorpb.FieldDescriptorProto_TYPE_FIXED32, "fixed64": descriptorpb.FieldDescriptorProto_TYPE_FIXED64,
	"sfixed32": descriptorpb.FieldDescriptorProto_TYPE_SFIXED32, "sfixed64": descriptorpb.FieldDescriptorProto_TYPE_SFIXED64,
	"bool": descriptorpb.FieldDescriptorProto_TYPE_BOOL, "string": descriptorpb.FieldDescriptorProto_TYPE_STRING,
	"bytes": descriptorpb.FieldDescriptorProto_TYPE_BYTES,
}

// protoReader reads .proto source into a file descriptor, then protodesc checks it.
type protoReader struct {
	toks    []string
	i       int
	proto3  bool
	pkg     string
	isEnum  map[string]bool // full name (".pkg.Outer.Inner") -> is an enum; every declared type is a key
	pending []pendingType
}

// pendingType is a field whose type name is resolved once every type is known.
type pendingType struct {
	f     *descriptorpb.FieldDescriptorProto
	scope string // full name of the enclosing message, "" at the top
}

// protoTokens splits source into identifiers (dots included), numbers, quoted strings and one-character symbols.
func protoTokens(src string) ([]string, error) {
	var out []string
	for i := 0; i < len(src); {
		c := src[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case strings.HasPrefix(src[i:], "//"):
			for i < len(src) && src[i] != '\n' {
				i++
			}
		case strings.HasPrefix(src[i:], "/*"):
			end := strings.Index(src[i+2:], "*/")
			if end < 0 {
				return nil, errors.New("unterminated comment")
			}
			i += end + 4
		case c == '"' || c == '\'':
			j := i + 1
			for j < len(src) && src[j] != c {
				if src[j] == '\\' {
					j++
				}
				j++
			}
			if j >= len(src) {
				return nil, errors.New("unterminated string")
			}
			out = append(out, src[i:j+1])
			i = j + 1
		case c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' && i+1 < len(src) && src[i+1] != ' ':
			j := i + 1
			for j < len(src) && (src[j] == '_' || src[j] == '.' || src[j] >= 'a' && src[j] <= 'z' || src[j] >= 'A' && src[j] <= 'Z' || src[j] >= '0' && src[j] <= '9') {
				j++
			}
			out = append(out, src[i:j])
			i = j
		default:
			out = append(out, string(c))
			i++
		}
	}
	return out, nil
}

func (p *protoReader) peek() string {
	if p.i < len(p.toks) {
		return p.toks[p.i]
	}
	return ""
}

func (p *protoReader) next() string {
	t := p.peek()
	p.i++
	return t
}

func (p *protoReader) expect(want string) error {
	if t := p.next(); t != want {
		return fmt.Errorf("expected %q, found %q", want, t)
	}
	return nil
}

func isIdent(s string) bool {
	return s != "" && (s[0] == '_' || s[0] >= 'a' && s[0] <= 'z' || s[0] >= 'A' && s[0] <= 'Z')
}

// ident reads a name that may have dots (a type reference).
func (p *protoReader) ident() (string, error) {
	t := p.next()
	if !isIdent(t) && !(strings.HasPrefix(t, ".") && len(t) > 1) {
		return "", fmt.Errorf("expected a name, found %q", t)
	}
	return t, nil
}

// skip drops the rest of a statement, braces and brackets included.
func (p *protoReader) skip() error {
	for depth := 0; p.i < len(p.toks); {
		switch p.next() {
		case "{", "[":
			depth++
		case "}", "]":
			depth--
		case ";":
			if depth == 0 {
				return nil
			}
		}
	}
	return errors.New("unexpected end of the schema")
}

func parseProto(def string) (protoreflect.MessageDescriptor, error) {
	toks, err := protoTokens(def)
	if err != nil {
		return nil, err
	}
	p := &protoReader{toks: toks, isEnum: map[string]bool{}}
	fd := &descriptorpb.FileDescriptorProto{Name: proto.String("schema.proto")}
	for p.i < len(p.toks) {
		switch t := p.next(); t {
		case ";":
		case "syntax":
			if err := p.expect("="); err != nil {
				return nil, err
			}
			s := strings.Trim(p.next(), `"'`)
			if s != "proto2" && s != "proto3" {
				return nil, fmt.Errorf("unknown syntax %q", s)
			}
			p.proto3 = s == "proto3"
			if p.proto3 {
				fd.Syntax = proto.String("proto3")
			}
			if err := p.expect(";"); err != nil {
				return nil, err
			}
		case "package":
			if p.pkg, err = p.ident(); err != nil {
				return nil, err
			}
			fd.Package = proto.String(p.pkg)
			if err := p.expect(";"); err != nil {
				return nil, err
			}
		case "option":
			if err := p.skip(); err != nil {
				return nil, err
			}
		case "import":
			return nil, errors.New("imports are not supported")
		case "message":
			m, err := p.message(p.pkg)
			if err != nil {
				return nil, err
			}
			fd.MessageType = append(fd.MessageType, m)
		case "enum":
			e, err := p.enum(p.pkg)
			if err != nil {
				return nil, err
			}
			fd.EnumType = append(fd.EnumType, e)
		default:
			return nil, fmt.Errorf("unexpected %q", t)
		}
	}
	if len(fd.MessageType) == 0 {
		return nil, errors.New("the schema defines no message")
	}
	for _, pt := range p.pending {
		if err := p.resolve(pt); err != nil {
			return nil, err
		}
	}
	file, err := protodesc.NewFile(fd, &protoregistry.Files{})
	if err != nil {
		return nil, err
	}
	return file.Messages().Get(0), nil
}

// resolve finds the declared type a field names, looking from its scope outward, and sets its kind.
func (p *protoReader) resolve(pt pendingType) error {
	ref := pt.f.GetTypeName()
	var full string
	if strings.HasPrefix(ref, ".") {
		full = ref
	} else {
		for scope := pt.scope; ; {
			cand := "." + scope + "." + ref
			if scope == "" {
				cand = "." + ref
			}
			if _, ok := p.isEnum[cand]; ok {
				full = cand
				break
			}
			if scope == "" {
				return fmt.Errorf("unknown type %q", ref)
			}
			scope = scope[:max(strings.LastIndexByte(scope, '.'), 0)]
		}
	}
	enum, ok := p.isEnum[full]
	if !ok {
		return fmt.Errorf("unknown type %q", ref)
	}
	pt.f.TypeName = proto.String(full)
	pt.f.Type = descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
	if enum {
		pt.f.Type = descriptorpb.FieldDescriptorProto_TYPE_ENUM.Enum()
	}
	return nil
}

func qualify(scope, name string) string {
	if scope == "" {
		return "." + name
	}
	return "." + scope + "." + name
}

func (p *protoReader) enum(scope string) (*descriptorpb.EnumDescriptorProto, error) {
	name, err := p.ident()
	if err != nil {
		return nil, err
	}
	p.isEnum[qualify(scope, name)] = true
	e := &descriptorpb.EnumDescriptorProto{Name: proto.String(name)}
	if err := p.expect("{"); err != nil {
		return nil, err
	}
	for t := p.next(); t != "}"; t = p.next() {
		switch {
		case t == ";":
		case t == "option" || t == "reserved":
			if err := p.skip(); err != nil {
				return nil, err
			}
		case isIdent(t):
			if err := p.expect("="); err != nil {
				return nil, err
			}
			n, err := p.number()
			if err != nil {
				return nil, err
			}
			e.Value = append(e.Value, &descriptorpb.EnumValueDescriptorProto{Name: proto.String(t), Number: proto.Int32(n)})
			if err := p.skip(); err != nil { // options and the semicolon
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unexpected %q in enum %s", t, name)
		}
	}
	return e, nil
}

func (p *protoReader) number() (int32, error) {
	t := p.next()
	if t == "-" {
		t += p.next()
	}
	n, err := strconv.ParseInt(t, 0, 32)
	if err != nil {
		return 0, fmt.Errorf("expected a number, found %q", t)
	}
	return int32(n), nil
}

func (p *protoReader) message(scope string) (*descriptorpb.DescriptorProto, error) {
	name, err := p.ident()
	if err != nil {
		return nil, err
	}
	p.isEnum[qualify(scope, name)] = false
	full := strings.TrimPrefix(qualify(scope, name), ".")
	m := &descriptorpb.DescriptorProto{Name: proto.String(name)}
	if err := p.expect("{"); err != nil {
		return nil, err
	}
	var synthetic []*descriptorpb.FieldDescriptorProto // proto3 optional fields, each with a oneof of its own
	for t := p.next(); t != "}"; t = p.next() {
		switch t {
		case ";":
		case "option", "reserved", "extensions":
			if err := p.skip(); err != nil {
				return nil, err
			}
		case "message":
			n, err := p.message(full)
			if err != nil {
				return nil, err
			}
			m.NestedType = append(m.NestedType, n)
		case "enum":
			e, err := p.enum(full)
			if err != nil {
				return nil, err
			}
			m.EnumType = append(m.EnumType, e)
		case "oneof":
			oname, err := p.ident()
			if err != nil {
				return nil, err
			}
			if err := p.expect("{"); err != nil {
				return nil, err
			}
			m.OneofDecl = append(m.OneofDecl, &descriptorpb.OneofDescriptorProto{Name: proto.String(oname)})
			for t := p.next(); t != "}"; t = p.next() {
				if t == "option" {
					if err := p.skip(); err != nil {
						return nil, err
					}
					continue
				}
				p.i--
				f, err := p.field(full, m, "")
				if err != nil {
					return nil, err
				}
				f.OneofIndex = proto.Int32(int32(len(m.OneofDecl) - 1))
			}
		case "extend", "group":
			return nil, fmt.Errorf("%s is not supported", t)
		default:
			p.i--
			label := ""
			if t == "optional" || t == "repeated" || t == "required" {
				label = p.next()
			}
			f, err := p.field(full, m, label)
			if err != nil {
				return nil, err
			}
			if label == "optional" && p.proto3 {
				f.Proto3Optional = proto.Bool(true)
				synthetic = append(synthetic, f)
			}
		}
	}
	for _, f := range synthetic { // synthetic oneofs come after the real ones
		f.OneofIndex = proto.Int32(int32(len(m.OneofDecl)))
		m.OneofDecl = append(m.OneofDecl, &descriptorpb.OneofDescriptorProto{Name: proto.String("_" + f.GetName())})
	}
	return m, nil
}

// field reads "type name = number [options];" (or a map field) and adds it to m.
func (p *protoReader) field(scope string, m *descriptorpb.DescriptorProto, label string) (*descriptorpb.FieldDescriptorProto, error) {
	typ, err := p.ident()
	if err != nil {
		return nil, err
	}
	f := &descriptorpb.FieldDescriptorProto{Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()}
	switch label {
	case "repeated":
		f.Label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()
	case "required":
		if p.proto3 {
			return nil, errors.New(`"required" is not allowed in proto3`)
		}
		f.Label = descriptorpb.FieldDescriptorProto_LABEL_REQUIRED.Enum()
	}
	var key, value string
	if typ == "map" && p.peek() == "<" {
		p.next()
		if key, err = p.ident(); err != nil {
			return nil, err
		}
		if err := p.expect(","); err != nil {
			return nil, err
		}
		if value, err = p.ident(); err != nil {
			return nil, err
		}
		if err := p.expect(">"); err != nil {
			return nil, err
		}
	}
	name, err := p.ident()
	if err != nil {
		return nil, err
	}
	if err := p.expect("="); err != nil {
		return nil, err
	}
	n, err := p.number()
	if err != nil {
		return nil, err
	}
	f.Name, f.Number = proto.String(name), proto.Int32(n)
	if key != "" { // a map is a repeated field of a generated entry message
		parts := strings.Split(name, "_")
		for i, s := range parts {
			parts[i] = strings.ToUpper(s[:min(1, len(s))]) + s[min(1, len(s)):]
		}
		entry := strings.Join(parts, "") + "Entry"
		kf := &descriptorpb.FieldDescriptorProto{Name: proto.String("key"), Number: proto.Int32(1), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()}
		vf := &descriptorpb.FieldDescriptorProto{Name: proto.String("value"), Number: proto.Int32(2), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()}
		for _, kv := range []struct {
			f *descriptorpb.FieldDescriptorProto
			t string
		}{{kf, key}, {vf, value}} {
			if st, ok := protoScalars[kv.t]; ok {
				kv.f.Type = st.Enum()
			} else {
				kv.f.TypeName = proto.String(kv.t)
				p.pending = append(p.pending, pendingType{kv.f, scope})
			}
		}
		m.NestedType = append(m.NestedType, &descriptorpb.DescriptorProto{Name: proto.String(entry), Field: []*descriptorpb.FieldDescriptorProto{kf, vf},
			Options: &descriptorpb.MessageOptions{MapEntry: proto.Bool(true)}})
		p.isEnum[qualify(scope, entry)] = false
		f.Label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()
		f.Type, f.TypeName = descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), proto.String(qualify(scope, entry))
	} else if st, ok := protoScalars[typ]; ok {
		f.Type = st.Enum()
	} else {
		f.TypeName = proto.String(typ)
		p.pending = append(p.pending, pendingType{f, scope})
	}
	if err := p.skip(); err != nil { // [options] and the semicolon
		return nil, err
	}
	m.Field = append(m.Field, f)
	return f, nil
}
