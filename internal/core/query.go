package core

import (
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Query-protocol field names come from the `json` tag (or the Go field name);
// a `query:"Name,flat,key=K,val=V"` tag overrides the element name, marks a
// list or map as flattened and names map entry elements (default key/value).
type qtag struct {
	name     string
	flat     bool
	key, val string
	omit     bool
}

func tagOf(f reflect.StructField) qtag {
	t := qtag{name: f.Name, key: "key", val: "value"}
	if jt := f.Tag.Get("json"); jt != "" {
		n, _, _ := strings.Cut(jt, ",")
		if n == "-" {
			t.omit = true
		} else if n != "" {
			t.name = n
		}
	}
	if qt, ok := f.Tag.Lookup("query"); ok {
		parts := strings.Split(qt, ",")
		switch parts[0] {
		case "-":
			t.omit = true
		case "":
		default:
			t.name, t.omit = parts[0], false
		}
		for _, p := range parts[1:] {
			switch {
			case p == "flat":
				t.flat = true
			case strings.HasPrefix(p, "key="):
				t.key = p[4:]
			case strings.HasPrefix(p, "val="):
				t.val = p[4:]
			}
		}
	}
	return t
}

// DecodeQuery fills v (a pointer to a struct) from AWS Query form values:
// lists as Name.member.N or flattened Name.N, maps as Name.entry.N.key/value
// (also Key/Name and Value), nested structs as Name.Field.
func DecodeQuery(form url.Values, v any) error {
	root := &qnode{}
	for k, vs := range form {
		if k == "Action" || k == "Version" || len(vs) == 0 {
			continue
		}
		root.set(strings.Split(k, "."), vs[0])
	}
	return root.assign(reflect.ValueOf(v).Elem())
}

type qnode struct {
	val  string
	kids map[string]*qnode
}

func (n *qnode) set(path []string, val string) {
	if len(path) == 0 {
		n.val = val
		return
	}
	if n.kids == nil {
		n.kids = map[string]*qnode{}
	}
	k := n.kids[path[0]]
	if k == nil {
		k = &qnode{}
		n.kids[path[0]] = k
	}
	k.set(path[1:], val)
}

func (n *qnode) child(names ...string) *qnode {
	for _, name := range names {
		if k := n.kids[name]; k != nil {
			return k
		}
	}
	return nil
}

// items returns list items or map entries in index order.
func (n *qnode) items() []*qnode {
	src := n
	if m := n.child("member", "entry"); m != nil {
		src = m
	}
	var idx []int
	for k := range src.kids {
		if i, err := strconv.Atoi(k); err == nil {
			idx = append(idx, i)
		}
	}
	sort.Ints(idx)
	out := make([]*qnode, len(idx))
	for i, x := range idx {
		out[i] = src.kids[strconv.Itoa(x)]
	}
	return out
}

var timeType = reflect.TypeOf(time.Time{})

func (n *qnode) assign(v reflect.Value) error {
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		return n.assign(v.Elem())
	case reflect.Interface:
		v.Set(reflect.ValueOf(n.val))
	case reflect.Struct:
		if v.Type() == timeType || v.Type() == awsTimeT {
			t, err := time.Parse(time.RFC3339, n.val)
			if err != nil {
				return fmt.Errorf("bad timestamp %q", n.val)
			}
			if v.Type() == awsTimeT {
				v.Set(reflect.ValueOf(Time(t)))
			} else {
				v.Set(reflect.ValueOf(t))
			}
			return nil
		}
		t := v.Type()
		for i := range t.NumField() {
			f := t.Field(i)
			if f.Anonymous && f.Type.Kind() == reflect.Struct && !tagOf(f).omit { // promoted fields, like encoding/json
				if err := n.assign(v.Field(i)); err != nil {
					return err
				}
				continue
			}
			if !f.IsExported() {
				continue
			}
			tag := tagOf(f)
			if tag.omit {
				continue
			}
			kid := n.child(tag.name, f.Name)
			if kid == nil {
				continue
			}
			if err := kid.assign(v.Field(i)); err != nil {
				return fmt.Errorf("%s: %w", tag.name, err)
			}
		}
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			b, err := base64.StdEncoding.DecodeString(n.val)
			if err != nil {
				return err
			}
			v.SetBytes(b)
			return nil
		}
		items := n.items()
		s := reflect.MakeSlice(v.Type(), len(items), len(items))
		for i, it := range items {
			if err := it.assign(s.Index(i)); err != nil {
				return err
			}
		}
		v.Set(s)
	case reflect.Map:
		m := reflect.MakeMap(v.Type())
		for _, e := range n.items() {
			k := e.child("key", "Key", "Name")
			if k == nil {
				continue
			}
			kv := reflect.New(v.Type().Key()).Elem()
			if err := k.assign(kv); err != nil {
				return err
			}
			vv := reflect.New(v.Type().Elem()).Elem()
			if val := e.child("value", "Value"); val != nil {
				if err := val.assign(vv); err != nil {
					return err
				}
			}
			m.SetMapIndex(kv, vv)
		}
		v.Set(m)
	case reflect.String:
		v.SetString(n.val)
	case reflect.Bool:
		b, err := strconv.ParseBool(n.val)
		if err != nil {
			return fmt.Errorf("bad boolean %q", n.val)
		}
		v.SetBool(b)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i, err := strconv.ParseInt(n.val, 10, 64)
		if err != nil {
			return fmt.Errorf("bad integer %q", n.val)
		}
		v.SetInt(i)
	case reflect.Float32, reflect.Float64:
		f, err := strconv.ParseFloat(n.val, 64)
		if err != nil {
			return fmt.Errorf("bad number %q", n.val)
		}
		v.SetFloat(f)
	}
	return nil
}

// QueryResponse renders <OpResponse><OpResult>…</OpResult><ResponseMetadata/></OpResponse>.
func QueryResponse(op, xmlns, requestID string, out any) []byte {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n")
	b.WriteString("<" + op + `Response xmlns="` + xmlns + `">`)
	b.WriteString("<" + op + "Result>") // always present: SDKs look for it even when the output is empty
	if out != nil {
		WriteXMLFields(&b, reflect.ValueOf(out))
	}
	b.WriteString("</" + op + "Result>")
	b.WriteString("<ResponseMetadata><RequestId>" + requestID + "</RequestId></ResponseMetadata>")
	b.WriteString("</" + op + "Response>")
	return []byte(b.String())
}

// WriteXMLFields writes the fields of a struct (or the entries of a map) as
// query-protocol XML elements. Empty strings, nil pointers and empty
// lists/maps are omitted, as AWS does.
func WriteXMLFields(b *strings.Builder, v reflect.Value) {
	for v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface {
		if v.IsNil() {
			return
		}
		v = v.Elem()
	}
	if v.Kind() == reflect.Map {
		for _, k := range v.MapKeys() {
			writeXML(b, fmt.Sprint(k.Interface()), v.MapIndex(k), qtag{key: "key", val: "value"})
		}
		return
	}
	if v.Kind() != reflect.Struct {
		return
	}
	t := v.Type()
	for i := range t.NumField() {
		f := t.Field(i)
		if f.Anonymous && f.Type.Kind() == reflect.Struct {
			if !tagOf(f).omit {
				WriteXMLFields(b, v.Field(i))
			}
			continue
		}
		if !f.IsExported() {
			continue
		}
		tag := tagOf(f)
		if !tag.omit {
			writeXML(b, tag.name, v.Field(i), tag)
		}
	}
}

func writeXML(b *strings.Builder, name string, v reflect.Value, t qtag) {
	for v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface {
		if v.IsNil() {
			return
		}
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.Struct:
		if v.Type() == timeType || v.Type() == awsTimeT {
			tm, ok := v.Interface().(time.Time)
			if !ok {
				tm = time.Time(v.Interface().(Time))
			}
			if !tm.IsZero() {
				elem(b, name, tm.UTC().Format(ISO))
			}
			return
		}
		b.WriteString("<" + name + ">")
		WriteXMLFields(b, v)
		b.WriteString("</" + name + ">")
	case reflect.Slice:
		if v.Len() == 0 {
			return
		}
		if v.Type().Elem().Kind() == reflect.Uint8 {
			elem(b, name, base64.StdEncoding.EncodeToString(v.Bytes()))
			return
		}
		if t.flat {
			for i := range v.Len() {
				writeXML(b, name, v.Index(i), qtag{key: "key", val: "value"})
			}
			return
		}
		b.WriteString("<" + name + ">")
		for i := range v.Len() {
			writeXML(b, "member", v.Index(i), qtag{key: "key", val: "value"})
		}
		b.WriteString("</" + name + ">")
	case reflect.Map:
		if v.Len() == 0 {
			return
		}
		keys := v.MapKeys()
		sort.Slice(keys, func(i, j int) bool { return fmt.Sprint(keys[i]) < fmt.Sprint(keys[j]) })
		if !t.flat {
			b.WriteString("<" + name + ">")
		}
		for _, k := range keys {
			if t.flat {
				b.WriteString("<" + name + ">")
			} else {
				b.WriteString("<entry>")
			}
			elem(b, t.key, fmt.Sprint(k.Interface()))
			writeXML(b, t.val, v.MapIndex(k), qtag{key: "key", val: "value"})
			if t.flat {
				b.WriteString("</" + name + ">")
			} else {
				b.WriteString("</entry>")
			}
		}
		if !t.flat {
			b.WriteString("</" + name + ">")
		}
	case reflect.String:
		if v.Len() > 0 {
			elem(b, name, v.String())
		}
	case reflect.Float32, reflect.Float64:
		elem(b, name, strconv.FormatFloat(v.Float(), 'f', -1, 64))
	default:
		elem(b, name, fmt.Sprint(v.Interface()))
	}
}

func elem(b *strings.Builder, name, text string) {
	b.WriteString("<" + name + ">")
	xml.EscapeText(writerOf{b}, []byte(text))
	b.WriteString("</" + name + ">")
}

type writerOf struct{ b *strings.Builder }

func (w writerOf) Write(p []byte) (int, error) { return w.b.Write(p) }
