package amqp

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"
	"unicode/utf8"
)

// Types of the AMQP type system that have no natural Go counterpart.
type (
	// Described is a described type: Descriptor is a uint64 or a Symbol.
	Described struct{ Descriptor, Value any }
	// Array is an AMQP array (all elements one type); a plain []any is a list.
	// Arrays of symbols decode to []Symbol.
	Array []any
	// Char is the AMQP char type, a UTF-32 code point.
	Char rune
	// Decimal is a decimal32, decimal64 or decimal128 in IEEE 754-2008 bytes.
	Decimal []byte

	// annotations is a map[string]any whose keys go on the wire as symbols.
	annotations map[string]any
)

// Encode appends the AMQP encoding of v: nil, bool, the integer and float
// types (int32 is an AMQP int, int64 and int a long, uint32 a uint, uint64 a
// ulong), Char, time.Time (timestamp, millisecond precision), UUID, []byte
// (binary), string, Symbol, Decimal, Described, []any (list), Array and typed
// slices such as []string, []Symbol and []int64 (arrays), and maps with
// string, Symbol or any keys.
func Encode(b []byte, v any) ([]byte, error) { return appendValue(b, v) }

// Decode reads one value from b and returns it with the rest of b.
// Maps whose keys are all strings or symbols decode to map[string]any, other
// maps to map[any]any; lists to []any; arrays to the typed slice of their
// elements ([]string, []Symbol, []int64, ...; an Array when there is none);
// symbols to Symbol; binary to []byte.
func Decode(b []byte) (any, []byte, error) {
	r := reader{b}
	v, err := r.value()
	return v, r.b, err
}

func appendValue(b []byte, v any) ([]byte, error) {
	switch x := v.(type) {
	case nil:
		return append(b, 0x40), nil
	case bool:
		if x {
			return append(b, 0x41), nil
		}
		return append(b, 0x42), nil
	case uint8:
		return append(b, 0x50, x), nil
	case uint16:
		return binary.BigEndian.AppendUint16(append(b, 0x60), x), nil
	case uint32:
		switch {
		case x == 0:
			return append(b, 0x43), nil
		case x < 256:
			return append(b, 0x52, byte(x)), nil
		}
		return binary.BigEndian.AppendUint32(append(b, 0x70), x), nil
	case uint64:
		switch {
		case x == 0:
			return append(b, 0x44), nil
		case x < 256:
			return append(b, 0x53, byte(x)), nil
		}
		return binary.BigEndian.AppendUint64(append(b, 0x80), x), nil
	case uint:
		return appendValue(b, uint64(x))
	case int8:
		return append(b, 0x51, byte(x)), nil
	case int16:
		return binary.BigEndian.AppendUint16(append(b, 0x61), uint16(x)), nil
	case int32:
		if x >= -128 && x <= 127 {
			return append(b, 0x54, byte(x)), nil
		}
		return binary.BigEndian.AppendUint32(append(b, 0x71), uint32(x)), nil
	case int64:
		if x >= -128 && x <= 127 {
			return append(b, 0x55, byte(x)), nil
		}
		return binary.BigEndian.AppendUint64(append(b, 0x81), uint64(x)), nil
	case int:
		return appendValue(b, int64(x))
	case float32:
		return binary.BigEndian.AppendUint32(append(b, 0x72), math.Float32bits(x)), nil
	case float64:
		return binary.BigEndian.AppendUint64(append(b, 0x82), math.Float64bits(x)), nil
	case Char:
		return binary.BigEndian.AppendUint32(append(b, 0x73), uint32(x)), nil
	case time.Time:
		return binary.BigEndian.AppendUint64(append(b, 0x83), uint64(x.UnixMilli())), nil
	case UUID:
		return append(append(b, 0x98), x[:]...), nil
	case []byte:
		return appendVar(b, 0xa0, 0xb0, x), nil
	case string:
		return appendVar(b, 0xa1, 0xb1, []byte(x)), nil
	case Symbol:
		return appendVar(b, 0xa3, 0xb3, []byte(x)), nil
	case Decimal:
		switch len(x) {
		case 4:
			return append(append(b, 0x74), x...), nil
		case 8:
			return append(append(b, 0x84), x...), nil
		case 16:
			return append(append(b, 0x94), x...), nil
		}
		return nil, fmt.Errorf("amqp: decimal of %d bytes", len(x))
	case Described:
		b = append(b, 0x00)
		var err error
		if b, err = appendValue(b, x.Descriptor); err != nil {
			return nil, err
		}
		return appendValue(b, x.Value)
	case []any:
		return appendList(b, x)
	case []string:
		return appendTyped(b, x)
	case []Symbol:
		return appendTyped(b, x)
	case []int64:
		return appendTyped(b, x)
	case []int32:
		return appendTyped(b, x)
	case []int16:
		return appendTyped(b, x)
	case []int8:
		return appendTyped(b, x)
	case []uint64:
		return appendTyped(b, x)
	case []uint32:
		return appendTyped(b, x)
	case []uint16:
		return appendTyped(b, x)
	case []bool:
		return appendTyped(b, x)
	case []float32:
		return appendTyped(b, x)
	case []float64:
		return appendTyped(b, x)
	case []UUID:
		return appendTyped(b, x)
	case []time.Time:
		return appendTyped(b, x)
	case Array:
		return appendArray(b, x, 0)
	case map[string]any:
		return appendMap(b, len(x), func(b []byte) ([]byte, error) { return appendStrMap(b, x, false) })
	case annotations:
		return appendMap(b, len(x), func(b []byte) ([]byte, error) { return appendStrMap(b, x, true) })
	case map[Symbol]any:
		m := make(map[string]any, len(x))
		for k, v := range x {
			m[string(k)] = v
		}
		return appendValue(b, annotations(m))
	case map[any]any:
		return appendMap(b, len(x), func(b []byte) ([]byte, error) {
			var err error
			for k, v := range x { // ponytail: Go map order; sort if byte-stable output is ever needed
				if b, err = appendValue(b, k); err != nil {
					return nil, err
				}
				if b, err = appendValue(b, v); err != nil {
					return nil, err
				}
			}
			return b, nil
		})
	case *Error:
		if x == nil {
			return append(b, 0x40), nil
		}
		return appendValue(b, errorDescribed(x))
	}
	return nil, fmt.Errorf("amqp: cannot encode %T", v)
}

func appendStrMap(b []byte, m map[string]any, sym bool) ([]byte, error) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var err error
	for _, k := range keys {
		var kv any = k
		if sym {
			kv = Symbol(k)
		}
		if b, err = appendValue(b, kv); err != nil {
			return nil, err
		}
		if b, err = appendValue(b, m[k]); err != nil {
			return nil, err
		}
	}
	return b, nil
}

func appendVar(b []byte, c8, c32 byte, data []byte) []byte {
	if len(data) < 256 {
		return append(append(b, c8, byte(len(data))), data...)
	}
	return append(binary.BigEndian.AppendUint32(append(b, c32), uint32(len(data))), data...)
}

// appendCompound writes a list or map: count items in the body made by f.
func appendCompound(b []byte, c8, c32 byte, count int, f func([]byte) ([]byte, error)) ([]byte, error) {
	body, err := f(nil)
	if err != nil {
		return nil, err
	}
	if len(body)+1 < 256 && count < 256 {
		return append(append(b, c8, byte(len(body)+1), byte(count)), body...), nil
	}
	b = append(b, c32)
	b = binary.BigEndian.AppendUint32(b, uint32(len(body)+4))
	b = binary.BigEndian.AppendUint32(b, uint32(count))
	return append(b, body...), nil
}

func appendList(b []byte, l []any) ([]byte, error) {
	if len(l) == 0 {
		return append(b, 0x45), nil
	}
	return appendCompound(b, 0xc0, 0xd0, len(l), func(b []byte) ([]byte, error) {
		var err error
		for _, v := range l {
			if b, err = appendValue(b, v); err != nil {
				return nil, err
			}
		}
		return b, nil
	})
}

func appendMap(b []byte, n int, f func([]byte) ([]byte, error)) ([]byte, error) {
	return appendCompound(b, 0xc1, 0xd1, 2*n, f)
}

// arrayElem is the full-width constructor and payload of a value in an array.
func arrayElem(v any) (byte, []byte, error) {
	switch x := v.(type) {
	case bool:
		if x {
			return 0x56, []byte{1}, nil
		}
		return 0x56, []byte{0}, nil
	case uint8:
		return 0x50, []byte{x}, nil
	case uint16:
		return 0x60, binary.BigEndian.AppendUint16(nil, x), nil
	case uint32:
		return 0x70, binary.BigEndian.AppendUint32(nil, x), nil
	case uint64:
		return 0x80, binary.BigEndian.AppendUint64(nil, x), nil
	case int8:
		return 0x51, []byte{byte(x)}, nil
	case int16:
		return 0x61, binary.BigEndian.AppendUint16(nil, uint16(x)), nil
	case int32:
		return 0x71, binary.BigEndian.AppendUint32(nil, uint32(x)), nil
	case int64:
		return 0x81, binary.BigEndian.AppendUint64(nil, uint64(x)), nil
	case float32:
		return 0x72, binary.BigEndian.AppendUint32(nil, math.Float32bits(x)), nil
	case float64:
		return 0x82, binary.BigEndian.AppendUint64(nil, math.Float64bits(x)), nil
	case Char:
		return 0x73, binary.BigEndian.AppendUint32(nil, uint32(x)), nil
	case time.Time:
		return 0x83, binary.BigEndian.AppendUint64(nil, uint64(x.UnixMilli())), nil
	case UUID:
		return 0x98, x[:], nil
	case []byte:
		return 0xb0, binary.BigEndian.AppendUint32(nil, uint32(len(x))), nil // payload appended by caller below
	case string:
		return 0xb1, nil, nil
	case Symbol:
		return 0xb3, nil, nil
	}
	return 0, nil, fmt.Errorf("amqp: cannot encode %T in an array", v)
}

// appendTyped encodes a typed Go slice as an array.
func appendTyped[T any](b []byte, x []T) ([]byte, error) {
	a := make(Array, len(x))
	for i, v := range x {
		a[i] = v
	}
	code, _, err := arrayElem(*new(T))
	if err != nil {
		return nil, err
	}
	return appendArray(b, a, code)
}

// appendArray encodes a; code is the element constructor when a is empty.
func appendArray(b []byte, a Array, code byte) ([]byte, error) {
	if len(a) > 0 {
		var err error
		if code, _, err = arrayElem(a[0]); err != nil {
			return nil, err
		}
	} else if code == 0 {
		return nil, errors.New("amqp: empty array")
	}
	return appendCompound(b, 0xe0, 0xf0, len(a), func(body []byte) ([]byte, error) {
		body = append(body, code)
		for _, v := range a {
			c, p, err := arrayElem(v)
			if err != nil {
				return nil, err
			}
			if c != code {
				return nil, errors.New("amqp: array elements differ in type")
			}
			switch x := v.(type) {
			case []byte:
				body = append(append(body, p...), x...)
			case string:
				body = append(binary.BigEndian.AppendUint32(body, uint32(len(x))), x...)
			case Symbol:
				body = append(binary.BigEndian.AppendUint32(body, uint32(len(x))), x...)
			default:
				body = append(body, p...)
			}
		}
		return body, nil
	})
}

var errShort = errors.New("amqp: short buffer")

type reader struct{ b []byte }

func (r *reader) take(n int) ([]byte, error) {
	if n < 0 || n > len(r.b) {
		return nil, errShort
	}
	p := r.b[:n]
	r.b = r.b[n:]
	return p, nil
}

func (r *reader) value() (any, error) {
	c, err := r.take(1)
	if err != nil {
		return nil, err
	}
	return r.coded(c[0])
}

// size reads a one- or four-byte length.
func (r *reader) size(wide bool) (int, error) {
	if !wide {
		p, err := r.take(1)
		if err != nil {
			return 0, err
		}
		return int(p[0]), nil
	}
	p, err := r.take(4)
	if err != nil {
		return 0, err
	}
	n := binary.BigEndian.Uint32(p)
	if uint64(n) > uint64(len(r.b)) {
		return 0, errShort
	}
	return int(n), nil
}

func (r *reader) coded(code byte) (any, error) {
	switch code {
	case 0x00:
		d, err := r.value()
		if err != nil {
			return nil, err
		}
		v, err := r.value()
		if err != nil {
			return nil, err
		}
		return Described{d, v}, nil
	case 0x40:
		return nil, nil
	case 0x41:
		return true, nil
	case 0x42:
		return false, nil
	case 0x43:
		return uint32(0), nil
	case 0x44:
		return uint64(0), nil
	case 0x45:
		return []any{}, nil
	}
	fixed := map[byte]int{0x50: 1, 0x51: 1, 0x52: 1, 0x53: 1, 0x54: 1, 0x55: 1, 0x56: 1, 0x60: 2, 0x61: 2,
		0x70: 4, 0x71: 4, 0x72: 4, 0x73: 4, 0x74: 4, 0x80: 8, 0x81: 8, 0x82: 8, 0x83: 8, 0x84: 8, 0x94: 16, 0x98: 16}
	if n, ok := fixed[code]; ok {
		p, err := r.take(n)
		if err != nil {
			return nil, err
		}
		return fixedValue(code, p)
	}
	switch code {
	case 0xa0, 0xa1, 0xa3, 0xb0, 0xb1, 0xb3:
		n, err := r.size(code >= 0xb0)
		if err != nil {
			return nil, err
		}
		p, err := r.take(n)
		if err != nil {
			return nil, err
		}
		switch code & 0x0f {
		case 0x0:
			return append([]byte{}, p...), nil
		case 0x1:
			if !utf8.Valid(p) {
				return nil, errors.New("amqp: invalid utf-8 string")
			}
			return string(p), nil
		}
		return Symbol(p), nil
	case 0xc0, 0xc1, 0xd0, 0xd1:
		n, err := r.size(code >= 0xd0)
		if err != nil {
			return nil, err
		}
		body, err := r.take(n)
		if err != nil {
			return nil, err
		}
		return r.compound(code, body)
	case 0xe0, 0xf0:
		n, err := r.size(code == 0xf0)
		if err != nil {
			return nil, err
		}
		body, err := r.take(n)
		if err != nil {
			return nil, err
		}
		return decodeArray(code == 0xf0, body)
	}
	return nil, fmt.Errorf("amqp: unknown type constructor 0x%02x", code)
}

func fixedValue(code byte, p []byte) (any, error) {
	switch code {
	case 0x50:
		return p[0], nil
	case 0x51:
		return int8(p[0]), nil
	case 0x52:
		return uint32(p[0]), nil
	case 0x53:
		return uint64(p[0]), nil
	case 0x54:
		return int32(int8(p[0])), nil
	case 0x55:
		return int64(int8(p[0])), nil
	case 0x56:
		return p[0] != 0, nil
	case 0x60:
		return binary.BigEndian.Uint16(p), nil
	case 0x61:
		return int16(binary.BigEndian.Uint16(p)), nil
	case 0x70:
		return binary.BigEndian.Uint32(p), nil
	case 0x71:
		return int32(binary.BigEndian.Uint32(p)), nil
	case 0x72:
		return math.Float32frombits(binary.BigEndian.Uint32(p)), nil
	case 0x73:
		return Char(binary.BigEndian.Uint32(p)), nil
	case 0x80:
		return binary.BigEndian.Uint64(p), nil
	case 0x81:
		return int64(binary.BigEndian.Uint64(p)), nil
	case 0x82:
		return math.Float64frombits(binary.BigEndian.Uint64(p)), nil
	case 0x83:
		return time.UnixMilli(int64(binary.BigEndian.Uint64(p))).UTC(), nil
	case 0x98:
		var u UUID
		copy(u[:], p)
		return u, nil
	}
	return Decimal(append([]byte{}, p...)), nil // 0x74, 0x84, 0x94
}

// compound decodes a list or map whose size-prefixed body (count included) is body.
func (r *reader) compound(code byte, body []byte) (any, error) {
	wide := code >= 0xd0
	w := 1
	if wide {
		w = 4
	}
	if len(body) < w {
		return nil, errShort
	}
	var cnt int
	if wide {
		cnt = int(binary.BigEndian.Uint32(body))
	} else {
		cnt = int(body[0])
	}
	sub := reader{body[w:]}
	if cnt > len(sub.b) { // every item takes at least a byte
		return nil, errShort
	}
	items := make([]any, 0, cnt)
	for range cnt {
		v, err := sub.value()
		if err != nil {
			return nil, err
		}
		items = append(items, v)
	}
	if code == 0xc0 || code == 0xd0 {
		return items, nil
	}
	if cnt%2 != 0 {
		return nil, errors.New("amqp: map with an odd item count")
	}
	return buildMap(items)
}

func buildMap(items []any) (any, error) {
	strKeys := true
	for i := 0; i < len(items); i += 2 {
		switch items[i].(type) {
		case string, Symbol:
		default:
			strKeys = false
		}
	}
	if strKeys {
		m := make(map[string]any, len(items)/2)
		for i := 0; i < len(items); i += 2 {
			m[keyString(items[i])] = items[i+1]
		}
		return m, nil
	}
	m := make(map[any]any, len(items)/2)
	for i := 0; i < len(items); i += 2 {
		if !hashable(items[i]) {
			return nil, errors.New("amqp: unhashable map key")
		}
		m[items[i]] = items[i+1]
	}
	return m, nil
}

func hashable(v any) bool {
	switch v.(type) {
	case []any, []Symbol, Array, Decimal, []byte, map[string]any, map[any]any, Described:
		return false
	}
	return true
}

func keyString(k any) string {
	switch x := k.(type) {
	case string:
		return x
	case Symbol:
		return string(x)
	}
	return fmt.Sprint(k)
}

func decodeArray(wide bool, body []byte) (any, error) {
	w := 1
	if wide {
		w = 4
	}
	if len(body) < w+1 {
		return nil, errShort
	}
	var cnt int
	if wide {
		cnt = int(binary.BigEndian.Uint32(body))
	} else {
		cnt = int(body[0])
	}
	sub := reader{body[w:]}
	if cnt > len(sub.b)+1 {
		return nil, errShort
	}
	var desc any
	described := false
	c, _ := sub.take(1)
	code := c[0]
	if code == 0x00 { // described element type: descriptor, then the real constructor
		var err error
		if desc, err = sub.value(); err != nil {
			return nil, err
		}
		described = true
		c, err := sub.take(1)
		if err != nil {
			return nil, err
		}
		code = c[0]
	}
	out := make(Array, 0, cnt)
	for range cnt {
		v, err := sub.coded(code)
		if err != nil {
			return nil, err
		}
		if described {
			v = Described{desc, v}
		}
		out = append(out, v)
	}
	if described {
		return out, nil
	}
	return typedArray(code, out), nil
}

func conv[T any](a Array) any {
	s := make([]T, len(a))
	for i, v := range a {
		s[i] = v.(T)
	}
	return s
}

// typedArray turns the elements of an array into the Go slice of their type
// ([]string, []int64, ...); types without one stay an Array.
func typedArray(code byte, a Array) any {
	switch code {
	case 0xb1, 0xa1:
		return conv[string](a)
	case 0xb3, 0xa3:
		return conv[Symbol](a)
	case 0x81, 0x55:
		return conv[int64](a)
	case 0x71, 0x54:
		return conv[int32](a)
	case 0x61:
		return conv[int16](a)
	case 0x51:
		return conv[int8](a)
	case 0x80, 0x53, 0x44:
		return conv[uint64](a)
	case 0x70, 0x52, 0x43:
		return conv[uint32](a)
	case 0x60:
		return conv[uint16](a)
	case 0x50:
		return conv[uint8](a)
	case 0x56, 0x41, 0x42:
		return conv[bool](a)
	case 0x72:
		return conv[float32](a)
	case 0x82:
		return conv[float64](a)
	case 0x98:
		return conv[UUID](a)
	case 0x83:
		return conv[time.Time](a)
	}
	return a
}
