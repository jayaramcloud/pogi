package core

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Time is an AWS timestamp in an output shape: epoch seconds in JSON and
// CBOR (tag 1), ISO 8601 in XML.
type Time time.Time

func (t Time) MarshalJSON() ([]byte, error) {
	return []byte(strconv.FormatFloat(Unix(time.Time(t)), 'f', -1, 64)), nil
}

func (t *Time) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		*t = Time(time.UnixMilli(int64(f * 1000)).UTC())
		return nil
	}
	v, err := time.Parse(time.RFC3339, s)
	*t = Time(v)
	return err
}

// ---- RPCv2 CBOR (RFC 8949), as much as Smithy uses ----

var errCBOR = errors.New("malformed CBOR")

// DecodeCBOR decodes a CBOR request into v by way of its JSON shape.
func DecodeCBOR(b []byte, v any) error {
	if len(b) == 0 {
		return nil
	}
	d := cborDecoder{b: b}
	generic, err := d.value()
	if err != nil {
		return err
	}
	j, err := json.Marshal(generic)
	if err != nil {
		return err
	}
	return json.Unmarshal(j, v)
}

type cborDecoder struct {
	b []byte
	i int
}

func (d *cborDecoder) byte() (byte, error) {
	if d.i >= len(d.b) {
		return 0, errCBOR
	}
	d.i++
	return d.b[d.i-1], nil
}

// head reads a major type and its argument; indefinite is true for length 31.
func (d *cborDecoder) head() (major byte, arg uint64, info byte, err error) {
	c, err := d.byte()
	if err != nil {
		return 0, 0, 0, err
	}
	major, info = c>>5, c&0x1f
	switch {
	case info < 24:
		return major, uint64(info), info, nil
	case info <= 27:
		n := 1 << (info - 24)
		if d.i+n > len(d.b) {
			return 0, 0, 0, errCBOR
		}
		for _, x := range d.b[d.i : d.i+n] {
			arg = arg<<8 | uint64(x)
		}
		d.i += n
		return major, arg, info, nil
	case info == 31:
		return major, 0, info, nil
	}
	return 0, 0, 0, errCBOR
}

func (d *cborDecoder) isBreak() bool {
	if d.i < len(d.b) && d.b[d.i] == 0xff {
		d.i++
		return true
	}
	return false
}

func (d *cborDecoder) value() (any, error) {
	major, arg, info, err := d.head()
	if err != nil {
		return nil, err
	}
	switch major {
	case 0:
		return arg, nil
	case 1:
		return -1 - int64(arg), nil
	case 2, 3:
		var buf []byte
		if info == 31 {
			for !d.isBreak() {
				chunk, err := d.value()
				if err != nil {
					return nil, err
				}
				switch c := chunk.(type) {
				case []byte:
					buf = append(buf, c...)
				case string:
					buf = append(buf, c...)
				}
			}
		} else {
			if d.i+int(arg) > len(d.b) {
				return nil, errCBOR
			}
			buf = d.b[d.i : d.i+int(arg)]
			d.i += int(arg)
		}
		if major == 2 {
			return append([]byte(nil), buf...), nil
		}
		return string(buf), nil
	case 4:
		out := []any{}
		for n := uint64(0); info == 31 && !d.isBreak() || info != 31 && n < arg; n++ {
			v, err := d.value()
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		return out, nil
	case 5:
		out := map[string]any{}
		for n := uint64(0); info == 31 && !d.isBreak() || info != 31 && n < arg; n++ {
			k, err := d.value()
			if err != nil {
				return nil, err
			}
			v, err := d.value()
			if err != nil {
				return nil, err
			}
			out[fmt.Sprint(k)] = v
		}
		return out, nil
	case 6: // tags: keep the value (tag 1 epoch timestamps stay numbers)
		return d.value()
	case 7:
		switch info {
		case 20:
			return false, nil
		case 21:
			return true, nil
		case 22, 23:
			return nil, nil
		case 25:
			return float64(halfToFloat(uint16(arg))), nil
		case 26:
			return float64(math.Float32frombits(uint32(arg))), nil
		case 27:
			return math.Float64frombits(arg), nil
		}
	}
	return nil, errCBOR
}

func halfToFloat(h uint16) float32 {
	sign := uint32(h>>15) << 31
	exp := uint32(h>>10) & 0x1f
	frac := uint32(h & 0x3ff)
	switch exp {
	case 0:
		f := float32(frac) * float32(math.Pow(2, -24))
		if sign != 0 {
			f = -f
		}
		return f
	case 31:
		return math.Float32frombits(sign | 0x7f800000 | frac<<13)
	}
	return math.Float32frombits(sign | (exp+112)<<23 | frac<<13)
}

// EncodeCBOR encodes an output value the way JSON would see it, with byte
// slices as byte strings and Time/time.Time as tag-1 epoch timestamps.
func EncodeCBOR(v any) []byte {
	var e cborEncoder
	if v == nil {
		v = map[string]any{}
	}
	e.encode(reflect.ValueOf(v))
	return e.b
}

type cborEncoder struct{ b []byte }

func (e *cborEncoder) head(major byte, n uint64) {
	switch {
	case n < 24:
		e.b = append(e.b, major<<5|byte(n))
	case n <= math.MaxUint8:
		e.b = append(e.b, major<<5|24, byte(n))
	case n <= math.MaxUint16:
		e.b = binary.BigEndian.AppendUint16(append(e.b, major<<5|25), uint16(n))
	case n <= math.MaxUint32:
		e.b = binary.BigEndian.AppendUint32(append(e.b, major<<5|26), uint32(n))
	default:
		e.b = binary.BigEndian.AppendUint64(append(e.b, major<<5|27), n)
	}
}

var (
	timeT    = reflect.TypeOf(time.Time{})
	awsTimeT = reflect.TypeOf(Time{})
	rawJSONT = reflect.TypeOf(json.RawMessage{})
)

func (e *cborEncoder) float(f float64) {
	e.b = binary.BigEndian.AppendUint64(append(e.b, 0xfb), math.Float64bits(f))
}

func (e *cborEncoder) encode(v reflect.Value) {
	for v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface {
		if v.IsNil() {
			e.b = append(e.b, 0xf6)
			return
		}
		v = v.Elem()
	}
	switch v.Type() {
	case timeT:
		e.b = append(e.b, 0xc1)
		e.float(Unix(v.Interface().(time.Time)))
		return
	case awsTimeT:
		e.b = append(e.b, 0xc1)
		e.float(Unix(time.Time(v.Interface().(Time))))
		return
	case rawJSONT:
		var g any
		json.Unmarshal(v.Bytes(), &g)
		e.encode(reflect.ValueOf(g))
		return
	}
	switch v.Kind() {
	case reflect.Bool:
		e.b = append(e.b, map[bool]byte{true: 0xf5, false: 0xf4}[v.Bool()])
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if n := v.Int(); n >= 0 {
			e.head(0, uint64(n))
		} else {
			e.head(1, uint64(-1-n))
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		e.head(0, v.Uint())
	case reflect.Float32, reflect.Float64:
		e.float(v.Float())
	case reflect.String:
		e.head(3, uint64(v.Len()))
		e.b = append(e.b, v.String()...)
	case reflect.Slice, reflect.Array:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			e.head(2, uint64(v.Len()))
			e.b = append(e.b, v.Bytes()...)
			return
		}
		e.head(4, uint64(v.Len()))
		for i := range v.Len() {
			e.encode(v.Index(i))
		}
	case reflect.Map:
		keys := v.MapKeys()
		sort.Slice(keys, func(i, j int) bool { return fmt.Sprint(keys[i]) < fmt.Sprint(keys[j]) })
		e.head(5, uint64(len(keys)))
		for _, k := range keys {
			e.encode(reflect.ValueOf(fmt.Sprint(k.Interface())))
			e.encode(v.MapIndex(k))
		}
	case reflect.Struct:
		type field struct {
			name string
			v    reflect.Value
		}
		var fields []field
		t := v.Type()
		for i := range t.NumField() {
			f := t.Field(i)
			if !f.IsExported() {
				continue
			}
			name, opts, _ := strings.Cut(f.Tag.Get("json"), ",")
			if name == "-" {
				continue
			}
			if name == "" {
				name = f.Name
			}
			fv := v.Field(i)
			if strings.Contains(opts, "omitempty") && fv.IsZero() {
				continue
			}
			if f.Anonymous && fv.Kind() == reflect.Struct {
				for j := range fv.NumField() {
					if fv.Type().Field(j).IsExported() {
						fields = append(fields, field{fv.Type().Field(j).Name, fv.Field(j)})
					}
				}
				continue
			}
			fields = append(fields, field{name, fv})
		}
		e.head(5, uint64(len(fields)))
		for _, f := range fields {
			e.encode(reflect.ValueOf(f.name))
			e.encode(f.v)
		}
	default:
		e.b = append(e.b, 0xf6)
	}
}
