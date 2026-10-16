package core

import (
	"bytes"
	"testing"
	"time"
)

func TestCBORRoundTrip(t *testing.T) {
	type dim struct{ Name, Value string }
	in := struct {
		Namespace string
		Values    []float64
		Count     int
		Neg       int
		Blob      []byte
		When      Time
		Dims      []dim
		Skip      string `json:",omitempty"`
		On        bool
	}{"App", []float64{1.5, -2}, 300, -7, []byte{1, 2, 3}, Time(time.UnixMilli(1760000000123).UTC()), []dim{{"Env", "dev"}}, "", true}
	var out struct {
		Namespace string
		Values    []float64
		Count     int
		Neg       int
		Blob      []byte
		When      Time
		Dims      []dim
		On        bool
	}
	if err := DecodeCBOR(EncodeCBOR(in), &out); err != nil {
		t.Fatal(err)
	}
	if out.Namespace != "App" || out.Values[0] != 1.5 || out.Count != 300 || out.Neg != -7 || !bytes.Equal(out.Blob, []byte{1, 2, 3}) ||
		!time.Time(out.When).Equal(time.Time(in.When)) || out.Dims[0].Value != "dev" || !out.On {
		t.Fatalf("round trip = %+v", out)
	}
	// {"a": [1, "x"], "b": 1.5 as half float} with an indefinite-length array.
	var g struct {
		A []any
		B float64
	}
	if err := DecodeCBOR([]byte{0xa2, 0x61, 'a', 0x9f, 0x01, 0x61, 'x', 0xff, 0x61, 'b', 0xf9, 0x3e, 0x00}, &g); err != nil || g.B != 1.5 || len(g.A) != 2 {
		t.Fatalf("decode = %+v %v", g, err)
	}
}
