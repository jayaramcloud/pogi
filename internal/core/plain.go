package core

import (
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
)

// PlainJSON says in a plain sentence what is wrong with a JSON request body that failed to decode. Go's own
// text follows after " — " for anyone who needs the detail. Any other error comes back unchanged.
func PlainJSON(err error) string { return PlainJSONOf("the request body", err) }

// PlainJSONOf is PlainJSON for a document that is not a request body, such as "the plan file".
func PlainJSONOf(what string, err error) string {
	var syn *json.SyntaxError
	var typ *json.UnmarshalTypeError
	var plain string
	switch {
	default:
		return err.Error() // not a JSON error: already plain
	case errors.As(err, &syn), errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, io.EOF), strings.Contains(err.Error(), "unexpected end of JSON"):
		plain = what + " is not valid JSON"
	case errors.As(err, &typ):
		want := "a different type"
		switch typ.Type.Kind() {
		case reflect.String:
			want = "a string"
		case reflect.Bool:
			want = "true or false"
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8,
			reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
			want = "a number"
		case reflect.Slice, reflect.Array:
			want = "a list"
		case reflect.Struct, reflect.Map:
			want = "an object"
		}
		if typ.Field == "" {
			plain = what + " must be " + want + ", not " + typ.Value
		} else {
			plain = "field " + typ.Field + " must be " + want + ", not " + typ.Value
		}
	}
	return plain + " — " + err.Error()
}
