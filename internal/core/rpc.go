package core

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// Handler is one RPC operation; decode fills the typed input from the wire.
type Handler func(c *Ctx, decode func(any) error) (any, error)

// H adapts a typed operation into a Handler.
func H[In any](fn func(*Ctx, *In) (any, error)) Handler {
	return func(c *Ctx, decode func(any) error) (any, error) {
		in := new(In)
		if err := decode(in); err != nil {
			return nil, Errorf(400, "SerializationException", "%s", PlainJSON(err))
		}
		return fn(c, in)
	}
}

// RPC serves an AWS RPC-style service over JSON 1.0/1.1 and Query, with one
// set of typed operations.
type RPC struct {
	JSONVersion string             // "1.0" or "1.1"
	XMLNS       string             // namespace for Query responses
	Ops         map[string]Handler // operation name → handler
	// QueryCodes maps a JSON error code to its legacy Query code
	// (awsQueryCompatible services such as SQS).
	QueryCodes map[string]string
}

// Wire formats an RPC request can arrive in.
const (
	wireJSON  = iota // awsJson1_0 / awsJson1_1
	wireQuery        // awsQuery
	wireCBOR         // smithy rpcv2Cbor
	wireXCBOR        // legacy CBOR of the Java SDK (Kinesis): X-Amz-Target + application/x-amz-cbor-1.1
)

// TargetOp is the operation of an X-Amz-Target header. It is what follows the last dot, so it also reads
// the long target some SDKs send to CloudTrail (com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101.Op).
func TargetOp(target string) string {
	return target[strings.LastIndex(target, ".")+1:]
}

func (p *RPC) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c := C(r)
	body := Body(r)
	var op string
	var decode func(any) error
	wire := wireJSON
	switch {
	case strings.HasPrefix(r.Header.Get("Smithy-Protocol"), "rpc-v2-cbor"): // POST /service/{Service}/operation/{Op}
		wire, op = wireCBOR, r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		decode = func(v any) error { return DecodeCBOR(body, v) }
	case r.Header.Get("X-Amz-Target") != "":
		op = TargetOp(r.Header.Get("X-Amz-Target"))
		if strings.HasPrefix(r.Header.Get("Content-Type"), "application/x-amz-cbor") {
			wire = wireXCBOR
			decode = func(v any) error { return DecodeCBOR(body, v) }
			break
		}
		decode = func(v any) error {
			if len(body) == 0 {
				return nil
			}
			return json.Unmarshal(body, v)
		}
	default:
		wire = wireQuery
		form, _ := url.ParseQuery(string(body))
		for k, v := range r.URL.Query() {
			if form.Get(k) == "" {
				form[k] = v
			}
		}
		op = form.Get("Action")
		decode = func(v any) error { return DecodeQuery(form, v) }
	}
	c.Op = op
	// A body that was too large or stalled is answered first: whatever it left behind is not worth a
	// "does not implement" or a gzip error.
	if err := c.BodyErr(); err != nil {
		p.writeErr(w, c, wire, err)
		return
	}
	// The Go and Java SDKs gzip a large request (CloudWatch PutMetricData over 10 KB) and say so here.
	if strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
		zr, err := gzip.NewReader(bytes.NewReader(body))
		if err == nil {
			body, err = io.ReadAll(zr)
		}
		if err != nil {
			p.writeErr(w, c, wireJSON, Errorf(400, "SerializationException", "the gzip request body is not valid: %v", err))
			return
		}
	}
	h := p.Ops[op]
	if h == nil {
		code := "UnknownOperationException"
		if wire == wireQuery {
			code = "InvalidAction"
		}
		p.writeErr(w, c, wire, Errorf(400, code, "pogi does not implement %s.%s yet", c.Service, op))
		return
	}
	if err := c.Intercept(); err != nil {
		p.writeErr(w, c, wire, err)
		return
	}
	out, err := h(c, decode)
	if err != nil {
		p.writeErr(w, c, wire, err)
		return
	}
	w.Header().Set("x-amzn-RequestId", c.RequestID)
	switch wire {
	case wireQuery:
		w.Header().Set("Content-Type", "text/xml")
		w.Write(QueryResponse(op, p.XMLNS, c.RequestID, out))
	case wireCBOR, wireXCBOR:
		cborHeaders(w, wire)
		w.Write(EncodeCBOR(out))
	default:
		WriteJSON(w, c, p.JSONVersion, 200, out)
	}
}

func cborHeaders(w http.ResponseWriter, wire int) {
	if wire == wireXCBOR {
		w.Header().Set("Content-Type", "application/x-amz-cbor-1.1")
		return
	}
	w.Header().Set("Smithy-Protocol", "rpc-v2-cbor")
	w.Header().Set("Content-Type", "application/cbor")
}

func (p *RPC) writeErr(w http.ResponseWriter, c *Ctx, wire int, err error) {
	e := AsError(err)
	c.ErrCode = e.Code
	switch wire {
	case wireQuery:
		code := e.Code
		if qc, ok := p.QueryCodes[code]; ok {
			code, _, _ = strings.Cut(qc, ";")
		}
		WriteQueryError(w, c, p.XMLNS, e.Status, code, e.Msg)
	case wireCBOR, wireXCBOR:
		cborHeaders(w, wire)
		if qc, ok := p.QueryCodes[e.Code]; ok {
			w.Header().Set("x-amzn-query-error", qc)
		}
		w.Header().Set("x-amzn-RequestId", c.RequestID)
		w.Header().Set("X-Amzn-ErrorType", e.Code)
		w.WriteHeader(e.Status)
		w.Write(EncodeCBOR(map[string]string{"__type": e.Code, "message": e.Msg}))
	default:
		if qc, ok := p.QueryCodes[e.Code]; ok {
			w.Header().Set("x-amzn-query-error", qc)
		}
		WriteJSONError(w, c, p.JSONVersion, e)
	}
}

// WriteJSON writes an AWS JSON protocol response.
func WriteJSON(w http.ResponseWriter, c *Ctx, version string, status int, out any) {
	if version == "" {
		version = "1.0"
	}
	w.Header().Set("Content-Type", "application/x-amz-json-"+version)
	w.Header().Set("x-amzn-RequestId", c.RequestID)
	w.WriteHeader(status)
	if out == nil {
		w.Write([]byte("{}"))
		return
	}
	json.NewEncoder(w).Encode(out)
}

// WriteJSONError writes an AWS JSON protocol error.
func WriteJSONError(w http.ResponseWriter, c *Ctx, version string, e *Error) {
	c.ErrCode = e.Code
	w.Header().Set("X-Amzn-ErrorType", e.Code)
	WriteJSON(w, c, version, e.Status, map[string]string{"__type": e.Code, "message": e.Msg})
}

// WriteQueryError writes an AWS Query protocol error.
func WriteQueryError(w http.ResponseWriter, c *Ctx, xmlns string, status int, code, msg string) {
	c.ErrCode = code
	kind := "Sender"
	if status >= 500 {
		kind = "Receiver"
	}
	w.Header().Set("Content-Type", "text/xml")
	w.Header().Set("x-amzn-RequestId", c.RequestID)
	w.WriteHeader(status)
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n")
	b.WriteString(`<ErrorResponse xmlns="` + xmlns + `"><Error><Type>` + kind + `</Type>`)
	elem(&b, "Code", code)
	elem(&b, "Message", msg)
	b.WriteString(`</Error>`)
	elem(&b, "RequestId", c.RequestID)
	b.WriteString(`</ErrorResponse>`)
	w.Write([]byte(b.String()))
}

// RESTJSONError writes the error format of REST-JSON services (Lambda, API Gateway…).
func RESTJSONError(w http.ResponseWriter, c *Ctx, err error) {
	e := AsError(err)
	c.ErrCode = e.Code
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Amzn-ErrorType", e.Code)
	w.Header().Set("x-amzn-RequestId", c.RequestID)
	w.WriteHeader(e.Status)
	kind := "User"
	if e.Status >= 500 {
		kind = "Service"
	}
	json.NewEncoder(w).Encode(map[string]string{"Type": kind, "message": e.Msg, "Message": e.Msg, "__type": e.Code})
}

// RESTJSON writes a REST-JSON success response.
func RESTJSON(w http.ResponseWriter, c *Ctx, status int, out any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("x-amzn-RequestId", c.RequestID)
	w.WriteHeader(status)
	if out != nil {
		json.NewEncoder(w).Encode(out)
	}
}
