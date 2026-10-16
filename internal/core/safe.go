package core

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Go runs f in a new goroutine. If f panics, the panic is logged once with
// the name and the stack, and only that goroutine ends. pogi keeps running.
func Go(name string, f func()) {
	go func() {
		defer Recover(name)
		f()
	}()
}

// Recover is for "defer core.Recover(name)" at the top of a goroutine.
// It logs a panic with the stack and swallows it.
func Recover(name string) {
	if r := recover(); r != nil {
		slog.Default().Error("panic in background goroutine", "goroutine", name, "panic", r, "stack", string(debug.Stack()))
	}
}

// guard runs svc for one request. A panic is logged with the stack and the
// client gets a 500 in the cloud's own error shape (Go's server would
// otherwise drop the connection, and the SDK would see a network error).
// http.ErrAbortHandler passes through: handlers use it to abort on purpose.
func (h *Hub) guard(svc Service, w *recorder, r *http.Request, c *Ctx) {
	defer func() {
		p := recover()
		if p == nil {
			return
		}
		if p == http.ErrAbortHandler {
			panic(p)
		}
		h.Log.Error("panic in a request", "service", c.Service, "method", r.Method, "path", r.URL.Path, "panic", p, "stack", string(debug.Stack()))
		if bw, ok := w.ResponseWriter.(*bufferWriter); ok { // held for persistence: nothing has reached the client yet
			bw.status = 0
			bw.buf.Reset()
			w.status = 0
			w.body.Reset()
		}
		if w.status != 0 {
			return // the response has started; a second header would be wrong
		}
		writePanic(w, c, svc.Info(), r)
	}()
	// input limits after the signature (as AWS: a badly signed request gets the signature error)
	if h.CheckInput != nil && c.Cloud == AWS && c.bodyErr == nil && h.verify(c) == nil && h.CheckInput(svc, c, w, r) {
		return
	}
	svc.ServeHTTP(w, r)
}

// writePanic answers 500 with the error shape of the request's cloud and protocol.
func writePanic(w http.ResponseWriter, c *Ctx, info Info, r *http.Request) {
	const msg = "pogi hit an internal error. The stack is in the pogi log."
	switch info.Cloud {
	case GCP:
		c.ErrCode = "INTERNAL"
		writePlainJSON(w, map[string]any{"error": map[string]any{"code": 500, "message": msg, "status": "INTERNAL"}})
	case Azure:
		c.ErrCode = "InternalServerError"
		writePlainJSON(w, map[string]any{"error": map[string]any{"code": "InternalServerError", "message": msg}})
	default:
		e := Errorf(http.StatusInternalServerError, "InternalFailure", "%s", msg)
		switch info.Protocol {
		case "query":
			WriteQueryError(w, c, "", e.Status, e.Code, e.Msg)
		case "rest-json":
			RESTJSONError(w, c, e)
		case "rest-xml":
			c.ErrCode = e.Code
			w.Header().Set("Content-Type", "application/xml")
			w.Header().Set("x-amzn-RequestId", c.RequestID)
			w.WriteHeader(e.Status)
			io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?>`+"\n<Error><Code>"+e.Code+"</Code><Message>"+e.Msg+"</Message><RequestId>"+c.RequestID+"</RequestId></Error>")
		default: // json
			_, version, _ := strings.Cut(r.Header.Get("Content-Type"), "application/x-amz-json-")
			WriteJSONError(w, c, version, e)
		}
	}
}

func writePlainJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusInternalServerError)
	json.NewEncoder(w).Encode(v)
}

// grpcRecover turns a panic in a gRPC handler into an INTERNAL error.
// It is deferred with the handler's named error result.
func (h *Hub) grpcRecover(method string, err *error) {
	if p := recover(); p != nil {
		h.Log.Error("panic in a gRPC call", "method", method, "panic", p, "stack", string(debug.Stack()))
		*err = status.Error(codes.Internal, "pogi hit an internal error. The stack is in the pogi log.")
	}
}
