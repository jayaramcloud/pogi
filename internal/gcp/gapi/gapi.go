// Package gapi holds what Google Cloud services share: the REST mirror that
// maps JSON/HTTP calls onto a service's gRPC handlers, Google-style errors,
// and the long-running operations service (google.longrunning.Operations).
package gapi

import (
	"net/http"
	"slices"
	"strings"

	"github.com/nrndev/pogi/internal/core"
	rpccode "google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Route maps one REST call onto a gRPC method.
type Route struct {
	Method string            // gRPC method name, e.g. "CreateService"
	Field  string            // request field the path's resource name fills, dotted ("parent", "service.name")
	Body   string            // request field the JSON body fills; "" means the whole body is the request
	Desc   *grpc.ServiceDesc // overrides REST.Desc for services with several gRPC services
}

// REST serves a Google API's REST surface by calling its gRPC handlers.
//
// Routes are keyed "VERB shape[:verb]", where shape is the path after
// /<version>/projects/<p>/ with every resource id replaced by "*":
//
//	"POST locations/*/services"          → parent = projects/p/locations/l
//	"GET locations/*/services/*"         → name   = projects/p/locations/l/services/s
//	"POST locations/*/jobs/*:run"        → name   = projects/p/locations/l/jobs/j
//
// The resource name is the path up to its last even segment, so collection
// calls get the parent and item calls get the item. Query parameters become
// request fields by their JSON names (serviceId, pageSize, updateMask).
type REST struct {
	Versions    []string // path versions, e.g. {"v2"}
	Collections []string // first collection after projects/<p>[/locations/<l>] that this service claims
	Desc        *grpc.ServiceDesc
	Impl        any // the gRPC server implementation
	Routes      map[string]Route
}

// Split parses /<version>/projects/<p>/<rest...>; parts keeps locations/<l>.
func (a *REST) Split(path string) (project string, parts []string, ok bool) {
	seg := strings.Split(path, "/")
	if len(seg) < 5 || seg[0] != "" || !slices.Contains(a.Versions, seg[1]) || seg[2] != "projects" {
		return "", nil, false
	}
	parts = seg[4:]
	coll := parts[0]
	if (coll == "locations" || coll == "regions") && len(parts) > 2 { // regions: Dataproc
		coll = parts[2]
	}
	coll, _, _ = strings.Cut(coll, ":")
	return seg[3], parts, slices.Contains(a.Collections, coll)
}

// Match is for core.Info.Match.
func (a *REST) Match(r *http.Request) bool {
	_, _, ok := a.Split(r.URL.Path)
	return ok
}

func (a *REST) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	project, parts, ok := a.Split(r.URL.Path)
	verb := ""
	if ok {
		if last, v, cut := strings.Cut(parts[len(parts)-1], ":"); cut {
			parts[len(parts)-1], verb = last, ":"+v
		}
	}
	shape := slices.Clone(parts)
	for i := 1; i < len(shape); i += 2 {
		shape[i] = "*"
	}
	rt, found := a.Routes[r.Method+" "+strings.Join(shape, "/")+verb]
	c.Op = rt.Method
	if !ok || !found {
		Err(w, c, status.Errorf(codes.Unimplemented, "pogi does not implement %s %s", r.Method, r.URL.Path))
		return
	}
	if err := c.Intercept(); err != nil {
		e := core.AsError(err)
		JSONErr(w, c, e.Status, e.Code, e.Msg)
		return
	}
	res := "projects/" + project
	if n := len(parts) &^ 1; n > 0 {
		res += "/" + strings.Join(parts[:n], "/")
	}
	out, err := a.call(r, rt, res)
	if err != nil {
		Err(w, c, err)
		return
	}
	b, _ := protojson.Marshal(out)
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.Write(b)
}

// call builds the gRPC request from the JSON body, the query parameters and
// the resource name in the path, and calls the handler.
func (a *REST) call(r *http.Request, rt Route, res string) (proto.Message, error) {
	vars := map[string]string{}
	if rt.Field != "" {
		vars[rt.Field] = res
	}
	return invoke(r, a.Impl, rt.Method, rt.Body, vars)
}

// setPath stores v at a dotted path of a JSON object.
func setPath(m map[string]any, path string, v any) {
	k, rest, nested := strings.Cut(path, ".")
	if !nested {
		m[k] = v
		return
	}
	child, _ := m[k].(map[string]any)
	if child == nil {
		child = map[string]any{}
		m[k] = child
	}
	setPath(child, rest, v)
}

// Invalid is an INVALID_ARGUMENT status.
func Invalid(format string, a ...any) error {
	return status.Errorf(codes.InvalidArgument, format, a...)
}

// NotFound is a NOT_FOUND status.
func NotFound(format string, a ...any) error { return status.Errorf(codes.NotFound, format, a...) }

// Exists is an ALREADY_EXISTS status.
func Exists(format string, a ...any) error { return status.Errorf(codes.AlreadyExists, format, a...) }

var httpStatus = map[codes.Code]int{codes.InvalidArgument: 400, codes.FailedPrecondition: 400, codes.OutOfRange: 400,
	codes.Unauthenticated: 401, codes.PermissionDenied: 403, codes.NotFound: 404, codes.AlreadyExists: 409, codes.Aborted: 409,
	codes.ResourceExhausted: 429, codes.Canceled: 499, codes.Unimplemented: 501, codes.Unavailable: 503, codes.DeadlineExceeded: 504}

// Err renders a gRPC status as a Google REST error.
func Err(w http.ResponseWriter, c *core.Ctx, err error) {
	st := status.Convert(err)
	code, ok := httpStatus[st.Code()]
	if !ok {
		code = http.StatusInternalServerError
	}
	JSONErr(w, c, code, rpccode.Code(st.Code()).String(), st.Message())
}

// JSONErr writes {"error": {"code", "message", "status"}}.
func JSONErr(w http.ResponseWriter, c *core.Ctx, code int, name, msg string) {
	core.WriteGoogleError(w, c, code, name, msg)
}
