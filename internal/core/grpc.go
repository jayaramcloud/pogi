package core

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	rpccode "google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// GRPCService is a service that also speaks gRPC (Google Cloud APIs). Its
// Info.Targets list the gRPC service names, e.g. "google.pubsub.v1.Publisher".
type GRPCService interface {
	RegisterGRPC(*grpc.Server)
}

// newGRPC builds the gRPC server shared by every service on the main port
// (cleartext HTTP/2), with request logging and chaos rules.
func (h *Hub) newGRPC() *grpc.Server {
	return grpc.NewServer(grpc.ChainUnaryInterceptor(h.grpcUnary), grpc.ChainStreamInterceptor(h.grpcStream))
}

func (h *Hub) grpcCtx(fullMethod string) *Ctx {
	svc, op, _ := strings.Cut(strings.TrimPrefix(fullMethod, "/"), "/")
	c := &Ctx{Hub: h, Cloud: GCP, Service: svc, Op: op, RequestID: NewID()}
	if s := h.targets[svc]; s != nil {
		c.Cloud, c.Service = s.Info().Cloud, s.Info().Name
	}
	return c
}

func (h *Hub) grpcLog(c *Ctx, start time.Time, req, resp any, err error) {
	code := status.Code(err)
	e := Entry{Time: start, Cloud: c.Cloud, Service: c.Service, Op: c.Op, Method: "gRPC", Path: c.Op, Status: grpcHTTP[code],
		Ms: float64(time.Since(start).Microseconds()) / 1000, Account: c.Account, Chaos: c.Chaos, Principal: c.Principal, IAM: c.IAM,
		ReqBody: protoPreview(req), RespBody: protoPreview(resp)}
	if e.Status == 0 {
		e.Status = 500
	}
	if err != nil {
		e.Error = code.String()
		e.RespBody = status.Convert(err).Message()
	}
	h.Requests.Add(e)
}

func protoPreview(v any) string {
	m, ok := v.(proto.Message)
	if !ok || m == nil {
		return ""
	}
	b, err := protojson.Marshal(m)
	if err != nil {
		return ""
	}
	return preview(b, "application/json", "")
}

func (h *Hub) chaosGRPC(c *Ctx) error {
	if e := h.Chaos.Apply(c); e != nil {
		c.Chaos = true
		code, ok := httpGRPC[e.Status]
		if !ok {
			code = codes.Internal
		}
		return status.Error(code, e.Msg)
	}
	return nil
}

// checkGRPC runs the cloud's IAM authorizer for a gRPC request: the bearer token comes from the
// metadata and the resource from the request message.
func (h *Hub) checkGRPC(ctx context.Context, c *Ctx, req any) error {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get("authorization"); len(v) > 0 {
			c.Bearer, _ = strings.CutPrefix(v[0], "Bearer ")
		}
	}
	if m, ok := req.(proto.Message); ok {
		c.Resource = resourceName(m.ProtoReflect())
	}
	if e := h.Check(c); e != nil {
		code, ok := httpGRPC[e.Status]
		if !ok {
			code = codes.PermissionDenied
		}
		return status.Error(code, e.Msg)
	}
	return nil
}

// resourceName is the resource a request message names: its name, parent, topic, subscription,
// resource, database or project field, else the same in a nested message (UpdateTopic.topic).
func resourceName(m protoreflect.Message) string {
	fields := m.Descriptor().Fields()
	for _, n := range []string{"name", "parent", "topic", "subscription", "resource", "database", "project"} {
		if f := fields.ByName(protoreflect.Name(n)); f != nil && f.Kind() == protoreflect.StringKind && !f.IsList() && m.Get(f).String() != "" {
			return m.Get(f).String()
		}
	}
	for i := 0; i < fields.Len(); i++ {
		if f := fields.Get(i); f.Message() != nil && !f.IsList() && !f.IsMap() && m.Has(f) {
			if r := resourceName(m.Get(f).Message()); r != "" {
				return r
			}
		}
	}
	return ""
}

// checkedStream checks IAM on the first message a client streams, which names the resource.
type checkedStream struct {
	grpc.ServerStream
	h    *Hub
	c    *Ctx
	done bool
}

func (s *checkedStream) RecvMsg(m any) error {
	err := s.ServerStream.RecvMsg(m)
	if err == nil && !s.done {
		s.done = true
		err = s.h.checkGRPC(s.Context(), s.c, m)
	}
	return err
}

func (h *Hub) grpcUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (resp any, err error) {
	defer h.grpcRecover(info.FullMethod, &err)
	c, start := h.grpcCtx(info.FullMethod), time.Now()
	err = h.chaosGRPC(c)
	if err == nil {
		err = h.checkGRPC(ctx, c, req)
	}
	if err != nil {
		h.grpcLog(c, start, req, nil, err)
		return nil, err
	}
	resp, err = next(ctx, req)
	if st := h.stores[c.Service]; st != nil && st.mode == "persistent" {
		h.sync(st, false) // on disk before the response, as in serve
	}
	h.grpcLog(c, start, req, resp, err)
	return resp, err
}

func (h *Hub) grpcStream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, next grpc.StreamHandler) (err error) {
	defer h.grpcRecover(info.FullMethod, &err)
	c, start := h.grpcCtx(info.FullMethod), time.Now()
	if err := h.chaosGRPC(c); err != nil {
		h.grpcLog(c, start, nil, nil, err)
		return err
	}
	err = next(srv, &checkedStream{ServerStream: ss, h: h, c: c})
	h.grpcLog(c, start, nil, nil, err)
	return err
}

var grpcHTTP = map[codes.Code]int{
	codes.OK: 200, codes.Canceled: 499, codes.Unknown: 500, codes.InvalidArgument: 400, codes.DeadlineExceeded: 504,
	codes.NotFound: 404, codes.AlreadyExists: 409, codes.PermissionDenied: 403, codes.ResourceExhausted: 429,
	codes.FailedPrecondition: 400, codes.Aborted: 409, codes.OutOfRange: 400, codes.Unimplemented: 501,
	codes.Internal: 500, codes.Unavailable: 503, codes.DataLoss: 500, codes.Unauthenticated: 401,
}

var httpGRPC = map[int]codes.Code{
	400: codes.InvalidArgument, 401: codes.Unauthenticated, 403: codes.PermissionDenied, 404: codes.NotFound,
	409: codes.Aborted, 429: codes.ResourceExhausted, 499: codes.Canceled, 500: codes.Internal, 501: codes.Unimplemented,
	503: codes.Unavailable, 504: codes.DeadlineExceeded,
}

// GoogleStatus is the error.status that goes with an HTTP status in a Google
// API error: 404 is "NOT_FOUND", 501 is "UNIMPLEMENTED".
func GoogleStatus(httpStatus int) string {
	switch httpStatus {
	case 409:
		return "ALREADY_EXISTS"
	case 412:
		return "FAILED_PRECONDITION"
	}
	if c, ok := httpGRPC[httpStatus]; ok {
		return rpccode.Code(c).String()
	}
	if httpStatus >= 500 {
		return "INTERNAL"
	}
	return "INVALID_ARGUMENT"
}

// WriteGoogleError writes the error body of a Google JSON API:
// {"error": {"code", "message", "status"}}.
func WriteGoogleError(w http.ResponseWriter, c *Ctx, code int, name, msg string) {
	c.ErrCode = name
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": code, "message": msg, "status": name}})
}

// GRPCError builds a gRPC status error.
func GRPCError(code codes.Code, format string, a ...any) error {
	return status.Errorf(code, format, a...)
}
