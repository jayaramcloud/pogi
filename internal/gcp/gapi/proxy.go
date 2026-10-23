package gapi

import (
	"context"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// ProxyDesc returns a copy of a generated service description whose methods
// forward every call to another gRPC server: a vendor's emulator that runs
// behind pogi. Register it with a nil implementation. conn is called on each
// call and may start the backend; its error is the call's error. check, if not
// nil, sees each request first and may refuse it.
// Methods that stream from the client are left out (the server answers
// Unimplemented): the backends here (Spanner, Bigtable's emulator) have none.
func ProxyDesc(sd *grpc.ServiceDesc, conn func(context.Context) (*grpc.ClientConn, error), check func(proto.Message) error) *grpc.ServiceDesc {
	d, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(sd.ServiceName))
	if err != nil {
		panic(err) // the generated package that gave sd registered it
	}
	svc := d.(protoreflect.ServiceDescriptor)
	newMsg := func(m protoreflect.MessageDescriptor) proto.Message {
		t, err := protoregistry.GlobalTypes.FindMessageByName(m.FullName())
		if err != nil {
			panic(err)
		}
		return t.New().Interface()
	}
	out := &grpc.ServiceDesc{ServiceName: sd.ServiceName, HandlerType: (*any)(nil), Metadata: sd.Metadata}
	for _, m := range sd.Methods {
		md := svc.Methods().ByName(protoreflect.Name(m.MethodName))
		full := "/" + sd.ServiceName + "/" + m.MethodName
		out.Methods = append(out.Methods, grpc.MethodDesc{MethodName: m.MethodName,
			Handler: func(srv any, ctx context.Context, dec func(any) error, ic grpc.UnaryServerInterceptor) (any, error) {
				in := newMsg(md.Input())
				if err := dec(in); err != nil {
					return nil, err
				}
				if err := verify(check, in); err != nil {
					return nil, err
				}
				call := func(ctx context.Context, req any) (any, error) {
					cc, err := conn(ctx)
					if err != nil {
						return nil, err
					}
					resp := newMsg(md.Output())
					return resp, cc.Invoke(forward(ctx), full, req, resp)
				}
				if ic == nil {
					return call(ctx, in)
				}
				return ic(ctx, in, &grpc.UnaryServerInfo{Server: srv, FullMethod: full}, call)
			}})
	}
	for _, s := range sd.Streams {
		md := svc.Methods().ByName(protoreflect.Name(s.StreamName))
		full := "/" + sd.ServiceName + "/" + s.StreamName
		if s.ClientStreams {
			continue
		}
		out.Streams = append(out.Streams, grpc.StreamDesc{StreamName: s.StreamName, ServerStreams: true,
			Handler: func(srv any, ss grpc.ServerStream) error {
				cc, err := conn(ss.Context())
				if err != nil {
					return err
				}
				ctx, cancel := context.WithCancel(forward(ss.Context()))
				defer cancel()
				cs, err := cc.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, full)
				if err != nil {
					return err
				}
				in := newMsg(md.Input())
				if err := ss.RecvMsg(in); err != nil {
					return err
				}
				if err := verify(check, in); err != nil {
					return err
				}
				if err := cs.SendMsg(in); err != nil {
					return err
				}
				if err := cs.CloseSend(); err != nil {
					return err
				}
				for {
					resp := newMsg(md.Output())
					if err := cs.RecvMsg(resp); err != nil {
						if err == io.EOF {
							return nil
						}
						return err
					}
					if err := ss.SendMsg(resp); err != nil {
						return err
					}
				}
			}})
	}
	return out
}

func verify(check func(proto.Message) error, in proto.Message) error {
	if check == nil {
		return nil
	}
	return check(in)
}

// forward passes the client's request headers on to the backend.
func forward(ctx context.Context) context.Context {
	md, _ := metadata.FromIncomingContext(ctx)
	return metadata.NewOutgoingContext(ctx, md.Copy())
}
