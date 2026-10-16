package gapi

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Client is the HTTP client for calls a service makes to user targets (Cloud
// Tasks, Scheduler, Eventarc). A *.localhost host (<svc>.run.localhost) goes
// to pogi's own port with its Host header kept, so no DNS is involved. It
// follows no redirects: a 3xx is a failed delivery, as at Google.
func Client(h *core.Hub) *http.Client {
	d := &net.Dialer{Timeout: 10 * time.Second}
	return &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		Transport: &http.Transport{DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if host, _, _ := net.SplitHostPort(addr); strings.HasSuffix(host, ".localhost") {
				if u, err := url.Parse(h.URL); err == nil && u.Host != "" {
					addr = u.Host
				}
			}
			return d.DialContext(ctx, network, addr)
		}},
	}
}

// Status is the google.rpc.Status of a delivery: OK for 2xx, otherwise the
// gRPC code Google maps the HTTP status to (httpCode 0 means no response, err says why).
func Status(httpCode int, err error) *spb.Status {
	if httpCode/100 == 2 {
		return &spb.Status{}
	}
	code := map[int]codes.Code{400: codes.InvalidArgument, 401: codes.Unauthenticated, 403: codes.PermissionDenied,
		404: codes.NotFound, 409: codes.Aborted, 429: codes.ResourceExhausted, 501: codes.Unimplemented,
		503: codes.Unavailable, 504: codes.DeadlineExceeded}[httpCode]
	msg := "HTTP status code " + strconv.Itoa(httpCode)
	if httpCode == 0 {
		code, msg = codes.Unavailable, "no response: "+err.Error()
	} else if code == codes.OK {
		code = codes.Internal
		if httpCode < 500 {
			code = codes.Unknown
		}
	}
	return &spb.Status{Code: int32(code), Message: msg}
}

// ---- lists and update masks ----

// MaxPageSize is the largest page Page returns; Google caps a bigger page size
// instead of refusing it. (A negative size is refused earlier, in invoke.)
const MaxPageSize = 1000

// PageSizeOK says whether v can be a maxResults query value: empty, or a whole
// number from 0 to 2^32-1. A bigger size is capped, a negative or non-numeric
// one is refused with a 400.
func PageSizeOK(v string) bool {
	_, err := strconv.ParseUint(v, 10, 32)
	return v == "" || err == nil
}

// Page cuts one page from a sorted list; the token is the offset of the next page.
func Page[T any](items []T, size int32, token string) ([]T, string) {
	start, _ := strconv.Atoi(token)
	start = min(max(start, 0), len(items))
	end := len(items)
	if size > 0 {
		end = min(end, start+int(min(size, MaxPageSize)))
	}
	if end < len(items) {
		return items[start:end], strconv.Itoa(end)
	}
	return items[start:end], ""
}

// Update copies the fields an update mask names from src to dst. An empty
// mask means the allowed fields that src sets, and "*" means all allowed fields.
func Update(dst, src proto.Message, paths []string, allowed ...string) error {
	src = proto.Clone(src)
	switch {
	case len(paths) == 0:
		for _, n := range allowed {
			if fd := src.ProtoReflect().Descriptor().Fields().ByName(protoreflect.Name(n)); fd != nil && src.ProtoReflect().Has(fd) {
				paths = append(paths, n)
			}
		}
	case len(paths) == 1 && paths[0] == "*":
		paths = allowed
	}
	for _, path := range paths {
		parts := strings.Split(path, ".")
		if !slices.Contains(allowed, parts[0]) {
			return Invalid("Invalid update_mask provided in the request: %q", path)
		}
		dm, sm := dst.ProtoReflect(), src.ProtoReflect()
		for i, name := range parts {
			fd := dm.Descriptor().Fields().ByName(protoreflect.Name(name))
			last := i == len(parts)-1
			switch {
			case fd == nil || !last && (fd.Message() == nil || fd.IsList() || fd.IsMap()):
				return Invalid("Invalid update_mask provided in the request: %q", path)
			case !last:
				dm, sm = dm.Mutable(fd).Message(), sm.Get(fd).Message()
			case sm.Has(fd):
				dm.Set(fd, sm.Get(fd))
			default:
				dm.Clear(fd)
			}
		}
	}
	return nil
}
