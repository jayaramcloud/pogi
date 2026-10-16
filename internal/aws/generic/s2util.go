package generic

import (
	"maps"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Helpers of the operations and storage services (config, rum, amp, oam, transfer, datasync, s3tables).

// s2After overrides an operation with the engine's own result, changed by fn.
func s2After(service, op string, fn func(c *Call, out map[string]any) error) {
	Override(service, op, func(c *Call) (map[string]any, error) {
		out, err := c.Default()
		if err != nil {
			return nil, err
		}
		return out, fn(c, out)
	})
}

// s2Set stores members on the resource a call just made or changed, and in its output where the output has them.
func s2Set(c *Call, out map[string]any, kv map[string]any) {
	r := c.Res
	if r.Data == nil {
		r.Data = map[string]any{}
	}
	for k, v := range kv {
		r.Data[k] = v
		if out != nil && c.op.out != nil && c.op.out.Members[k] != nil {
			out[k] = v
		}
	}
	c.Put(r)
}

// s2Store makes or replaces a resource that the engine would not infer. A resource
// that exists keeps its creation time and tags, and the members of its data that begin with "_".
// idName is the identifier member of the resource's type when the engine should be able to find it (TaskArn).
func s2Store(c *Call, typ, key, name, arn string, data map[string]any, idName ...string) *Resource {
	now := time.Now().UTC()
	id := typ
	if len(idName) > 0 {
		id = idName[0]
	}
	r := &Resource{Type: typ, Key: key, IDs: map[string]string{id: key}, Name: name, ARN: arn, Account: c.Account,
		Region: c.key(), Data: data, Created: now, Updated: now}
	if old := c.Get(typ, key); old != nil {
		r.Created, r.Tags = old.Created, old.Tags
		for k, v := range old.Data {
			if k[0] == '_' {
				if r.Data == nil {
					r.Data = map[string]any{}
				}
				r.Data[k] = v
			}
		}
		if arn == "" {
			r.ARN = old.ARN
		}
	}
	c.Put(r)
	return r
}

// s2Tags adds the tags of a create call's Tags member to a resource.
func s2Tags(c *Call, r *Resource) {
	if _, add, _ := takeTags(c.op.in, c.In); len(add) > 0 {
		if r.Tags == nil {
			r.Tags = map[string]string{}
		}
		maps.Copy(r.Tags, add)
		c.Put(r)
	}
}

// s2Err is an error with an AWS code, for the exceptions a model declares but the engine cannot tell apart.
func s2Err(code, format string, a ...any) error { return core.Errorf(400, code, format, a...) }

// s2Time is a timestamp as the engine stores them.
func s2Time(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

// s2Names reads a list of strings from a request member.
func s2Names(v any) []string {
	var out []string
	for _, e := range asList(v) {
		out = append(out, str(e))
	}
	return out
}

// s2Page cuts a list the way the engine's lists do: a limit and an opaque offset token.
func s2Page(items []any, limit any, token string) (page []any, next string) {
	off, _ := strconv.Atoi(token)
	off = min(max(off, 0), len(items))
	page = items[off:]
	if n, _ := strconv.Atoi(str(limit)); n > 0 && n < len(page) {
		page, next = page[:n], strconv.Itoa(off+n)
	}
	return page, next
}

// s2Map is a nested structure of a request, or an empty one.
func s2Map(v any) map[string]any {
	m, _ := v.(map[string]any)
	if m == nil {
		m = map[string]any{}
	}
	return m
}

// s2Clone copies a structure, so that a stored one is not changed through an output.
func s2Clone(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		if sub, ok := v.(map[string]any); ok {
			v = s2Clone(sub)
		}
		out[k] = v
	}
	return out
}

// Raw lets a service answer requests that are not operations of its model (the Prometheus API of Managed
// Prometheus): fn reports whether it took the request. It runs before the request is matched to an operation.
func Raw(service string, fn func(s *Service, w http.ResponseWriter, r *http.Request) bool) {
	s2Reg.Lock()
	defer s2Reg.Unlock()
	if s2Reg.raw == nil {
		s2Reg.raw = map[string]func(*Service, http.ResponseWriter, *http.Request) bool{}
	}
	s2Reg.raw[service] = fn
}

// OnClose runs fn when the hub shuts a service down (it stops a listener, forgets a store).
func OnClose(service string, fn func(s *Service)) {
	s2Reg.Lock()
	defer s2Reg.Unlock()
	if s2Reg.closers == nil {
		s2Reg.closers = map[string][]func(*Service){}
	}
	s2Reg.closers[service] = append(s2Reg.closers[service], fn)
}

var s2Reg struct {
	sync.RWMutex
	raw     map[string]func(*Service, http.ResponseWriter, *http.Request) bool
	closers map[string][]func(*Service)
}

func rawFor(service string) func(*Service, http.ResponseWriter, *http.Request) bool {
	s2Reg.RLock()
	defer s2Reg.RUnlock()
	return s2Reg.raw[service]
}

// runClosers runs the OnClose hooks; Close calls it on shutdown.
func (s *Service) runClosers() {
	s2Reg.RLock()
	fns := s2Reg.closers[s.meta.Name]
	s2Reg.RUnlock()
	for _, fn := range fns {
		fn(s)
	}
}
