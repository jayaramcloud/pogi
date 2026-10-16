package generic

import (
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// lifecycleDelay is how long a resource stays in each transitional state
// (CREATING, STARTING ...) before it moves to the next. State is worked out
// when a resource is read, so nothing runs in the background and a create
// never blocks.
var lifecycleDelay atomic.Int64

func init() { lifecycleDelay.Store(int64(1500 * time.Millisecond)) }

// SetLifecycleDelay is the test hook: 0 settles every resource at once, a large
// value freezes it in its first state. It returns the previous value.
func SetLifecycleDelay(d time.Duration) time.Duration {
	return time.Duration(lifecycleDelay.Swap(int64(d)))
}

var derivers struct {
	sync.RWMutex
	m map[string]func(*Resource) map[string]any
}

// Derive registers fn, which computes members of a service's resources of one
// type each time one is read (describe, list, create output). The members it
// returns, keyed by the model's names, win over the stored ones. See Step.
func Derive(service, typ string, fn func(r *Resource) map[string]any) {
	derivers.Lock()
	defer derivers.Unlock()
	if derivers.m == nil {
		derivers.m = map[string]func(*Resource) map[string]any{}
	}
	derivers.m[service+"/"+typ] = fn
}

func derived(service string, r *Resource) map[string]any {
	derivers.RLock()
	fn := derivers.m[service+"/"+r.Type]
	derivers.RUnlock()
	if fn == nil {
		return nil
	}
	return fn(r)
}

// Begin starts (or restarts) the timed sequence key of a resource now. Call it
// on a resource you then Put.
func Begin(r *Resource, key string) {
	if r.Data == nil {
		r.Data = map[string]any{}
	}
	r.Data["_t_"+key] = time.Now().UTC().Format(time.RFC3339Nano)
}

// Step is where the timed sequence key of a resource is: states[0] when it
// began, then one state further per LifecycleDelay, and the last one for good.
// A sequence that was never begun started when the resource was created.
func Step(r *Resource, key string, states ...string) string {
	start := r.Created
	if s, _ := r.Data["_t_"+key].(string); s != "" {
		if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
			start = t
		}
	}
	i := len(states) - 1
	if d := time.Duration(lifecycleDelay.Load()); d > 0 {
		i = min(i, int(time.Since(start)/d))
	}
	return states[i]
}

// byARN is a copy of the resource with an ARN, or nil.
func (s *Service) byARN(account, arn string) *Resource {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r := s.byARNLocked(account, arn); r != nil {
		return r.clone()
	}
	return nil
}

// pageOf cuts a list by the request's limit and token members; the token is an
// offset. A token that is not one is refused, as AWS refuses a made-up marker.
func pageOf(c *Call, all []any, tokenKey, limitKey string) ([]any, string, error) {
	off := 0
	if tok := str(c.In[tokenKey]); tok != "" {
		var err error
		if off, err = strconv.Atoi(tok); err != nil || off < 0 {
			return nil, "", c.Invalid("Invalid %s: %s", tokenKey, tok)
		}
	}
	off = min(off, len(all))
	limit, _ := strconv.Atoi(str(c.In[limitKey]))
	if limit <= 0 || off+limit >= len(all) {
		return append([]any{}, all[off:]...), "", nil
	}
	return all[off : off+limit], strconv.Itoa(off + limit), nil
}
