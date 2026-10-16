package lambda

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// accountLimit is an account's concurrency in a region. Reserved concurrency
// is carved out of it; functions without a reservation share the rest, of
// which PutFunctionConcurrency must leave minUnreserved.
const accountLimit, minUnreserved = 1000, 100

var errRateExceeded = core.Errorf(429, "TooManyRequestsException", "Rate Exceeded.")

// throttleError is a TooManyRequestsException; reason is the ThrottleReason
// SDKs read from the body. It unwraps to a plain 429 for other services.
type throttleError struct{ reason string }

func (e *throttleError) Error() string { return errRateExceeded.Error() }
func (e *throttleError) Unwrap() error { return errRateExceeded }

// write sends the throttle as Lambda does: core.RESTJSONError's shape plus Reason.
func (e *throttleError) write(w http.ResponseWriter, c *core.Ctx) {
	c.ErrCode = errRateExceeded.Code
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("X-Amzn-ErrorType", errRateExceeded.Code)
	h.Set("x-amzn-RequestId", c.RequestID)
	w.WriteHeader(http.StatusTooManyRequests)
	json.NewEncoder(w).Encode(map[string]string{"Type": "User", "message": errRateExceeded.Msg, "Message": errRateExceeded.Msg,
		"__type": errRateExceeded.Code, "Reason": e.reason})
}

// unreserved is the concurrency an account shares in a region; the lock must be held.
// ponytail: sums every function per invoke; keep a running total if accounts get huge.
func (s *Service) unreserved(account, region string) int {
	n := accountLimit
	for _, f := range s.st.Functions {
		if f.Reserved != nil && f.Account == account && f.Region == region {
			n -= *f.Reserved
		}
	}
	return n
}

// admit counts an invocation in, or throttles it when the function's
// reserved concurrency or its account's unreserved pool is used up; done
// counts it out.
func (s *Service) admit(t *target) (done func(), err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.st.Functions[t.functionID]
	if f == nil { // deleted while the event waited
		return nil, errNoFunction(t.spec.arn)
	}
	keys := []string{t.functionID}
	if f.Reserved != nil {
		if s.busy[t.functionID] >= *f.Reserved {
			return nil, &throttleError{"ReservedFunctionConcurrentInvocationLimitExceeded"}
		}
	} else {
		shared := f.Account + "/" + f.Region
		if s.busy[shared] >= s.unreserved(f.Account, f.Region) {
			return nil, &throttleError{"ConcurrentInvocationLimitExceeded"}
		}
		keys = append(keys, shared)
	}
	for _, k := range keys {
		s.busy[k]++
	}
	return func() {
		s.mu.Lock()
		for _, k := range keys {
			s.busy[k]--
		}
		s.mu.Unlock()
	}, nil
}

// runQueued runs an event that waits in a queue (async invocations, event
// source mappings): when throttled it backs off and tries again, while live
// (nil: always) still wants it, instead of failing.
// ponytail: AWS drops a throttled async event after 6 hours; this retries until pogi stops.
func (s *Service) runQueued(t *target, payload []byte, o origin, live func() bool) (outcome, error) {
	for wait := 100 * time.Millisecond; ; wait = min(2*wait, 5*time.Second) {
		out, err := s.run(t, payload, "", o)
		if !errors.As(err, new(*throttleError)) || live != nil && !live() {
			return out, err
		}
		select {
		case <-s.pool.stop:
			return out, err
		case <-time.After(wait):
		}
	}
}

// ---- provisioned concurrency ----

// Provisioned is the provisioned concurrency of a version or alias.
type Provisioned struct {
	Requested int       `json:"requested"`
	Status    string    `json:"status"` // IN_PROGRESS, READY or FAILED
	Reason    string    `json:"reason,omitempty"`
	Modified  time.Time `json:"modified"`
}

func (pc *Provisioned) json() map[string]any {
	ready := 0
	if pc.Status == "READY" {
		ready = pc.Requested
	}
	out := map[string]any{"RequestedProvisionedConcurrentExecutions": pc.Requested, "AllocatedProvisionedConcurrentExecutions": ready,
		"AvailableProvisionedConcurrentExecutions": ready, "Status": pc.Status, "LastModified": pc.Modified.Format("2006-01-02T15:04:05.000-0700")}
	if pc.Reason != "" {
		out["StatusReason"] = pc.Reason
	}
	return out
}

func (s *Service) putProvisioned(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	var in struct{ ProvisionedConcurrentExecutions int }
	if err := decode(r, &in); err != nil {
		return err
	}
	q, n := r.URL.Query().Get("Qualifier"), in.ProvisionedConcurrentExecutions
	err := s.withFunction(w, r, c, 202, func(f *Function) (any, error) {
		switch {
		case q == "" || q == "$LATEST":
			return nil, invalid("Provisioned Concurrency Configs cannot be applied to unpublished function versions.")
		case f.Versions[q] == nil && f.Aliases[q] == nil:
			return nil, errNoFunction(f.arn() + ":" + q)
		case n < 1:
			return nil, invalid("1 validation error detected: Value '%d' at 'provisionedConcurrentExecutions' failed to satisfy constraint: Member must have value greater than or equal to 1", n)
		case f.Reserved != nil && n > *f.Reserved:
			return nil, invalid("Requested Provisioned Concurrency should not be greater than the reservedConcurrentExecution for function")
		}
		if f.Provisioned == nil {
			f.Provisioned = map[string]*Provisioned{}
		}
		pc := &Provisioned{Requested: n, Status: "IN_PROGRESS", Modified: time.Now().UTC()}
		f.Provisioned[q] = pc
		return pc.json(), nil
	})
	if err == nil {
		core.Go("lambda.provision", func() { s.provision() })
	}
	return err
}

func (s *Service) getProvisioned(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	return s.withFunction(w, r, c, 200, func(f *Function) (any, error) {
		if r.URL.Query().Get("List") == "ALL" { // ListProvisionedConcurrencyConfigs
			list := []any{}
			for _, q := range core.SortedKeys(f.Provisioned) {
				item := f.Provisioned[q].json()
				item["FunctionArn"] = f.arn() + ":" + q
				list = append(list, item)
			}
			return map[string]any{"ProvisionedConcurrencyConfigs": list}, nil
		}
		pc := f.Provisioned[r.URL.Query().Get("Qualifier")]
		if pc == nil {
			return nil, core.Errorf(404, "ProvisionedConcurrencyConfigNotFoundException", "No Provisioned Concurrency Config found for this function")
		}
		return pc.json(), nil
	})
}

func (s *Service) deleteProvisioned(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	return s.withFunction(w, r, c, http.StatusNoContent, func(f *Function) (any, error) {
		delete(f.Provisioned, r.URL.Query().Get("Qualifier"))
		return nil, nil
	})
}

// provision keeps the environments provisioned concurrency asks for
// started (the pool keeps them warm) and moves configs from IN_PROGRESS to
// READY, or FAILED when they cannot start.
func (s *Service) provision() {
	type want struct {
		c       *core.Ctx
		name, v string
		n       int
		pc      *Provisioned
		t       *target
	}
	var wants []*want
	s.mu.Lock()
	for _, f := range s.st.Functions {
		for q, pc := range f.Provisioned {
			if pc.Status == "FAILED" {
				continue
			}
			v := q
			if a := f.Aliases[q]; a != nil {
				v = a.Version // ponytail: an alias's routing weights get no provisioned environments
			}
			wants = append(wants, &want{c: &core.Ctx{Account: f.Account, Region: f.Region}, name: f.Name, v: v, n: pc.Requested, pc: pc})
		}
	}
	s.mu.Unlock()
	floor := map[string]int{}
	for _, w := range wants {
		if t, err := s.target(w.c, w.name, w.v); err == nil {
			w.t = t
			floor[t.spec.key] += w.n
		}
	}
	s.pool.setFloor(floor)
	for _, w := range wants {
		if w.t == nil {
			continue
		}
		err := s.pool.fill(w.t.spec, floor[w.t.spec.key])
		s.mu.Lock()
		if err != nil {
			w.pc.Status, w.pc.Reason = "FAILED", core.AsError(err).Msg
		} else if w.pc.Status == "IN_PROGRESS" {
			w.pc.Status = "READY"
		}
		s.mu.Unlock()
	}
}

// Tick keeps provisioned environments started, then stops idle environments
// unused for keepAlive. New runs it every 5 seconds; tests call it with any time.
func (s *Service) Tick(now time.Time) {
	s.provision()
	s.pool.expire(now)
}
