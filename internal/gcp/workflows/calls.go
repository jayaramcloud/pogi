package workflows

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

const (
	httpTimeout = 300 * time.Second // the default when a call gives no timeout
	maxBody     = 10 << 20          // largest response body read
)

// callStep runs a `call` step: a standard library call, an HTTP call or a subworkflow.
func (s *scope) callStep(b map[string]any) error {
	name, _ := b["call"].(string)
	var args map[string]any
	if has(b, "args") {
		v, err := s.evalValue(b["args"])
		if err != nil {
			return err
		}
		var ok bool
		if args, ok = v.(map[string]any); !ok {
			return newErr("args must be a map", "ValueError")
		}
	}
	var out any
	var err error
	switch {
	case name == "sys.log":
		// ponytail: the log text is checked and dropped; write it to Cloud Logging if users ask to see it.
	case name == "sys.sleep":
		f, ok := num(args["seconds"])
		if !ok || f < 0 {
			return newErr("sys.sleep wants seconds (a number, 0 or more)", "ValueError")
		}
		err = sleep(s.env.ctx, time.Duration(f*float64(time.Second)))
	case name == "sys.get_env":
		n, _ := args["name"].(string)
		out, err = s.env.getEnv(n)
	case name == "http.request":
		m, _ := args["method"].(string)
		out, err = s.env.http(strings.ToUpper(m), args)
	case strings.HasPrefix(name, "http."):
		m := strings.ToUpper(strings.TrimPrefix(name, "http."))
		if m != "GET" && m != "POST" && m != "PUT" && m != "PATCH" && m != "DELETE" {
			return unsupported("the call %s", name)
		}
		out, err = s.env.http(m, args)
	case s.env.prog[name] != nil:
		out, err = s.env.callRoutine(name, s.env.prog[name], args, s.depth+1)
	case strings.HasPrefix(name, "googleapis."), strings.HasPrefix(name, "events."), strings.HasPrefix(name, "experimental."):
		return unsupported("the connector call %s", name)
	case stdlib[name] != nil:
		return unsupported("the call %s as a step (use it inside a ${...} expression)", name)
	default:
		return unsupported("the call %q: there is no such subworkflow or function", name)
	}
	if err != nil {
		return err
	}
	if r, ok := b["result"].(string); ok {
		return s.set(r, copyOf(out))
	}
	return nil
}

// http makes one HTTP call. A *.googleapis.com URL goes to pogi itself, so a workflow can call pogi's
// own services; any other URL is called for real. A status outside 2xx raises an HttpError.
func (e *runEnv) http(method string, a map[string]any) (any, error) {
	raw, _ := a["url"].(string)
	u, err := url.Parse(raw)
	if err != nil || raw == "" || u.Host == "" || u.Scheme != "http" && u.Scheme != "https" {
		return nil, newErr(fmt.Sprintf("the url %q is not an absolute http or https URL", raw), "ValueError")
	}
	if q, ok := a["query"].(map[string]any); ok {
		vals := u.Query()
		for _, k := range sortedKeys(q) {
			if l, ok := q[k].([]any); ok {
				for _, x := range l {
					vals.Add(k, fmtValue(x))
				}
			} else {
				vals.Set(k, fmtValue(q[k]))
			}
		}
		u.RawQuery = vals.Encode()
	}
	var body io.Reader
	ctype := ""
	switch x := a["body"].(type) {
	case nil:
	case string:
		body = strings.NewReader(x)
	default:
		b, err := json.Marshal(x)
		if err != nil {
			return nil, newErr("the body cannot be encoded as JSON: "+err.Error(), "ValueError")
		}
		body, ctype = bytes.NewReader(b), "application/json"
	}
	timeout := httpTimeout
	if f, ok := num(a["timeout"]); ok && f > 0 {
		timeout = time.Duration(f * float64(time.Second))
	}
	ctx, cancel := context.WithTimeout(e.ctx, timeout)
	defer cancel()
	host := ""
	if strings.HasSuffix(strings.ToLower(u.Hostname()), ".googleapis.com") {
		host = u.Host
		self, err := url.Parse(e.svc.hub.URL)
		if err != nil || self.Host == "" {
			return nil, newErr("pogi does not know its own address", "ConnectionError")
		}
		u.Scheme, u.Host = self.Scheme, self.Host
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, newErr(err.Error(), "ValueError")
	}
	req.Host = host
	if ctype != "" {
		req.Header.Set("Content-Type", ctype)
	}
	if h, ok := a["headers"].(map[string]any); ok {
		for k, v := range h {
			req.Header.Set(k, fmtValue(v))
		}
	}
	resp, err := e.svc.client.Do(req)
	if err != nil {
		if e.ctx.Err() != nil {
			return nil, e.ctx.Err()
		}
		var ne net.Error
		if errors.Is(err, context.DeadlineExceeded) || errors.As(err, &ne) && ne.Timeout() {
			return nil, newErr(fmt.Sprintf("the call to %s took longer than %v", raw, timeout), "TimeoutError")
		}
		return nil, newErr("connection error: "+err.Error(), "ConnectionError")
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return nil, newErr("reading the response: "+err.Error(), "ConnectionError")
	}
	var parsed any = string(data)
	if strings.Contains(resp.Header.Get("Content-Type"), "json") {
		if len(bytes.TrimSpace(data)) == 0 {
			parsed = nil
		} else if v, err := decodeJSON(data); err == nil {
			parsed = v
		}
	}
	headers := map[string]any{}
	for k := range resp.Header {
		headers[strings.ToLower(k)] = resp.Header.Get(k)
	}
	if resp.StatusCode/100 != 2 {
		return nil, &wfErr{val: map[string]any{
			"code": int64(resp.StatusCode), "headers": headers, "body": parsed,
			"message": fmt.Sprintf("HTTP server responded with error code %d", resp.StatusCode), "tags": []any{"HttpError"},
		}}
	}
	return map[string]any{"code": int64(resp.StatusCode), "headers": headers, "body": parsed}, nil
}

func sortedKeys(m map[string]any) []string {
	ks := keysOf(m)
	sort.Strings(ks)
	return ks
}
