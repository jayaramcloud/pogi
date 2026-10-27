package lambda

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// target is a resolved function version ready to run.
type target struct {
	spec       *spec
	version    string
	dlq        string
	retries    int
	onSuccess  string
	onFailure  string
	functionID string // account/region/name
}

// target resolves a function reference (name or ARN, optional qualifier).
func (s *Service) target(c *core.Ctx, ref, qualifier string) (*target, error) {
	s.mu.Lock()
	f, q, err := s.function(c, ref)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	if q, err = qualify(q, qualifier); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	cfg, version, err := f.resolve(q)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	sp := &spec{name: f.Name, version: version, arn: f.arn(), account: f.Account, region: f.Region, runtime: cfg.Runtime,
		handler: cfg.Handler, packageType: cfg.PackageType, image: cfg.ImageURI, imageConfig: cfg.ImageConfig, code: cfg.Code, codeDir: cfg.CodeDir,
		codeSha: cfg.CodeSha256, env: cfg.Env, timeout: time.Duration(cfg.Timeout) * time.Second, memory: cfg.MemorySize,
		role: cfg.Role}
	var vc struct{ SubnetIds []string }
	json.Unmarshal(cfg.Vpc, &vc) // none or malformed: no VPC
	sp.vpcSubnets = vc.SubnetIds
	if q != "" && q != "$LATEST" {
		sp.arn += ":" + q
	}
	for _, l := range cfg.Layers {
		if z := s.layerZip(l); z != nil {
			sp.layers = append(sp.layers, z)
		} else {
			s.hub.Log.Warn("lambda layer not found locally, skipping", "layer", l)
		}
	}
	t := &target{spec: sp, version: version, dlq: cfg.DeadLetterArn, retries: 2, functionID: key(f.Account, f.Region, f.Name)}
	if ei := f.EventInvoke; ei != nil {
		if n, ok := ei["MaximumRetryAttempts"].(float64); ok {
			t.retries = int(n)
		}
		if d, ok := ei["DestinationConfig"].(map[string]any); ok {
			t.onSuccess = dest(d, "OnSuccess")
			t.onFailure = dest(d, "OnFailure")
		}
	}
	sp.key = t.functionID + ":" + version + "@" + cfg.RevisionID + "/" + cfg.CodeSha256
	s.mu.Unlock()
	if sp.codeDir != "" {
		sp.key += "/" + fingerprint(sp.codeDir)
	}
	return t, nil
}

func dest(cfg map[string]any, k string) string {
	m, _ := cfg[k].(map[string]any)
	d, _ := m["Destination"].(string)
	return d
}

// origin says which flow a run belongs to and who asked for it. A run that
// is not an HTTP request of its own (span) is a step of the flow.
type origin struct {
	trace, from string
	span        bool
}

func (s *Service) run(t *target, payload []byte, clientCtx string, o origin) (outcome, error) {
	start := time.Now()
	done, err := s.admit(t)
	if err != nil {
		return outcome{}, err
	}
	defer done()
	out, err := s.pool.run(t.spec, payload, clientCtx, o.trace)
	if o.span {
		failed := err
		if failed == nil && out.errType != "" {
			failed = errors.New(out.errType + ": " + functionMessage(out.body))
		}
		s.hub.Span(core.SpanInvoke, "lambda", "Invoke "+t.spec.name, o.from, t.spec.arn, o.trace, start, failed)
	}
	return out, err
}

// functionMessage is the errorMessage of a function error payload.
func functionMessage(body []byte) string {
	var e struct{ ErrorMessage string }
	json.Unmarshal(body, &e)
	return e.ErrorMessage
}

func (s *Service) invoke(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	payload := core.Body(r)
	if len(payload) > 6<<20 {
		return core.Errorf(413, "RequestTooLargeException", "Request must be smaller than 6291456 bytes for the InvokeFunction operation")
	}
	if len(payload) == 0 {
		payload = []byte("{}")
	}
	t, err := s.target(c, r.PathValue("name"), r.URL.Query().Get("Qualifier"))
	if err != nil {
		return err
	}
	switch r.Header.Get("X-Amz-Invocation-Type") {
	case "DryRun":
		w.WriteHeader(http.StatusNoContent)
		return nil
	case "Event":
		core.Go("lambda.runAsync", func() { s.runAsync(t, payload, origin{trace: c.Trace, span: true}) })
		w.Header().Set("x-amzn-RequestId", c.RequestID)
		w.WriteHeader(http.StatusAccepted)
		return nil
	}
	clientCtx := ""
	if cc := r.Header.Get("X-Amz-Client-Context"); cc != "" {
		if b, err := base64.StdEncoding.DecodeString(cc); err == nil {
			clientCtx = string(b)
		}
	}
	out, err := s.run(t, payload, clientCtx, origin{trace: c.Trace})
	if err != nil {
		return err
	}
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("X-Amz-Executed-Version", t.version)
	h.Set("x-amzn-RequestId", c.RequestID)
	if out.errType != "" {
		h.Set("X-Amz-Function-Error", out.errType)
		c.ErrCode = "FunctionError"
	}
	if r.Header.Get("X-Amz-Log-Type") == "Tail" {
		logs := out.logs
		if len(logs) > 4096 {
			logs = logs[len(logs)-4096:]
		}
		h.Set("X-Amz-Log-Result", base64.StdEncoding.EncodeToString([]byte(logs)))
	}
	w.WriteHeader(http.StatusOK)
	w.Write(out.body)
	return nil
}

func (s *Service) invokeAsyncLegacy(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	t, err := s.target(c, r.PathValue("name"), "")
	if err != nil {
		return err
	}
	body := core.Body(r)
	core.Go("lambda.runAsync", func() { s.runAsync(t, body, origin{trace: c.Trace, span: true}) })
	core.RESTJSON(w, c, 202, map[string]int{"Status": 202})
	return nil
}

// runAsync is the Event invocation path: retries, then destinations or the DLQ.
func (s *Service) runAsync(t *target, payload []byte, o origin) {
	var out outcome
	var err error
	attempts := 0
	for attempts <= t.retries {
		if attempts > 0 {
			time.Sleep(time.Duration(attempts) * time.Second) // ponytail: AWS waits ~1 and ~2 minutes
		}
		attempts++
		out, err = s.runQueued(t, payload, o, nil)
		if err == nil && out.errType == "" {
			s.sendResult(t, o, t.onSuccess, "Success", attempts, payload, out)
			return
		}
	}
	if err != nil {
		out = outcome{body: []byte(`{"errorMessage":` + strconv.Quote(err.Error()) + `}`), errType: "Unhandled"}
	}
	switch {
	case t.onFailure != "":
		s.sendResult(t, o, t.onFailure, "RetriesExhausted", attempts, payload, out)
	case t.dlq != "":
		msg := &core.Message{Body: string(payload), Trace: o.trace, From: t.spec.arn, Attributes: map[string]core.MessageAttribute{
			"RequestID":    {DataType: "String", StringValue: core.UUID()},
			"ErrorCode":    {DataType: "String", StringValue: "200"},
			"ErrorMessage": {DataType: "String", StringValue: string(out.body)},
		}}
		if err := s.hub.Deliver(t.dlq, msg); err != nil {
			s.hub.Log.Warn("lambda dead-letter delivery failed", "function", t.spec.name, "err", err)
		}
	default:
		s.hub.Log.Warn("async lambda invocation failed", "function", t.spec.name, "error", string(out.body))
	}
}

func asJSON(b []byte) any {
	var v any
	if json.Unmarshal(b, &v) != nil {
		return string(b)
	}
	return v
}

// sendResult delivers an async invocation record to a destination.
func (s *Service) sendResult(t *target, o origin, arn, condition string, attempts int, payload []byte, out outcome) {
	if arn == "" {
		return
	}
	resp := map[string]any{"statusCode": 200, "executedVersion": t.version}
	if out.errType != "" {
		resp["functionError"] = out.errType
	}
	rec, _ := json.Marshal(map[string]any{
		"version": "1.0", "timestamp": time.Now().UTC().Format(core.ISO),
		"requestContext": map[string]any{"requestId": core.UUID(), "functionArn": t.spec.arn + ":" + t.version,
			"condition": condition, "approximateInvokeCount": attempts},
		"requestPayload": asJSON(payload), "responseContext": resp, "responsePayload": asJSON(out.body),
	})
	msg := &core.Message{Body: string(rec), Source: "lambda", Trace: o.trace, From: t.spec.arn,
		DetailType: "Lambda Function Invocation Result - " + map[bool]string{true: "Success", false: "Failure"}[condition == "Success"],
		Resources:  []string{t.spec.arn}}
	if svc, _, _, _, _ := core.ParseARN(arn); svc != "events" {
		msg.Source, msg.DetailType, msg.Resources = "", "", nil
	}
	if err := s.hub.Deliver(arn, msg); err != nil {
		s.hub.Log.Warn("lambda destination delivery failed", "destination", arn, "err", err)
	}
}

// ctxFor builds a context for an ARN-addressed internal call.
func ctxFor(arn string) *core.Ctx {
	_, _, region, account := splitName(arn)
	if region == "" {
		region = core.DefaultRegion
	}
	if account == "" {
		account = core.DefaultAccount
	}
	return &core.Ctx{Account: account, Region: region}
}

// invokeSync runs a function for another service (API Gateway, Step Functions…).
func (s *Service) invokeSync(arn string, payload []byte) ([]byte, error) {
	return s.invokeFrom("", "", arn, payload)
}

// invokeFrom is invokeSync for a caller that is part of a flow.
func (s *Service) invokeFrom(trace, from, arn string, payload []byte) ([]byte, error) {
	t, err := s.target(ctxFor(arn), arn, "")
	if err != nil {
		return nil, err
	}
	out, err := s.run(t, payload, "", origin{trace: trace, from: from, span: trace != "" || from != ""})
	if err != nil {
		return nil, err
	}
	if out.errType != "" {
		return out.body, &core.FunctionError{Payload: out.body}
	}
	return out.body, nil
}

// deliver is the async event path used by SNS, S3, EventBridge and schedules.
func (s *Service) deliver(arn string, m *core.Message) error {
	t, err := s.target(ctxFor(arn), arn, "")
	if err != nil {
		return err
	}
	o := origin{trace: m.Trace, from: m.From, span: m.Trace != "" || m.From != ""}
	core.Go("lambda.runAsync", func() { s.runAsync(t, []byte(m.Body), o) })
	return nil
}

// ---- function URLs ----

// serveURL invokes a function through its URL with an HTTP API v2.0 event.
func (s *Service) serveURL(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	c.Op = "InvokeFunctionUrl"
	id, _ := core.SplitHost(r.Host, "")
	s.mu.Lock()
	var f *Function
	for _, fn := range s.st.Functions {
		if fn.URL != nil && fn.URL.ID == id {
			f = fn
		}
	}
	var cors map[string]any
	if f != nil && len(f.URL.Cors) > 0 {
		json.Unmarshal(f.URL.Cors, &cors)
	}
	s.mu.Unlock()
	if f == nil {
		writeURLError(w, c, http.StatusNotFound, "Not Found")
		return
	}
	if origin := r.Header.Get("Origin"); origin != "" && cors != nil {
		applyCORS(w.Header(), cors, origin)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
	}
	if err := c.Intercept(); err != nil {
		writeURLError(w, c, core.AsError(err).Status, core.AsError(err).Msg)
		return
	}
	t, err := s.target(&core.Ctx{Account: f.Account, Region: f.Region}, f.Name, "")
	if err != nil {
		writeURLError(w, c, http.StatusNotFound, "Not Found")
		return
	}
	out, err := s.run(t, HTTPEvent(r, id, "$default"), "", origin{trace: c.Trace})
	if errors.As(err, new(*throttleError)) {
		writeURLError(w, c, http.StatusTooManyRequests, "Rate Exceeded.")
		return
	}
	if err != nil || out.errType != "" {
		writeURLError(w, c, http.StatusBadGateway, "Internal Server Error")
		return
	}
	WriteHTTPResult(w, out.body)
}

func writeURLError(w http.ResponseWriter, c *core.Ctx, status int, msg string) {
	c.ErrCode = http.StatusText(status)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"Message": msg})
}

func applyCORS(h http.Header, cors map[string]any, origin string) {
	join := func(k string) string {
		return strings.Join(asList(cors[k]), ",")
	}
	allowed := false
	for _, o := range asList(cors["AllowOrigins"]) {
		allowed = allowed || o == "*" || o == origin
	}
	if !allowed {
		return
	}
	h.Set("Access-Control-Allow-Origin", origin)
	if v := join("AllowMethods"); v != "" {
		h.Set("Access-Control-Allow-Methods", v)
	}
	if v := join("AllowHeaders"); v != "" {
		h.Set("Access-Control-Allow-Headers", v)
	}
	if v := join("ExposeHeaders"); v != "" {
		h.Set("Access-Control-Expose-Headers", v)
	}
	if b, _ := cors["AllowCredentials"].(bool); b {
		h.Set("Access-Control-Allow-Credentials", "true")
	}
	if n, ok := cors["MaxAge"].(float64); ok {
		h.Set("Access-Control-Max-Age", strconv.Itoa(int(n)))
	}
}

func asList(v any) []string {
	var out []string
	if l, ok := v.([]any); ok {
		for _, x := range l {
			if s, ok := x.(string); ok {
				out = append(out, s)
			}
		}
	}
	return out
}

// HTTPEvent builds an API Gateway HTTP API / function URL payload (version 2.0).
func HTTPEvent(r *http.Request, apiID, routeKey string) []byte {
	headers := map[string]string{}
	var cookies []string
	for k, v := range r.Header {
		if strings.EqualFold(k, "Cookie") {
			for _, c := range v {
				for _, part := range strings.Split(c, ";") {
					cookies = append(cookies, strings.TrimSpace(part))
				}
			}
			continue
		}
		headers[strings.ToLower(k)] = strings.Join(v, ",")
	}
	query := map[string]string{}
	for k, v := range r.URL.Query() {
		query[k] = strings.Join(v, ",")
	}
	body := core.Body(r)
	ct := r.Header.Get("Content-Type")
	text := ct == "" || strings.HasPrefix(ct, "text/") || strings.Contains(ct, "json") || strings.Contains(ct, "xml") || strings.Contains(ct, "x-www-form-urlencoded")
	now := time.Now().UTC()
	ev := map[string]any{
		"version": "2.0", "routeKey": routeKey, "rawPath": r.URL.Path, "rawQueryString": r.URL.RawQuery, "headers": headers,
		"requestContext": map[string]any{
			"accountId": "anonymous", "apiId": apiID, "domainName": r.Host, "domainPrefix": apiID, "requestId": core.UUID(),
			"routeKey": routeKey, "stage": "$default", "time": now.Format("02/Jan/2006:15:04:05 -0700"), "timeEpoch": now.UnixMilli(),
			"http": map[string]string{"method": r.Method, "path": r.URL.Path, "protocol": r.Proto, "sourceIp": strings.Split(r.RemoteAddr, ":")[0], "userAgent": r.UserAgent()},
		},
		"isBase64Encoded": !text,
	}
	if len(cookies) > 0 {
		ev["cookies"] = cookies
	}
	if len(query) > 0 {
		ev["queryStringParameters"] = query
	}
	if len(body) > 0 {
		if text {
			ev["body"] = string(body)
		} else {
			ev["body"] = base64.StdEncoding.EncodeToString(body)
		}
	}
	b, _ := json.Marshal(ev)
	return b
}

// WriteHTTPResult maps a function result to an HTTP response (payload 2.0 rules):
// an object with statusCode is a full response, anything else is a JSON body.
func WriteHTTPResult(w http.ResponseWriter, result []byte) {
	var res struct {
		StatusCode      int               `json:"statusCode"`
		Headers         map[string]string `json:"headers"`
		Cookies         []string          `json:"cookies"`
		Body            *string           `json:"body"`
		IsBase64Encoded bool              `json:"isBase64Encoded"`
	}
	if json.Unmarshal(result, &res) != nil || res.StatusCode == 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(result)
		return
	}
	for k, v := range res.Headers {
		w.Header().Set(k, v)
	}
	for _, c := range res.Cookies {
		w.Header().Add("Set-Cookie", c)
	}
	var body []byte
	if res.Body != nil {
		body = []byte(*res.Body)
		if res.IsBase64Encoded {
			if b, err := base64.StdEncoding.DecodeString(*res.Body); err == nil {
				body = b
			}
		}
	}
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "application/json")
	}
	w.WriteHeader(res.StatusCode)
	w.Write(body)
}

// ---- console ----

// InvokeResult is what the console shows after a test invoke.
type InvokeResult struct {
	Payload string
	Error   string
	Logs    string
	Elapsed time.Duration
}

// Invoke runs a function synchronously from the console.
func (s *Service) Invoke(account, region, name string, payload []byte) (InvokeResult, error) {
	t, err := s.target(&core.Ctx{Account: account, Region: region}, name, "")
	if err != nil {
		return InvokeResult{}, err
	}
	start := time.Now()
	out, err := s.run(t, payload, "", origin{})
	if err != nil {
		var e *core.Error
		if errors.As(err, &e) {
			return InvokeResult{Error: e.Code + ": " + e.Msg, Elapsed: time.Since(start)}, nil
		}
		return InvokeResult{}, err
	}
	return InvokeResult{Payload: string(out.body), Error: out.errType, Logs: out.logs, Elapsed: time.Since(start)}, nil
}
