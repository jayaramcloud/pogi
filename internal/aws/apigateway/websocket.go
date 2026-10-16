package apigateway

import (
	"bytes"
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/coder/websocket"
	"github.com/nrndev/pogi/internal/core"
)

// idleTimeout closes a connection without traffic, as API Gateway does.
const idleTimeout = 10 * time.Minute

// wsConn is an open connection to a WebSocket API.
type wsConn struct {
	*websocket.Conn
	id, api, account, region, stage string
	vars                            map[string]string // stage variables as of $connect
	auth                            map[string]any    // the $connect authorizer's context
	r                               *http.Request     // the upgrade request
	c                               *core.Ctx
	at, last                        time.Time // connected, last message either way (Service.mu)
	idle                            *time.Timer
	code                            int // close status and reason, once closed
	reason                          string
}

// call is a new execute-api call on the connection.
func (wc *wsConn) call() *call {
	return &call{r: wc.r, c: wc.c, id: core.UUID(), apiID: wc.api, account: wc.account, region: wc.region,
		stage: wc.stage, vars: wc.vars, auth: wc.auth, conn: wc}
}

// resolveWS picks the stage of a WebSocket connection and its $connect route; lock held.
func (a *httpAPI) resolveWS(x *call) error {
	x.stage, _, _ = strings.Cut(strings.TrimPrefix(x.full, "/"), "/")
	st := a.Stages[x.stage]
	if st == nil {
		return errForbidden
	}
	x.account, x.region, x.vars = a.Account, a.Region, maps.Clone(st.StageVariables)
	return a.wsRoute(x, "$connect")
}

// wsRoute fills x with the route of key, if the API has one, its
// integration with that integration's responses, and its authorizer; lock held.
func (a *httpAPI) wsRoute(x *call, key string) error {
	x.routeKey, x.route = key, nil
	for _, rt := range a.Routes {
		if rt.RouteKey == key {
			x.route = clone(rt)
		}
	}
	if x.route == nil {
		return nil
	}
	id := strings.TrimPrefix(x.route.Target, "integrations/")
	irs := a.IntegrationResponses[id]
	x.integ, x.irs, x.twoWay = clone(a.Integrations[id]), *clone(&irs), len(a.RouteResponses[x.route.RouteID]) > 0
	if x.route.AuthorizationType == "CUSTOM" {
		if x.hauthz = clone(a.Authorizers[x.route.AuthorizerID]); x.hauthz == nil {
			return errAuthorizer
		}
	}
	return nil
}

// connect runs $connect, its authorizer and then its integration, and when
// they let the client in, upgrades the request and serves the connection.
func (s *Service) connect(w http.ResponseWriter, x *call) error {
	if !strings.EqualFold(x.r.Header.Get("Upgrade"), "websocket") {
		return core.Errorf(http.StatusUpgradeRequired, "UpgradeRequired", "WebSocket APIs take WebSocket connections")
	}
	now := time.Now().UTC()
	wc := &wsConn{id: core.RandString(15, core.AlphaNum) + "=", api: x.apiID, account: x.account, region: x.region,
		stage: x.stage, vars: x.vars, r: x.r, c: x.c, at: now, last: now}
	x.conn = wc
	if x.hauthz != nil {
		if err := s.authorizeWS(x); err != nil {
			return err
		}
		wc.auth = x.auth
	}
	var res reply
	if x.integ != nil {
		var err error
		if res, err = s.integrate(x, x.wsEvent("CONNECT", nil, false), nil); err != nil {
			return err
		}
		if res.status/100 != 2 {
			return core.Errorf(res.status, "ForbiddenException", "Forbidden")
		}
	}
	opts := &websocket.AcceptOptions{InsecureSkipVerify: true} // API Gateway checks no Origin
	if p := res.header.Get("Sec-WebSocket-Protocol"); p != "" {
		opts.Subprotocols = []string{p}
	}
	conn, err := websocket.Accept(w, x.r, opts)
	if err != nil {
		return nil // Accept has answered
	}
	conn.SetReadLimit(128 << 10) // API Gateway's message size limit
	wc.Conn = conn
	s.mu.Lock()
	wc.idle = time.AfterFunc(idleTimeout, func() { conn.Close(websocket.StatusGoingAway, "Going away") })
	s.conns[wc.id] = wc
	s.mu.Unlock()
	core.Go("apigateway.serve", func() { s.serve(wc) })
	return nil
}

// serve routes the client's messages until the connection ends, then runs $disconnect.
func (s *Service) serve(wc *wsConn) {
	for {
		typ, msg, err := wc.Read(context.Background())
		if err != nil {
			ce := websocket.CloseError{Code: websocket.StatusAbnormalClosure}
			errors.As(err, &ce)
			wc.code, wc.reason = int(ce.Code), ce.Reason
			break
		}
		s.touch(wc)
		s.message(wc, typ, msg)
	}
	s.mu.Lock()
	wc.idle.Stop()
	if s.conns[wc.id] == wc {
		delete(s.conns, wc.id)
	}
	s.mu.Unlock()
	wc.CloseNow()
	s.disconnect(wc)
}

// touch records traffic on the connection, which restarts its idle timer.
func (s *Service) touch(wc *wsConn) {
	s.mu.Lock()
	wc.last = time.Now().UTC()
	wc.idle.Reset(idleTimeout)
	s.mu.Unlock()
}

// message routes a message by the API's route selection expression, else
// to $default, and runs the route's integration. A two-way route (one with
// a route response) sends the answer back; failures always go back.
func (s *Service) message(wc *wsConn, typ websocket.MessageType, msg []byte) {
	x := wc.call()
	var err error
	s.mu.Lock()
	if a := s.st.HTTP[wc.api]; a != nil {
		key := routeKey(a.RouteSelectionExpression, msg)
		if strings.HasPrefix(key, "$") { // $connect and $disconnect take no messages
			key = "$default"
		}
		if err = a.wsRoute(x, key); err == nil && x.route == nil {
			err = a.wsRoute(x, "$default")
		}
	}
	s.mu.Unlock()
	var res reply
	switch {
	case err != nil:
	case x.route == nil:
		err = errForbidden
	case x.integ == nil:
		err = errInternal
	default:
		res, err = s.integrate(x, x.wsEvent("MESSAGE", msg, typ == websocket.MessageBinary), msg)
	}
	if err != nil {
		res.body, _ = json.Marshal(map[string]string{"message": core.AsError(err).Msg, "connectionId": wc.id, "requestId": x.id})
	} else if !x.twoWay {
		return
	}
	if len(res.body) > 0 {
		s.push(wc, res.body)
	}
}

// disconnect runs the $disconnect route of a closed connection; its answer goes nowhere.
func (s *Service) disconnect(wc *wsConn) {
	x := wc.call()
	var err error
	s.mu.Lock()
	if a := s.st.HTTP[wc.api]; a != nil {
		err = a.wsRoute(x, "$disconnect")
	}
	s.mu.Unlock()
	if err == nil && x.integ != nil {
		_, err = s.integrate(x, x.wsEvent("DISCONNECT", nil, false), nil)
	}
	if err != nil {
		s.hub.Log.Warn("api gateway: $disconnect failed", "api", wc.api, "connection", wc.id, "err", err)
	}
}

// push sends data to the client, as a text message when it is UTF-8.
func (s *Service) push(wc *wsConn, data []byte) error {
	typ := websocket.MessageText
	if !utf8.Valid(data) {
		typ = websocket.MessageBinary
	}
	s.touch(wc)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return wc.Write(ctx, typ, data)
}

// routeKey evaluates a route selection expression such as
// $request.body.action on a message; "" when it selects nothing.
func routeKey(expr string, msg []byte) string {
	p, ok := strings.CutPrefix(strings.Trim(expr, "${}"), "request.body")
	if !ok {
		return ""
	}
	v, ok := jsonPath(msg, "$"+p)
	if s, isString := v.(string); isString || !ok || v == nil {
		return s
	}
	b, _ := json.Marshal(v)
	return string(b)
}

// wsEvent is the Lambda proxy event of a WebSocket route.
func (x *call) wsEvent(kind string, msg []byte, binary bool) []byte {
	wc, now := x.conn, time.Now().UTC()
	ip, _, _ := net.SplitHostPort(wc.r.RemoteAddr)
	rc := map[string]any{"routeKey": x.routeKey, "eventType": kind, "messageDirection": "IN", "connectionId": wc.id,
		"connectedAt": wc.at.UnixMilli(), "domainName": wc.r.Host, "stage": x.stage, "apiId": x.apiID, "requestId": x.id,
		"extendedRequestId": x.id, "requestTime": now.Format("02/Jan/2006:15:04:05 -0700"), "requestTimeEpoch": now.UnixMilli(),
		"identity": map[string]any{"sourceIp": ip, "userAgent": wc.r.UserAgent()}}
	if x.auth != nil {
		rc["authorizer"] = x.auth
	}
	ev := map[string]any{"requestContext": rc, "isBase64Encoded": false}
	switch kind {
	case "CONNECT":
		maps.Copy(ev, requestParts(wc.r))
	case "MESSAGE":
		rc["messageId"] = core.RandString(11, core.AlphaNum) + "="
		ev["body"] = string(msg)
		if binary {
			ev["body"], ev["isBase64Encoded"] = base64.StdEncoding.EncodeToString(msg), true
		}
	case "DISCONNECT":
		rc["disconnectStatusCode"], rc["disconnectReason"] = wc.code, wc.reason
	}
	if len(x.vars) > 0 {
		ev["stageVariables"] = x.vars
	}
	b, _ := json.Marshal(ev)
	return b
}

// authorizeWS runs the REQUEST Lambda authorizer of $connect and fills x.auth.
func (s *Service) authorizeWS(x *call) error {
	for _, src := range x.hauthz.IdentitySource {
		if x.identity(src) == "" {
			return errUnauthorized
		}
	}
	arn := core.ARN("execute-api", x.region, x.account, x.apiID+"/"+x.stage+"/$connect")
	var ev map[string]any
	json.Unmarshal(x.wsEvent("CONNECT", nil, false), &ev)
	ev["type"], ev["methodArn"] = "REQUEST", arn
	res, err := s.authorizer(x, x.hauthz.AuthorizerURI, ev)
	if err != nil {
		return err
	}
	if !allows(res["policyDocument"], arn) {
		return errForbidden
	}
	x.auth = map[string]any{}
	if m, ok := res["context"].(map[string]any); ok {
		maps.Copy(x.auth, m)
	}
	x.auth["principalId"] = res["principalId"]
	return nil
}

// reply is an integration's answer to a WebSocket event.
type reply struct {
	status int
	header http.Header
	body   []byte
}

// integrate runs a WebSocket route's integration: a Lambda proxy gets the
// event, an HTTP proxy the message, a mock answers from its integration
// responses.
// ponytail: integration responses shape MOCK answers only; other integrations' answers pass through as they are.
func (s *Service) integrate(x *call, event, msg []byte) (reply, error) {
	in := x.integ
	switch in.IntegrationType {
	case "AWS_PROXY":
		out, err := s.hub.InvokeFrom(x.c.Trace, x.from(), functionARN(in.IntegrationURI, x.vars), event)
		if err != nil {
			return reply{}, s.invokeErr(in.IntegrationURI, err)
		}
		var res struct {
			StatusCode      int               `json:"statusCode"`
			Headers         map[string]string `json:"headers"`
			Body            *string           `json:"body"`
			IsBase64Encoded bool              `json:"isBase64Encoded"`
		}
		if json.Unmarshal(out, &res) != nil || res.StatusCode == 0 && res.Body == nil {
			if string(bytes.TrimSpace(out)) == "null" {
				out = nil
			}
			return reply{status: http.StatusOK, body: out}, nil // not a proxy result: the answer itself, as HTTP APIs take it
		}
		r := reply{status: cmp.Or(res.StatusCode, http.StatusOK), header: http.Header{}}
		for k, v := range res.Headers {
			r.header.Set(k, v)
		}
		if res.Body != nil {
			r.body = []byte(*res.Body)
			if res.IsBase64Encoded {
				r.body, _ = base64.StdEncoding.DecodeString(*res.Body)
			}
		}
		return r, nil
	case "HTTP_PROXY", "HTTP":
		req, err := http.NewRequest(cmp.Or(in.IntegrationMethod, http.MethodPost), stageVars(in.IntegrationURI, x.vars), bytes.NewReader(msg))
		if err != nil {
			return reply{}, errInternal
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := backend.Do(req)
		if err != nil {
			s.hub.Log.Warn("api gateway: HTTP integration failed", "uri", in.IntegrationURI, "err", err)
			return reply{}, errInternal
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return reply{status: resp.StatusCode, header: resp.Header, body: body}, nil
	case "MOCK": // the request template says which response: {"statusCode": 200}
		var m struct {
			StatusCode json.Number `json:"statusCode"`
		}
		if _, t, ok := template(in.RequestTemplates, "$default"); ok {
			json.Unmarshal([]byte(x.render(t, msg)), &m)
		}
		r := reply{}
		r.status, _ = strconv.Atoi(cmp.Or(m.StatusCode.String(), "200"))
		if ir := wsResponse(x.irs, r.status); ir != nil {
			if _, t, ok := template(ir.ResponseTemplates, "$default"); ok {
				r.body = []byte(x.render(t, msg))
			}
		}
		return r, nil
	}
	s.hub.Log.Warn("api gateway: unsupported WebSocket integration", "type", in.IntegrationType)
	return reply{}, errInternal
}

// wsResponse picks the integration response whose key, a status pattern
// such as /2\d\d/, matches status, else the $default one.
func wsResponse(rs map[string]*HTTPIntegrationResponse, status int) *HTTPIntegrationResponse {
	var def *HTTPIntegrationResponse
	for _, ir := range rs {
		if key := ir.IntegrationResponseKey; key == "$default" {
			def = ir
		} else if re, err := regexp.Compile("^(?:" + strings.Trim(key, "/") + ")$"); err == nil && re.MatchString(strconv.Itoa(status)) {
			return ir
		}
	}
	return def
}

// manage serves the @connections API (the apigatewaymanagementapi client):
// POST sends data to the client, GET describes the connection and DELETE
// closes it. A connection that is gone answers 410.
func (s *Service) manage(w http.ResponseWriter, x *call, id string) {
	c, r := x.c, x.r
	c.Op = map[string]string{http.MethodPost: "PostToConnection", http.MethodGet: "GetConnection", http.MethodDelete: "DeleteConnection"}[r.Method]
	err := c.Intercept()
	if err == nil && c.Op == "" {
		err = errNotFound
	}
	if err != nil {
		core.RESTJSONError(w, c, err)
		return
	}
	var info map[string]any
	s.mu.Lock()
	wc := s.conns[id]
	if wc != nil && x.apiID != "" && wc.api != x.apiID {
		wc = nil // another API's connection
	}
	if wc != nil {
		ip, _, _ := net.SplitHostPort(wc.r.RemoteAddr)
		info = map[string]any{"connectedAt": wc.at.Format(time.RFC3339), "lastActiveAt": wc.last.Format(time.RFC3339),
			"identity": map[string]string{"sourceIp": ip, "userAgent": wc.r.UserAgent()}}
		if r.Method == http.MethodDelete {
			delete(s.conns, id)
		}
	}
	s.mu.Unlock()
	gone := core.Errorf(http.StatusGone, "GoneException", "Connection %s is gone", id)
	switch {
	case wc == nil:
		core.RESTJSONError(w, c, gone)
	case r.Method == http.MethodGet:
		core.RESTJSON(w, c, http.StatusOK, info)
	case r.Method == http.MethodDelete:
		go wc.Close(websocket.StatusNormalClosure, "") // the read loop then runs $disconnect
		core.RESTJSON(w, c, http.StatusNoContent, nil)
	case s.push(wc, core.Body(r)) != nil:
		core.RESTJSONError(w, c, gone)
	default:
		core.RESTJSON(w, c, http.StatusOK, nil)
	}
}

// Close drops the open WebSocket connections.
func (s *Service) Close() error {
	s.mu.Lock()
	open := slices.Collect(maps.Values(s.conns))
	s.mu.Unlock()
	for _, wc := range open {
		wc.CloseNow()
	}
	return nil
}
