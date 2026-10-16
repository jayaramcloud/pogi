package appsync

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/nrndev/pogi/internal/core"
	"github.com/vektah/gqlparser/v2/ast"
)

// conn is a real-time connection; its subscriptions are under Service.subMu.
type conn struct {
	ws   *websocket.Conn
	ctx  context.Context
	subs map[string]*sub // by subscription id
}

// sub is a subscription to one field of the Subscription type.
// ponytail: the subscription field's own resolver does not run; enhanced filters are no-ops.
type sub struct {
	c         *conn
	id        string
	api       *API
	field     *ast.Field
	args      map[string]any
	vars      map[string]any
	auth      *auth
	mutations []string // from @aws_subscribe
}

const keepAlive = time.Minute

func (c *conn) send(v any) {
	ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
	defer cancel()
	c.ws.Write(ctx, websocket.MessageText, []byte(toJSON(v)))
}

func errorsPayload(e *gqlErr) map[string]any { return map[string]any{"errors": []*gqlErr{e}} }

// realtime serves AppSync's real-time protocol (subprotocol graphql-ws):
// connection_init and connection_ack, ka, start and start_ack, data, stop and
// complete, and errors. The handshake carries the auth headers as base64 JSON
// in ?header=.
func (s *Service) realtime(w http.ResponseWriter, r *http.Request, apiID, ip string) {
	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"graphql-ws"}, InsecureSkipVerify: true})
	if err != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &conn{ws: ws, ctx: ctx, subs: map[string]*sub{}}
	s.subMu.Lock()
	s.conns[c] = true
	s.subMu.Unlock()
	defer func() {
		cancel()
		s.subMu.Lock()
		delete(s.conns, c)
		s.subMu.Unlock()
		ws.CloseNow()
	}()
	hdr, acked := handshake(r), false
	for {
		_, b, err := ws.Read(ctx)
		if err != nil {
			return
		}
		var m struct {
			ID      string `json:"id"`
			Type    string `json:"type"`
			Payload struct {
				Data       string `json:"data"`
				Extensions struct {
					Authorization map[string]any `json:"authorization"`
				} `json:"extensions"`
			} `json:"payload"`
		}
		json.Unmarshal(b, &m)
		switch m.Type {
		case "connection_init":
			a := s.snapshot(apiID)
			if a == nil {
				return
			}
			if au, msg := s.authenticate(a, &gqlRequest{h: hdr, ip: ip}); au == nil {
				c.send(map[string]any{"type": "connection_error", "payload": map[string]any{"errors": []any{
					map[string]any{"errorType": "UnauthorizedException", "message": msg, "errorCode": 401}}}})
				return
			}
			c.send(map[string]any{"type": "connection_ack", "payload": map[string]any{"connectionTimeoutMs": 300000}})
			c.send(map[string]any{"type": "ka"})
			if !acked {
				acked = true
				core.Go("appsync.keepAlive", func() { c.keepAlive() })
			}
		case "start":
			if e := s.subscribe(c, apiID, m.ID, m.Payload.Data, toHeader(m.Payload.Extensions.Authorization), ip); e != nil {
				c.send(map[string]any{"type": "error", "id": m.ID, "payload": errorsPayload(e)})
			} else {
				c.send(map[string]any{"type": "start_ack", "id": m.ID})
			}
		case "stop":
			s.subMu.Lock()
			delete(c.subs, m.ID)
			s.subMu.Unlock()
			c.send(map[string]any{"type": "complete", "id": m.ID})
		default:
			c.send(map[string]any{"type": "error", "id": m.ID, "payload": errorsPayload(&gqlErr{ErrorType: "UnsupportedOperation",
				Message: fmt.Sprintf("Unsupported operation %q.", m.Type)})})
		}
	}
}

func (c *conn) keepAlive() {
	t := time.NewTicker(keepAlive)
	defer t.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-t.C:
			c.send(map[string]any{"type": "ka"})
		}
	}
}

// handshake decodes the auth headers of a connection request (base64 JSON in ?header=).
func handshake(r *http.Request) header {
	raw, _ := base64.RawStdEncoding.DecodeString(strings.TrimRight(r.URL.Query().Get("header"), "="))
	var m map[string]any
	json.Unmarshal(raw, &m)
	return toHeader(m)
}

func toHeader(m map[string]any) header {
	h := header{}
	for k, v := range m {
		h[strings.ToLower(k)] = fmt.Sprint(v)
	}
	return h
}

// subscribe registers the subscription of a start message.
func (s *Service) subscribe(c *conn, apiID, id, data string, h header, ip string) *gqlErr {
	a := s.snapshot(apiID)
	q := &gqlRequest{h: h, ip: ip}
	if a == nil || json.Unmarshal([]byte(data), q) != nil || q.Query == "" {
		return &gqlErr{ErrorType: "BadRequestException", Message: "The subscription request is not valid."}
	}
	au, msg := s.authenticate(a, q)
	if au == nil {
		return &gqlErr{ErrorType: "UnauthorizedException", Message: msg}
	}
	op, vars, bad := parse(a.parsed, q)
	if bad != nil {
		return bad["errors"].([]*gqlErr)[0]
	}
	if op.Operation != ast.Subscription {
		return &gqlErr{ErrorType: "BadRequestException", Message: "The real-time endpoint serves subscriptions only."}
	}
	x := &exec{s: s, api: a, schema: a.parsed, auth: au, vars: vars, req: q}
	root := a.parsed.Subscription
	fields := x.collect(op.SelectionSet, root)
	if len(fields) != 1 { // validation allows one root field, which @skip may drop
		return &gqlErr{ErrorType: "BadRequestException", Message: "A subscription selects exactly one field."}
	}
	f := fields[0]
	if !x.allowed(root, f.Definition) {
		return &gqlErr{ErrorType: "Unauthorized", Message: fmt.Sprintf("Not Authorized to access %s on type %s", f.Name, root.Name)}
	}
	var mutations []string
	if d := f.Definition.Directives.ForName("aws_subscribe"); d != nil {
		mutations = strs(d.ArgumentMap(nil)["mutations"])
	}
	s.subMu.Lock()
	c.subs[id] = &sub{c: c, id: id, api: a, field: f, args: f.ArgumentMap(vars), vars: vars, auth: au, mutations: mutations}
	s.subMu.Unlock()
	return nil
}

// publish sends a mutation field's result to the subscriptions that listen
// to the mutation and whose arguments match, shaped to their selection sets.
func (s *Service) publish(apiID, mutation string, v any) {
	var hits []*sub
	s.subMu.Lock()
	for c := range s.conns {
		for _, sb := range c.subs {
			if sb.api.ID == apiID && slices.Contains(sb.mutations, mutation) && matches(sb.args, v) {
				hits = append(hits, sb)
			}
		}
	}
	s.subMu.Unlock()
	for _, sb := range hits {
		x := &exec{s: s, api: sb.api, schema: sb.api.parsed, auth: sb.auth, vars: sb.vars, req: &gqlRequest{}, plain: true}
		out, _ := x.complete(sb.field.Definition.Type, sb.field, v, []any{sb.field.Alias})
		data := &object{vals: map[string]any{}}
		data.set(sb.field.Alias, out)
		payload := map[string]any{"data": data}
		if len(x.errs) > 0 {
			payload["errors"] = x.errs
		}
		sb.c.send(map[string]any{"type": "data", "id": sb.id, "payload": payload})
	}
}

// matches is AppSync's subscription filter: every argument the subscriber
// gave must equal the field of the same name in the mutation's result.
func matches(args map[string]any, v any) bool {
	for k, want := range args {
		if want != nil && toJSON(prop(v, k)) != toJSON(want) {
			return false
		}
	}
	return true
}
