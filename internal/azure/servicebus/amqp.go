package servicebus

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/amqp"
	"github.com/nrndev/pogi/internal/core"
)

// encode and decode turn a message into the AMQP 1.0 bytes of its sections and back: Service
// Bus carries whole messages inside management requests and responses (peek, schedule,
// receive-by-sequence-number). Tests replace them with stand-ins.
var (
	encode = (*amqp.Message).Marshal
	decode = amqp.Unmarshal
)

const sessionFilter = "com.microsoft:session-filter"

// resolve answers for Service Bus addresses: <queue>, <topic>,
// <topic>/Subscriptions/<sub>, their /$DeadLetterQueue and /$management.
func (s *Service) resolve(l *amqp.Link) (any, error) {
	host := ""
	if l.Conn != nil {
		host = l.Conn.Host
	}
	nsName := namespaceOf(host)
	s.mu.Lock()
	n := s.find(nsName, l.Address)
	known := n != nil && !n.mgmt
	s.mu.Unlock()
	if known { // Azure RBAC: a JWT is checked against the data actions send and receive (SAS is not)
		if err := s.authorize(l, host); err != nil {
			if l.Receiving {
				return failSource{err}, nil // the receiver sees the error on its first read, as it does for a disabled entity
			}
			return nil, err
		}
	}
	s.mu.Lock()
	n = s.find(nsName, l.Address)
	if n == nil {
		s.mu.Unlock()
		return nil, nil // not ours: Event Hubs may know it
	}
	path, dlq := n.path, n.dlq
	if n.mgmt {
		s.mu.Unlock()
		return amqp.Management(func(ctx context.Context, l *amqp.Link, req *amqp.Message) (*amqp.Message, error) {
			return s.manage(nsName, path, req), nil
		}), nil
	}
	if !l.Receiving {
		s.mu.Unlock()
		if dlq {
			return nil, amqp.Errorf(amqp.NotAllowed, "Cannot send to the dead-letter queue of '%s'.", path)
		}
		l.ReplyMaxMessageSize = 1 << 20 // azservicebus sizes batches by it; 1 MiB, what Standard namespaces allow
		return &sink{svc: s, ns: nsName, path: path}, nil
	}
	if n.kind == "topic" {
		s.mu.Unlock()
		return nil, amqp.Errorf(amqp.NotAllowed, "Cannot receive from the topic '%s'; receive from a subscription.", path)
	}
	if st := n.props().Status; st == "Disabled" || st == "ReceiveDisabled" {
		s.mu.Unlock()
		return nil, amqp.Errorf("com.microsoft:entity-disabled", "The messaging entity '%s' is disabled for receiving.", path)
	}
	src := &source{svc: s, ns: nsName, addr: path, dlq: dlq}
	if dlq {
		s.mu.Unlock()
		return src, nil
	}
	want, sessioned := l.Filter[sessionFilter]
	switch requires := n.props().RequiresSession; {
	case requires && !sessioned:
		s.mu.Unlock()
		return failSource{amqp.Errorf(amqp.NotAllowed, "It is not possible for an entity that requires sessions to create a non-sessionful message receiver. Entity '%s'.", path)}, nil
	case !requires && sessioned:
		s.mu.Unlock()
		return failSource{amqp.Errorf(amqp.NotAllowed, "It is not possible for an entity that does not require sessions to create a sessionful message receiver. Entity '%s'.", path)}, nil
	case !sessioned:
		s.mu.Unlock()
		return src, nil
	}
	id, specific := want.(string)
	wait := time.Duration(0)
	if !specific {
		wait = time.Minute
		if ms, ok := toInt64(l.Properties["com.microsoft:timeout"]); ok {
			wait = time.Duration(ms) * time.Millisecond
		}
	}
	deadline := time.Now().Add(wait)
	for {
		now := s.now()
		var err error
		if !specific {
			id, specific = s.nextSession(n, now)
			if !specific {
				err = amqp.Errorf("com.microsoft:timeout", "No session is available for '%s'.", path)
			}
		}
		if specific {
			err = s.sessionLock(n, id, src, now)
		}
		if err == nil {
			until := n.store().Sessions[id].Until
			src.session = &id
			l.ReplySourceFilter = map[string]any{sessionFilter: id}
			l.ReplyProperties = map[string]any{"com.microsoft:locked-until-utc": ticks(until)}
			s.mu.Unlock()
			return src, nil
		}
		if wait == 0 || !time.Now().Before(deadline) {
			s.mu.Unlock()
			return nil, err
		}
		// ponytail: polling attach for "next available session"; a wake-up per entity if it ever matters
		w := s.waiter()
		s.mu.Unlock()
		select {
		case <-w:
		case <-time.After(50 * time.Millisecond):
		}
		s.mu.Lock()
		if n = s.find(nsName, l.Address); n == nil {
			s.mu.Unlock()
			return nil, amqp.Errorf(amqp.NotFound, "The messaging entity '%s' could not be found.", l.Address)
		}
	}
}

// ticks is a time in .NET ticks, as session lock times travel.
func ticks(t time.Time) int64 { return t.UnixNano()/100 + 621355968000000000 }

func toInt64(v any) (int64, bool) {
	switch x := norm(v).(type) {
	case int64:
		return x, true
	case float64:
		return int64(x), true
	}
	return 0, false
}

// sink takes what senders put on a queue or topic.
type sink struct {
	svc      *Service
	ns, path string
}

func (k *sink) Accept(ctx context.Context, l *amqp.Link, m *amqp.Message) error {
	s := k.svc
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.find(k.ns, k.path)
	if n == nil {
		return amqp.Errorf(amqp.NotFound, "The messaging entity '%s' could not be found.", k.path)
	}
	_, err := s.send(n, ingest(m), s.now(), 0)
	return err
}

// failSource accepts the attach and detaches with its error on the first Next: azservicebus retries a
// refused attach for a long time, and takes an error on an attached link as final.
type failSource struct{ err *amqp.Error }

func (f failSource) Next(context.Context, *amqp.Link) (*amqp.Message, error) { return nil, f.err }
func (f failSource) Settle(*amqp.Link, *amqp.Message, amqp.Outcome)          {}

// source gives a receiver link its messages.
type source struct {
	svc     *Service
	ns      string
	addr    string
	dlq     bool
	session *string
}

func (src *source) node() *node {
	addr := src.addr
	if src.dlq {
		addr += "/$DeadLetterQueue"
	}
	return src.svc.find(src.ns, addr)
}

func (src *source) Next(ctx context.Context, l *amqp.Link) (*amqp.Message, error) {
	s := src.svc
	for {
		s.mu.Lock()
		n := src.node()
		if n == nil {
			s.mu.Unlock()
			return nil, amqp.Errorf(amqp.NotFound, "The messaging entity '%s' could not be found.", src.addr)
		}
		now := s.now()
		if src.session != nil {
			if sess := n.store().Sessions[*src.session]; sess == nil || sess.owner != src || !sess.Until.After(now) {
				s.mu.Unlock()
				return nil, amqp.Errorf("com.microsoft:session-lock-lost", "The session lock has expired on the MessageSession. Accept a new MessageSession.")
			}
		}
		m := s.take(n, src.session, l.Settled, now)
		w := s.waiter()
		s.mu.Unlock()
		if m != nil {
			return m, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-w:
		}
	}
}

func (src *source) Settle(l *amqp.Link, m *amqp.Message, o amqp.Outcome) {
	s := src.svc
	tok, ok := tokenOf(m)
	if !ok {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if n := src.node(); n != nil {
		if _, msg := n.byToken(tok); msg != nil {
			s.settle(n, msg, o, s.now())
		}
	}
}

// Detached releases the session a closed receiver held (amqp.Detacher: the broker calls it
// when the link goes away, after settling what was left).
func (src *source) Detached(l *amqp.Link) {
	if src.session == nil {
		return
	}
	s := src.svc
	s.mu.Lock()
	defer s.mu.Unlock()
	if n := src.node(); n != nil {
		if sess := n.store().Sessions[*src.session]; sess != nil && sess.owner == src {
			sess.owner, sess.Until = nil, time.Time{}
			s.notify()
		}
	}
}

// ---- $management ----

// reply is a management response: status code, description and body.
func reply(code int, desc string, body any) *amqp.Message {
	m := &amqp.Message{ApplicationProperties: map[string]any{"status-code": int32(code), "status-description": desc}}
	if body != nil {
		m.Value = body
	}
	if code >= 400 {
		m.ApplicationProperties["errorCondition"] = map[int]string{404: amqp.NotFound, 410: "com.microsoft:message-lock-lost"}[code]
	}
	return m
}

func fail(code int, format string, a ...any) *amqp.Message {
	return reply(code, fmt.Sprintf(format, a...), nil)
}

// manage answers a request on <entity>/$management.
func (s *Service) manage(nsName, path string, req *amqp.Message) *amqp.Message {
	op, _ := req.ApplicationProperties["operation"].(string)
	body, _ := req.Value.(map[string]any)
	s.mu.Lock()
	defer s.mu.Unlock()
	n := s.find(nsName, path)
	if n == nil {
		return fail(404, "The messaging entity '%s' could not be found.", path)
	}
	now := s.now()
	defer s.notify()
	switch op {
	case "com.microsoft:renew-lock":
		return s.renewLock(n, body, now)
	case "com.microsoft:peek-message":
		return s.peek(n, body)
	case "com.microsoft:receive-by-sequence-number":
		return s.receiveDeferred(n, body, now)
	case "com.microsoft:update-disposition":
		return s.updateDisposition(n, body, now)
	case "com.microsoft:schedule-message":
		return s.schedule(n, body, now)
	case "com.microsoft:cancel-scheduled-message":
		return s.cancelScheduled(n, body)
	case "com.microsoft:get-session-state", "com.microsoft:set-session-state", "com.microsoft:renew-session-lock",
		"com.microsoft:get-message-sessions":
		return s.sessionOp(n, op, body, now)
	case "com.microsoft:add-rule", "com.microsoft:remove-rule", "com.microsoft:enumerate-rules":
		return s.ruleOp(n, op, body)
	}
	return fail(501, "The management operation '%s' is not supported.", op)
}

// toTokens reads lock tokens: a slice of UUIDs, however the codec typed it.
func toTokens(v any) [][16]byte {
	var out [][16]byte
	add := func(x any) {
		switch t := x.(type) {
		case amqp.UUID:
			out = append(out, t)
		case [16]byte:
			out = append(out, t)
		case []byte:
			if len(t) == 16 {
				out = append(out, [16]byte(t))
			}
		}
	}
	switch t := v.(type) {
	case []amqp.UUID:
		for _, u := range t {
			add(u)
		}
	case [][16]byte:
		out = t
	case []any:
		for _, x := range t {
			add(x)
		}
	}
	return out
}

func toInt64s(v any) []int64 {
	switch t := v.(type) {
	case []int64:
		return t
	case []any:
		var out []int64
		for _, x := range t {
			if n, ok := toInt64(x); ok {
				out = append(out, n)
			}
		}
		return out
	}
	return nil
}

func lockLost() *amqp.Message {
	return fail(410, "The lock supplied is invalid. Either the lock expired, or the message has already been removed from the queue.")
}

func (s *Service) renewLock(n *node, body map[string]any, now time.Time) *amqp.Message {
	var msgs []*Msg
	for _, t := range toTokens(body["lock-tokens"]) {
		_, m := n.byToken(t)
		if m == nil {
			return lockLost()
		}
		msgs = append(msgs, m)
	}
	exp := make([]time.Time, 0, len(msgs))
	for _, m := range msgs {
		m.Until = now.Add(time.Duration(n.props().LockDuration))
		n.store().touch(m.Until)
		exp = append(exp, m.Until)
	}
	return reply(200, "OK", map[string]any{"expirations": exp})
}

// wrap packs rendered messages the way management responses carry them.
func wrap(msgs []*amqp.Message) (*amqp.Message, error) {
	items := make([]any, 0, len(msgs))
	for _, m := range msgs {
		b, err := encode(m)
		if err != nil {
			return nil, err
		}
		items = append(items, map[string]any{"message": b})
	}
	return reply(200, "OK", map[string]any{"messages": items}), nil
}

func (s *Service) peek(n *node, body map[string]any) *amqp.Message {
	from, _ := toInt64(body["from-sequence-number"])
	count, ok := toInt64(body["message-count"])
	if !ok || count < 1 {
		count = 1
	}
	sess, bySession := body["session-id"].(string)
	type item struct {
		m     *Msg
		sched bool
	}
	var items []item
	for _, m := range *n.list() {
		items = append(items, item{m, false})
	}
	if !n.dlq {
		for _, m := range n.store().Sched {
			items = append(items, item{m, true})
		}
	}
	slices.SortStableFunc(items, func(a, b item) int { return int(a.m.Seq - b.m.Seq) })
	var out []*amqp.Message
	for _, it := range items {
		if it.m.Seq < from || bySession && it.m.session() != sess {
			continue
		}
		r := n.render(it.m, false)
		s.span(core.SpanDeliver, "peek", n.id(), n.id(), it.m.Trace)
		if it.sched {
			r.MessageAnnotations["x-opt-message-state"] = int32(2)
		}
		out = append(out, r)
		if int64(len(out)) == count {
			break
		}
	}
	if len(out) == 0 {
		return reply(204, "No Content", nil)
	}
	resp, err := wrap(out)
	if err != nil {
		return fail(500, "%v", err)
	}
	return resp
}

func (s *Service) receiveDeferred(n *node, body map[string]any, now time.Time) *amqp.Message {
	mode, _ := toInt64(body["receiver-settle-mode"])
	var found []*Msg
	for _, seq := range toInt64s(body["sequence-numbers"]) {
		m := bySeq(*n.list(), seq)
		if m == nil || !m.Deferred && !n.dlq || m.Locked {
			return fail(404, "The message with sequence number '%d' could not be found, or it is not deferred.", seq)
		}
		found = append(found, m)
	}
	if len(found) == 0 {
		return reply(204, "No Content", nil)
	}
	var out []*amqp.Message
	for _, m := range found {
		if mode == 1 {
			m.Deferred = false
			s.lock(n, m, now)
			m.Deferred = true // stays deferred (invisible to receivers) until settled
			out = append(out, n.render(m, true))
		} else {
			n.remove(m)
			m.Deliveries++
			out = append(out, n.render(m, false))
		}
	}
	resp, err := wrap(out)
	if err != nil {
		return fail(500, "%v", err)
	}
	return resp
}

func (s *Service) updateDisposition(n *node, body map[string]any, now time.Time) *amqp.Message {
	status, _ := body["disposition-status"].(string)
	props, _ := body["properties-to-modify"].(map[string]any)
	reason, _ := body["deadletter-reason"].(string)
	desc, _ := body["deadletter-description"].(string)
	var msgs []*Msg
	for _, t := range toTokens(body["lock-tokens"]) {
		_, m := n.byToken(t)
		if m == nil {
			return lockLost()
		}
		msgs = append(msgs, m)
	}
	for _, m := range msgs {
		m.Deferred = false // a deferred message settled by lock goes back to being plain
		switch status {
		case "completed":
			s.settle(n, m, amqp.Outcome{State: "accepted"}, now)
		case "abandoned":
			s.settle(n, m, amqp.Outcome{State: "modified", Annotations: props}, now)
		case "defered":
			s.settle(n, m, amqp.Outcome{State: "modified", UndeliverableHere: true, Annotations: props}, now)
		case "suspended":
			s.settle(n, m, amqp.Outcome{State: "rejected", Error: &amqp.Error{Condition: "com.microsoft:dead-letter", Info: mergeInfo(props, reason, desc)}}, now)
		default:
			return fail(400, "Unknown disposition status '%s'.", status)
		}
	}
	return reply(200, "OK", nil)
}

func mergeInfo(props map[string]any, reason, desc string) map[string]any {
	info := map[string]any{"DeadLetterReason": reason, "DeadLetterErrorDescription": desc}
	for k, v := range props {
		info[k] = v
	}
	return info
}

func (s *Service) schedule(n *node, body map[string]any, now time.Time) *amqp.Message {
	if n.kind == "subscription" || n.dlq {
		return fail(400, "Cannot schedule messages on '%s'.", n.path)
	}
	items, _ := body["messages"].([]any)
	var seqs []int64
	for _, it := range items {
		entry, _ := it.(map[string]any)
		raw, _ := entry["message"].([]byte)
		am, err := decode(raw)
		if err != nil {
			return fail(400, "Could not read the scheduled message: %v", err)
		}
		m := ingest(am)
		if m.Sched.IsZero() {
			m.Sched = now
		}
		seq, err := s.send(n, m, now, 0)
		if err != nil {
			var ae *amqp.Error
			if errors.As(err, &ae) {
				return fail(400, "%s", ae.Description)
			}
			return fail(500, "%v", err)
		}
		seqs = append(seqs, seq)
	}
	return reply(200, "OK", map[string]any{"sequence-numbers": seqs})
}

func (s *Service) cancelScheduled(n *node, body map[string]any) *amqp.Message {
	st := n.store()
	for _, seq := range toInt64s(body["sequence-numbers"]) {
		m := bySeq(st.Sched, seq)
		if m == nil {
			return fail(404, "The message with sequence number '%d' was not found: it is not scheduled, or was already enqueued.", seq)
		}
		st.Sched = slices.DeleteFunc(st.Sched, func(x *Msg) bool { return x == m })
	}
	return reply(200, "OK", nil)
}

func (s *Service) sessionOp(n *node, op string, body map[string]any, now time.Time) *amqp.Message {
	st := n.store()
	if op == "com.microsoft:get-message-sessions" {
		var ids []string
		for _, m := range st.Msgs {
			if id := m.session(); !slices.Contains(ids, id) {
				ids = append(ids, id)
			}
		}
		if len(ids) == 0 {
			return fail(404, "Session not found")
		}
		return reply(200, "OK", map[string]any{"sessions-ids": ids, "skip": int32(0)})
	}
	id, _ := body["session-id"].(string)
	sess := st.Sessions[id]
	if sess == nil || sess.owner == nil || !sess.Until.After(now) {
		return fail(410, "The session lock has expired on the MessageSession. Accept a new MessageSession.")
	}
	switch op {
	case "com.microsoft:get-session-state":
		var state any
		if len(sess.State) > 0 {
			state = sess.State
		}
		return reply(200, "OK", map[string]any{"session-state": state})
	case "com.microsoft:set-session-state":
		state, _ := body["session-state"].([]byte)
		sess.State = state
		return reply(200, "OK", nil)
	}
	sess.Until = now.Add(time.Duration(n.props().LockDuration))
	st.touch(sess.Until)
	return reply(200, "OK", map[string]any{"expiration": sess.Until})
}

// ruleOp serves add-rule, remove-rule and enumerate-rules on a subscription.
//
// ponytail: rule descriptions travel as plain maps ({"sql-filter": {"expression"}, "correlation-filter":
// {...}, "sql-rule-action": {"expression"}}); the described-list encoding the .NET client
// uses needs the codec's described types. The Go SDK only uses the ATOM API.
func (s *Service) ruleOp(n *node, op string, body map[string]any) *amqp.Message {
	if n.kind != "subscription" {
		return fail(400, "Rules belong to subscriptions.")
	}
	name, _ := body["rule-name"].(string)
	switch op {
	case "com.microsoft:remove-rule":
		for i, r := range n.sub.Rules {
			if strings.EqualFold(r.Name, name) {
				n.sub.Rules = slices.Delete(n.sub.Rules, i, i+1)
				return reply(200, "OK", nil)
			}
		}
		return fail(404, "The rule '%s' could not be found.", name)
	case "com.microsoft:enumerate-rules":
		var rules []any
		for _, r := range n.sub.Rules {
			rules = append(rules, map[string]any{"rule-description": ruleMap(r)})
		}
		return reply(200, "OK", map[string]any{"rules": rules})
	}
	desc, _ := body["rule-description"].(map[string]any)
	r := &Rule{Name: name, Filter: Filter{Type: "TrueFilter"}, Created: s.now()}
	if f, ok := desc["sql-filter"].(map[string]any); ok {
		r.Filter = Filter{Type: "SqlFilter"}
		r.Filter.SQL, _ = f["expression"].(string)
	}
	if f, ok := desc["correlation-filter"].(map[string]any); ok {
		r.Filter = Filter{Type: "CorrelationFilter"}
		r.Filter.CorrelationID, _ = f["correlation-id"].(string)
		r.Filter.MessageID, _ = f["message-id"].(string)
		r.Filter.To, _ = f["to"].(string)
		r.Filter.ReplyTo, _ = f["reply-to"].(string)
		r.Filter.Label, _ = f["label"].(string)
		r.Filter.SessionID, _ = f["session-id"].(string)
		r.Filter.ReplyToSessionID, _ = f["reply-to-session-id"].(string)
		r.Filter.ContentType, _ = f["content-type"].(string)
		r.Filter.Properties, _ = f["properties"].(map[string]any)
	}
	if a, ok := desc["sql-rule-action"].(map[string]any); ok {
		r.Action, _ = a["expression"].(string)
	}
	if err := r.compile(); err != nil {
		return fail(400, "%v", err)
	}
	if slices.ContainsFunc(n.sub.Rules, func(x *Rule) bool { return strings.EqualFold(x.Name, r.Name) }) {
		return fail(409, "The rule '%s' already exists.", r.Name)
	}
	n.sub.Rules = append(n.sub.Rules, r)
	return reply(200, "OK", nil)
}

func ruleMap(r *Rule) map[string]any {
	d := map[string]any{}
	switch r.Filter.Type {
	case "SqlFilter":
		d["sql-filter"] = map[string]any{"expression": r.Filter.SQL}
	case "CorrelationFilter":
		d["correlation-filter"] = map[string]any{"correlation-id": r.Filter.CorrelationID, "message-id": r.Filter.MessageID,
			"to": r.Filter.To, "reply-to": r.Filter.ReplyTo, "label": r.Filter.Label, "session-id": r.Filter.SessionID,
			"reply-to-session-id": r.Filter.ReplyToSessionID, "content-type": r.Filter.ContentType, "properties": r.Filter.Properties}
	default:
		d["true-filter"] = map[string]any{}
	}
	if r.Action != "" {
		d["sql-rule-action"] = map[string]any{"expression": r.Action}
	}
	d["name"] = r.Name
	return d
}

// authorize asks the hub's Azure authorizer whether the JWT put with $cbs may send to, or receive from, the entity.
func (s *Service) authorize(l *amqp.Link, host string) *amqp.Error {
	tok := ""
	if l.Conn != nil {
		tok = l.Conn.JWT(l.Address)
	}
	if tok == "" || s.hub == nil {
		return nil
	}
	op := "amqp send"
	if l.Receiving {
		op = "amqp receive"
	}
	c := &core.Ctx{Hub: s.hub, Cloud: core.Azure, Service: "servicebus", Op: op, Host: host, Path: "/" + l.Address, RequestID: core.NewID(), Bearer: tok}
	if e := s.hub.Check(c); e != nil {
		return amqp.Errorf(amqp.Unauthorized, "%s", e.Msg)
	}
	return nil
}
