package amqp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// serveRequest answers a request on a request-response node: it calls the
// handler and queues the reply on the client's response link.
func (l *lnk) serveRequest(d *inDelivery) error {
	req, err := Unmarshal(d.payload)
	if err != nil {
		return Errorf("amqp:decode-error", "%v", err)
	}
	req.Format, req.DeliveryTag = d.format, d.tag
	var replyTo string
	var id any
	if p := req.Properties; p != nil {
		replyTo, id = p.ReplyTo, p.MessageID
	}
	c := l.c
	c.mu.Lock()
	rl := c.replies[replyTo]
	c.mu.Unlock()
	if rl == nil {
		return Errorf(NotFound, "no link receives replies at %q", replyTo)
	}
	resp, herr := l.mgmt(l.ctx, l.pub, req)
	select {
	case rl.reply <- finishResponse(resp, herr, id, replyTo):
	case <-l.ctx.Done():
	case <-rl.ctx.Done():
	}
	return nil
}

// statusCode maps a handler error to the HTTP-like status Service Bus and
// Event Hubs management replies carry.
func statusCode(e *Error) int {
	switch e.Condition {
	case NotFound:
		return 404
	case Unauthorized:
		return 401
	case InvalidField:
		return 400
	case NotAllowed, ResourceLimit:
		return 403
	case "com.microsoft:entity-already-exists":
		return 409
	case "com.microsoft:message-lock-lost", "com.microsoft:session-lock-lost":
		return 410
	}
	return 500
}

// finishResponse makes the reply to a request: correlation-id from the
// request's message-id, and the status in both spellings the SDKs read
// (Service Bus statusCode, Event Hubs and $cbs status-code), as int32.
func finishResponse(resp *Message, herr error, id any, replyTo string) *Message {
	out := &Message{}
	if resp != nil {
		*out = *resp
	}
	props := map[string]any{}
	for k, v := range out.ApplicationProperties {
		props[k] = v
	}
	if herr != nil {
		e := asError(herr)
		out = &Message{}
		props = map[string]any{"status-code": int32(statusCode(e)), "status-description": e.Description, "errorCondition": e.Condition}
	}
	p := Properties{}
	if out.Properties != nil {
		p = *out.Properties
	}
	p.CorrelationID = id
	if p.To == "" {
		p.To = replyTo
	}
	out.Properties = &p
	var code any
	for _, k := range []string{"status-code", "statusCode"} {
		if v, ok := props[k]; ok {
			n, _ := asInt(v)
			code = int32(n)
			break
		}
	}
	if code == nil {
		code = int32(200)
	}
	props["status-code"], props["statusCode"] = code, code
	for _, pair := range [][2]string{{"status-description", "statusDescription"}} {
		for _, k := range pair {
			if v, ok := props[k]; ok {
				props[pair[0]], props[pair[1]] = v, v
				break
			}
		}
	}
	out.ApplicationProperties = props
	return out
}

func asInt(v any) (int64, bool) {
	switch x := v.(type) {
	case int:
		return int64(x), true
	case int8:
		return int64(x), true
	case int16:
		return int64(x), true
	case int32:
		return int64(x), true
	case int64:
		return x, true
	}
	n, ok := asUint(v)
	return int64(n), ok
}

// putToken is the $cbs node: it checks the token with Options.Authorize and
// records the claim on the connection.
func (c *conn) putToken(ctx context.Context, l *Link, req *Message) (*Message, error) {
	ap := req.ApplicationProperties
	if op := str(ap["operation"]); op != "put-token" {
		return nil, Errorf(InvalidField, "unsupported $cbs operation %q", op)
	}
	typ, name := str(ap["type"]), str(ap["name"])
	var token string
	switch v := req.Value.(type) {
	case string:
		token = v
	case Symbol:
		token = string(v)
	case []byte:
		token = string(v)
	default:
		if len(req.Data) > 0 {
			token = string(req.Data[0])
		}
	}
	if token == "" || name == "" {
		return nil, Errorf(InvalidField, "put-token needs a name and a token")
	}
	if auth := c.s.opt.Authorize; auth != nil {
		if err := auth(typ, name, token); err != nil {
			e := asError(err)
			if e.Condition == InternalError { // a plain error from the callback means the token is refused
				e = Errorf(Unauthorized, "%v", err)
			}
			return nil, e
		}
	}
	exp := tokenExpiry(typ, token)
	if t, ok := ap["expiration"].(time.Time); ok && exp.IsZero() {
		exp = t
	}
	c.pub.addClaim(name, exp)
	if typ == "jwt" {
		c.pub.addJWT(name, token)
	}
	return &Message{ApplicationProperties: map[string]any{"status-code": int32(202), "status-description": "Accepted"}}, nil
}

// tokenExpiry reads the expiry of a SAS token (se=<unix seconds>) or a JWT
// (exp claim); zero when it can't tell.
func tokenExpiry(typ, token string) time.Time {
	if strings.HasPrefix(token, "SharedAccessSignature ") {
		q, err := url.ParseQuery(strings.TrimPrefix(token, "SharedAccessSignature "))
		if err == nil {
			if n, err := strconv.ParseInt(q.Get("se"), 10, 64); err == nil {
				return time.Unix(n, 0)
			}
		}
		return time.Time{}
	}
	if parts := strings.Split(token, "."); len(parts) == 3 {
		if b, err := base64.RawURLEncoding.DecodeString(parts[1]); err == nil {
			var claims struct {
				Exp float64 `json:"exp"`
			}
			if json.Unmarshal(b, &claims) == nil && claims.Exp > 0 {
				return time.Unix(int64(claims.Exp), 0)
			}
		}
	}
	return time.Time{}
}
