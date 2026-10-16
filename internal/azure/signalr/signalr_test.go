package signalr_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/signalr"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

type env struct {
	srv *pogi.Running
	svc *signalr.Service
}

func setup(t *testing.T) *env {
	t.Helper()
	srv := pogi.Start(t)
	return &env{srv, srv.Hub.Service("signalr").(*signalr.Service)}
}

// call makes a request; host overrides the Host header.
func (e *env) call(t *testing.T, method, path, body, host string, header ...string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(method, e.srv.URL+path, strings.NewReader(body))
	if host != "" {
		req.Host = host
	}
	for i := 0; i < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	res := must(http.DefaultClient.Do(req))
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return res.StatusCode, string(b)
}

func (e *env) status(t *testing.T, want int, method, path, body string) {
	t.Helper()
	if got, out := e.call(t, method, path, body, ""); got != want {
		t.Fatalf("%s %s = %d %s, want %d", method, path, got, out, want)
	}
}

// verify checks an HS256 token by hand and returns its claims.
func verify(t *testing.T, key, token string) map[string]any {
	t.Helper()
	p := strings.Split(token, ".")
	m := hmac.New(sha256.New, []byte(key))
	m.Write([]byte(p[0] + "." + p[1]))
	if p[2] != base64.RawURLEncoding.EncodeToString(m.Sum(nil)) {
		t.Fatalf("the token is not signed with %q: %s", key, token)
	}
	var head, claims map[string]any
	h := must(base64.RawURLEncoding.DecodeString(p[0]))
	json.Unmarshal(h, &head)
	json.Unmarshal(must(base64.RawURLEncoding.DecodeString(p[1])), &claims)
	if head["alg"] != "HS256" {
		t.Fatalf("token header = %s", h)
	}
	return claims
}

// client is a raw SignalR client over WebSocket.
type client struct {
	t      *testing.T
	ws     *websocket.Conn
	id     string
	frames chan []byte // a reader goroutine feeds it: a Read that times out would close the connection
}

func newClient(t *testing.T, ws *websocket.Conn) *client {
	c := &client{t: t, ws: ws, frames: make(chan []byte, 100)}
	go func() {
		for {
			_, b, err := ws.Read(ctx)
			if err != nil {
				close(c.frames)
				return
			}
			c.frames <- b
		}
	}()
	return c
}

// connect negotiates in both steps and connects, as the SignalR JavaScript client does.
func (e *env) connect(t *testing.T, hub, user string) *client {
	t.Helper()
	status, out := e.call(t, "POST", "/client/negotiate?hub="+hub+"&userId="+user, "", "")
	var first struct{ URL, AccessToken string }
	if json.Unmarshal([]byte(out), &first); status != 200 || first.URL == "" || first.AccessToken == "" {
		t.Fatalf("negotiate = %d %s", status, out)
	}
	status, out = e.call(t, "POST", "/client/negotiate?hub="+hub+"&negotiateVersion=1", "", "", "Authorization", "Bearer "+first.AccessToken)
	var second struct {
		ConnectionID, ConnectionToken string
		AvailableTransports           []struct{ Transport string }
	}
	if json.Unmarshal([]byte(out), &second); status != 200 || second.ConnectionID == "" || second.AvailableTransports[0].Transport != "WebSockets" {
		t.Fatalf("second negotiate = %d %s", status, out)
	}
	url := "ws" + strings.TrimPrefix(e.srv.URL, "http") + "/client/?hub=" + hub + "&id=" + second.ConnectionToken + "&access_token=" + first.AccessToken
	ws, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ws.CloseNow() })
	c := newClient(t, ws)
	c.id = second.ConnectionID
	c.write(`{"protocol":"json","version":1}`)
	if got := c.read(); got != "{}" {
		t.Fatalf("handshake answer = %q", got)
	}
	return c
}

func (c *client) write(frame string) {
	c.t.Helper()
	if err := c.ws.Write(ctx, websocket.MessageText, []byte(frame+"\x1e")); err != nil {
		c.t.Fatal(err)
	}
}

// read returns the next frame, or "" when nothing comes soon.
func (c *client) read() string {
	c.t.Helper()
	var b []byte
	select {
	case b = <-c.frames:
	case <-time.After(400 * time.Millisecond):
	}
	if b == nil {
		return ""
	}
	if !strings.HasSuffix(string(b), "\x1e") {
		c.t.Fatalf("frame %q does not end with the record separator", b)
	}
	return strings.TrimSuffix(string(b), "\x1e")
}

func (c *client) want(frame string) {
	c.t.Helper()
	// the first read can be slow under -race; allow a few tries
	got := ""
	for range 10 {
		if got = c.read(); got != "" {
			break
		}
	}
	if got != frame {
		c.t.Fatalf("got frame %q, want %q", got, frame)
	}
}

func TestNegotiateConnectBroadcastAndGroups(t *testing.T) {
	e := setup(t)

	// Negotiation hands out a token signed with the resource's access key.
	_, out := e.call(t, "POST", "/client/negotiate?hub=chat&userId=alice", "", "")
	var neg struct{ URL, AccessToken string }
	json.Unmarshal([]byte(out), &neg)
	if !strings.HasSuffix(neg.URL, "/client/?hub=chat") {
		t.Fatalf("negotiate url = %q", neg.URL)
	}
	if claims := verify(t, signalr.DefaultKey, neg.AccessToken); claims["nameid"] != "alice" || claims["aud"] != neg.URL {
		t.Fatalf("token claims = %v", claims)
	}
	if status, _ := e.call(t, "POST", "/_azure/signalr/default/client/negotiate", "", ""); status != 400 {
		t.Fatalf("negotiate without a hub = %d", status)
	}
	// A WebSocket without a valid token is refused.
	if _, resp, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(e.srv.URL, "http")+"/client/?hub=chat&access_token=a.b.c", nil); err == nil || resp.StatusCode != 401 {
		t.Fatalf("a bad token: %v", err)
	}

	alice, bob := e.connect(t, "chat", "alice"), e.connect(t, "chat", "bob")
	other := e.connect(t, "other", "alice") // another hub hears nothing of chat

	// Broadcast, v1.
	e.status(t, 202, "POST", "/api/v1/hubs/chat", `{"target":"hello","arguments":["world",1]}`)
	for _, c := range []*client{alice, bob} {
		c.want(`{"type":1,"target":"hello","arguments":["world",1]}`)
	}
	if got := other.read(); got != "" {
		t.Fatalf("another hub got %q", got)
	}
	e.status(t, 400, "POST", "/api/v1/hubs/chat", `{"arguments":[]}`)
	// Excluded connections.
	e.status(t, 202, "POST", "/api/v1/hubs/chat?excluded="+bob.id, `{"target":"x"}`)
	alice.want(`{"type":1,"target":"x","arguments":[]}`)
	if got := bob.read(); got != "" {
		t.Fatalf("an excluded connection got %q", got)
	}

	// To a connection and to a user.
	e.status(t, 202, "POST", "/api/v1/hubs/chat/connections/"+bob.id, `{"target":"dm","arguments":[]}`)
	bob.want(`{"type":1,"target":"dm","arguments":[]}`)
	e.status(t, 202, "POST", "/api/v1/hubs/chat/users/alice", `{"target":"u","arguments":["a"]}`)
	alice.want(`{"type":1,"target":"u","arguments":["a"]}`)
	if got := bob.read(); got != "" {
		t.Fatalf("bob got alice's message %q", got)
	}

	// Groups: add the connection, send, remove, and the connection no longer hears the group.
	e.status(t, 404, "HEAD", "/api/v1/hubs/chat/groups/room", "")
	e.status(t, 404, "PUT", "/api/v1/hubs/chat/groups/room/connections/nobody", "")
	e.status(t, 202, "PUT", "/api/v1/hubs/chat/groups/room/connections/"+alice.id, "")
	e.status(t, 200, "HEAD", "/api/v1/hubs/chat/groups/room", "")
	e.status(t, 200, "HEAD", "/api/v1/hubs/chat/groups/room/connections/"+alice.id, "")
	e.status(t, 404, "HEAD", "/api/v1/hubs/chat/groups/room/connections/"+bob.id, "")
	e.status(t, 202, "POST", "/api/v1/hubs/chat/groups/room", `{"target":"g","arguments":["in"]}`)
	alice.want(`{"type":1,"target":"g","arguments":["in"]}`)
	if got := bob.read(); got != "" {
		t.Fatalf("a non-member got %q", got)
	}
	e.status(t, 202, "DELETE", "/api/v1/hubs/chat/groups/room/connections/"+alice.id, "")
	e.status(t, 404, "HEAD", "/api/v1/hubs/chat/groups/room", "")
	e.status(t, 202, "POST", "/api/v1/hubs/chat/groups/room", `{"target":"g","arguments":["out"]}`)
	if got := alice.read(); got != "" {
		t.Fatalf("a removed member got %q", got)
	}

	// A user in a group: all of the user's connections get it, including ones made later.
	e.status(t, 202, "PUT", "/api/v1/hubs/chat/users/bob/groups/vip", "")
	e.status(t, 200, "HEAD", "/api/v1/hubs/chat/users/bob/groups/vip", "")
	e.status(t, 202, "POST", "/api/v1/hubs/chat/groups/vip", `{"target":"v","arguments":[]}`)
	bob.want(`{"type":1,"target":"v","arguments":[]}`)
	bob2 := e.connect(t, "chat", "bob")
	e.status(t, 202, "POST", "/api/v1/hubs/chat/groups/vip", `{"target":"v2","arguments":[]}`)
	bob.want(`{"type":1,"target":"v2","arguments":[]}`)
	bob2.want(`{"type":1,"target":"v2","arguments":[]}`)
	e.status(t, 202, "DELETE", "/api/v1/hubs/chat/users/bob/groups", "")
	e.status(t, 404, "HEAD", "/api/v1/hubs/chat/users/bob/groups/vip", "")

	// The 2022-11-01 API: :send, 200 for add, 204 for remove.
	const v = "?api-version=2022-11-01"
	e.status(t, 202, "POST", "/api/hubs/chat/:send"+v, `{"target":"new","arguments":[true]}`)
	alice.want(`{"type":1,"target":"new","arguments":[true]}`)
	bob.want(`{"type":1,"target":"new","arguments":[true]}`)
	bob2.want(`{"type":1,"target":"new","arguments":[true]}`)
	e.status(t, 202, "POST", "/api/hubs/chat/users/alice/:send"+v, `{"target":"nu","arguments":[]}`)
	alice.want(`{"type":1,"target":"nu","arguments":[]}`)
	e.status(t, 202, "POST", "/api/hubs/chat/connections/"+bob.id+"/:send"+v, `{"target":"nc","arguments":[]}`)
	bob.want(`{"type":1,"target":"nc","arguments":[]}`)
	e.status(t, 200, "PUT", "/api/hubs/chat/groups/g22/connections/"+alice.id+v, "")
	e.status(t, 202, "POST", "/api/hubs/chat/groups/g22/:send"+v, `{"target":"ng","arguments":[]}`)
	alice.want(`{"type":1,"target":"ng","arguments":[]}`)
	e.status(t, 204, "DELETE", "/api/hubs/chat/groups/g22/connections/"+alice.id+v, "")
	e.status(t, 200, "HEAD", "/api/hubs/chat/connections/"+alice.id+v, "")
	e.status(t, 200, "HEAD", "/api/hubs/chat/users/alice"+v, "")
	e.status(t, 404, "HEAD", "/api/hubs/chat/users/nobody"+v, "")
	e.status(t, 200, "GET", "/_azure/signalr/default/api/health", "")

	// Pings are welcome, and invocations complete (there is no app server).
	alice.write(`{"type":6}`)
	alice.write(`{"type":1,"invocationId":"7","target":"send","arguments":["hi"]}`)
	alice.want(`{"type":3,"invocationId":"7"}`)

	// The console sees the connections.
	var conns int
	for _, tb := range e.svc.Resources() {
		if tb.Name == "Connections" {
			conns = len(tb.Rows)
		}
	}
	if conns != 4 {
		t.Fatalf("console connections = %d, want 4", conns)
	}
	if got, _ := e.svc.Inspect("Connections", alice.id).(map[string]any); got["user"] != "alice" || got["hub"] != "chat" {
		t.Fatalf("Inspect connection = %v", got)
	}

	// Closing a connection from the server sends the close message, and it is gone.
	e.status(t, 202, "DELETE", "/api/v1/hubs/chat/connections/"+bob2.id, "")
	bob2.want(`{"type":7}`)
	e.status(t, 404, "HEAD", "/api/v1/hubs/chat/connections/"+bob2.id, "")
	e.status(t, 404, "DELETE", "/api/v1/hubs/chat/connections/"+bob2.id, "")
}

func TestResourcesAndKeys(t *testing.T) {
	e := setup(t)
	e.svc.SetKey("prod", "a-different-key")

	// The host form and the path form name the resource; each has its own key and its own hubs.
	_, out := e.call(t, "POST", "/client/negotiate?hub=h", "", "prod.service.signalr.localhost:7644")
	var host struct{ URL, AccessToken string }
	json.Unmarshal([]byte(out), &host)
	if verify(t, "a-different-key", host.AccessToken)["aud"] != host.URL || !strings.HasPrefix(host.URL, "http://prod.service.signalr.localhost:7644/client/?hub=h") {
		t.Fatalf("host form = %s", out)
	}
	_, out = e.call(t, "POST", "/_azure/signalr/prod/client/negotiate?hub=h", "", "")
	var path struct{ URL, AccessToken string }
	json.Unmarshal([]byte(out), &path)
	if !strings.Contains(path.URL, "/_azure/signalr/prod/client/?hub=h") {
		t.Fatalf("path form = %s", out)
	}
	verify(t, "a-different-key", path.AccessToken)

	// A token of one resource is no good at another.
	if status, _ := e.call(t, "POST", "/client/negotiate?hub=h", "", "", "Authorization", "Bearer "+host.AccessToken); status != 401 {
		t.Fatalf("a token of another resource = %d", status)
	}

	// A client of the host-routed resource, and the REST API on the same host.
	url := "ws" + strings.TrimPrefix(e.srv.URL, "http") + "/_azure/signalr/prod/client/?hub=h&access_token=" + path.AccessToken
	ws, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.CloseNow()
	ws.Write(ctx, websocket.MessageText, []byte(`{"protocol":"json","version":1}`+"\x1e"))
	ws.Read(ctx)
	if status, _ := e.call(t, "POST", "/api/v1/hubs/h", `{"target":"t","arguments":[]}`, "prod.service.signalr.localhost"); status != 202 {
		t.Fatalf("REST on the host form = %d", status)
	}
	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, b, err := ws.Read(rctx); err != nil || string(b) != `{"type":1,"target":"t","arguments":[]}`+"\x1e" {
		t.Fatalf("host-form broadcast = %q %v", b, err)
	}
	// the default resource's hub "h" is a different one
	if status, _ := e.call(t, "HEAD", "/api/v1/hubs/h/users/x", "", ""); status != 404 {
		t.Fatalf("default resource = %d", status)
	}

	// An unknown protocol is refused in the handshake.
	ws2, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ws2.CloseNow()
	ws2.Write(ctx, websocket.MessageText, []byte(`{"protocol":"messagepack","version":1}`+"\x1e"))
	if _, b, _ := ws2.Read(rctx); !strings.Contains(string(b), "messagepack") || !strings.Contains(string(b), `"error"`) {
		t.Fatalf("handshake error = %q", b)
	}

	// Save, reset, load.
	saved := must(e.svc.Save())
	e.svc.Reset()
	if len(e.svc.Resources()[0].Rows) != 0 {
		t.Fatal("Reset kept resources")
	}
	if err := e.svc.Load(saved); err != nil {
		t.Fatal(err)
	}
	if got, _ := e.svc.Inspect("Resources", "prod").(map[string]any); got["accessKey"] != "a-different-key" {
		t.Fatalf("after Load = %v", got)
	}
}
