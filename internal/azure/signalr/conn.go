package signalr

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/nrndev/pogi/internal/core"
)

// rs ends every message of the SignalR hub protocol.
const rs = "\x1e"

// conn is a connected client.
type conn struct {
	id, user, hub string
	frames        chan []byte
	cancel        context.CancelFunc
	once          sync.Once
	closing       chan struct{}
}

var closeFrame = []byte(`{"type":7}` + rs)

// send queues a frame for the writer. A client that does not keep up loses frames.
// ponytail: 256 frames of buffer per connection, then drops; block or disconnect if that matters
func (c *conn) send(frame []byte) {
	select {
	case c.frames <- frame:
	default:
	}
}

// close sends the close message and ends the connection.
func (c *conn) close() { c.once.Do(func() { close(c.closing) }) }

// connect upgrades to a WebSocket and speaks the JSON hub protocol until the client leaves.
func (s *Service) connect(w http.ResponseWriter, r *http.Request, c *core.Ctx, name string) {
	hub := r.URL.Query().Get("hub")
	cl, ok := claims(s.key(name), bearer(r))
	if !ok {
		writeError(w, c, http.StatusUnauthorized, "Unauthorized", "The access token is missing or not valid for this resource.")
		return
	}
	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true, Subprotocols: []string{"json"}})
	if err != nil {
		return // Accept has answered
	}
	defer ws.CloseNow()
	ws.SetReadLimit(1 << 20)
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	// the handshake: {"protocol":"json","version":1} answered by {}
	_, first, err := ws.Read(ctx)
	if err != nil {
		return
	}
	var hs struct{ Protocol string }
	if json.Unmarshal(bytes.TrimSuffix(first, []byte(rs)), &hs) != nil || hs.Protocol != "json" {
		ws.Write(ctx, websocket.MessageText, []byte(`{"error":"Requested protocol '`+hs.Protocol+`' is not available."}`+rs))
		ws.Close(websocket.StatusNormalClosure, "")
		return
	}
	if ws.Write(ctx, websocket.MessageText, []byte("{}"+rs)) != nil {
		return
	}

	cn := &conn{id: core.RandString(22, core.AlphaNum), user: user(cl), hub: hub, frames: make(chan []byte, 256), cancel: cancel, closing: make(chan struct{})}
	s.mu.Lock()
	if p := s.pending[r.URL.Query().Get("id")]; p != nil {
		cn.id, cn.user = p.id, p.user
		delete(s.pending, r.URL.Query().Get("id"))
	}
	s.hub(name, hub).conns[cn.id] = cn
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.hub(name, hub).remove(cn.id)
		s.mu.Unlock()
	}()

	core.Go("signalr.connect", func() { // the writer; the reader below is the only other user of ws
		ping := time.NewTicker(15 * time.Second)
		defer ping.Stop()
		for {
			var frame []byte
			closed := false
			select {
			case frame = <-cn.frames:
			case <-ping.C:
				frame = []byte(`{"type":6}` + rs)
			case <-cn.closing:
				frame, closed = closeFrame, true
			case <-ctx.Done():
				return
			}
			if ws.Write(ctx, websocket.MessageText, frame) != nil || closed {
				ws.Close(websocket.StatusNormalClosure, "")
				cancel()
				return
			}
		}
	})

	for {
		_, data, err := ws.Read(ctx)
		if err != nil {
			return
		}
		for _, m := range bytes.Split(data, []byte(rs)) {
			var msg struct {
				Type         int
				InvocationID string `json:"invocationId"`
			}
			if json.Unmarshal(m, &msg) != nil {
				continue
			}
			switch msg.Type {
			case 7: // the client leaves
				return
			case 1: // an invocation: there is no app server behind pogi's hub, so it completes with nothing
				if msg.InvocationID != "" {
					b, _ := json.Marshal(struct {
						Type         int    `json:"type"`
						InvocationID string `json:"invocationId"`
					}{3, msg.InvocationID})
					cn.send(append(b, rs...))
				}
			}
		}
	}
}
