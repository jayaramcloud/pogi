package iot

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/nrndev/pogi/internal/core"
)

// Data is the IoT data plane (iotdata): Publish, device shadows and retained
// messages, on the broker and state of the control plane. It also takes MQTT
// over WebSocket at /mqtt.
type Data struct {
	s   *Service
	mux *http.ServeMux
	own bool // no iot service is registered: the broker is ours to close
}

// NewData creates the data plane on the iot service, or on a private one when iot is off.
func NewData(h *core.Hub) *Data {
	s, ok := h.Service("iot").(*Service)
	if !ok {
		s = New(h)
	}
	d := &Data{s: s, mux: http.NewServeMux(), own: !ok}
	for pattern, o := range map[string]struct {
		name string
		fn   func(*core.Ctx, *http.Request) (any, error)
	}{
		"POST /topics/{topic...}":                                 {"Publish", d.publish},
		"GET /things/{thing}/shadow":                              {"GetThingShadow", d.getShadow},
		"POST /things/{thing}/shadow":                             {"UpdateThingShadow", d.updateShadow},
		"DELETE /things/{thing}/shadow":                           {"DeleteThingShadow", d.deleteShadow},
		"GET /retainedMessage/{topic...}":                         {"GetRetainedMessage", d.getRetained},
		"GET /retainedMessage":                                    {"ListRetainedMessages", d.listRetained},
		"GET /api/things/shadow/ListNamedShadowsForThing/{thing}": {"ListNamedShadowsForThing", d.listShadows},
		"/": {"Unknown", s.unknown},
	} {
		d.mux.HandleFunc(pattern, s.handle(o.name, o.fn))
	}
	return d
}

func (d *Data) Info() core.Info {
	return core.Info{Name: "iotdata", Cloud: core.AWS, Title: "IoT Data", Signing: []string{"iotdata"}, Match: socket, Protocol: "rest-json"}
}

// socket matches MQTT over WebSocket: wss://<endpoint>/mqtt, signed or not.
func socket(r *http.Request) bool {
	return r.URL.Path == "/mqtt" && strings.EqualFold(r.Header.Get("Upgrade"), "websocket")
}

func (d *Data) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if socket(r) {
		core.C(r).Op = "MQTT"
		d.s.connect(w, r)
		return
	}
	d.mux.ServeHTTP(w, r)
}

// Close stops a private broker.
func (d *Data) Close() error {
	if d.own {
		return d.s.Close()
	}
	return nil
}

// ---- the broker ----

// hook ties the broker to pogi: every client may connect and use any topic,
// and every message goes through the shadows and the topic rules.
type hook struct {
	mqtt.HookBase
	s *Service
}

func (h *hook) ID() string { return "pogi" }

func (h *hook) Provides(b byte) bool {
	return b == mqtt.OnConnectAuthenticate || b == mqtt.OnACLCheck || b == mqtt.OnPublished
}

// OnConnectAuthenticate lets every client in.
// ponytail: no TLS client-certificate auth; check a client's certificate here
// against the ACTIVE certificates and their policies when it matters.
func (h *hook) OnConnectAuthenticate(*mqtt.Client, packets.Packet) bool { return true }

func (h *hook) OnACLCheck(*mqtt.Client, string, bool) bool { return true }

func (h *hook) OnPublished(cl *mqtt.Client, pk packets.Packet) {
	client := cl.ID
	if cl.Net.Inline {
		client = "" // the HTTP API, the shadows and republish
	}
	h.s.published(client, pk.TopicName, bytes.Clone(pk.Payload))
}

// published runs a message through the shadow service and the topic rules.
func (s *Service) published(client, topic string, payload []byte) {
	if thing, name, op, ok := shadowTopic(topic); ok {
		s.shadowRequest(thing, name, op, payload)
	}
	m := &message{topic: topic, client: client, payload: payload, doc: undef, at: time.Now()}
	var doc any
	if json.Unmarshal(payload, &doc) == nil {
		m.doc = doc
	}
	s.rules(m)
}

// wsListener stands for the WebSocket clients of /mqtt, so Close ends them too.
type wsListener struct{}

func (wsListener) Init(*slog.Logger) error          { return nil }
func (wsListener) Serve(listeners.EstablishFn)      {}
func (wsListener) ID() string                       { return "ws" }
func (wsListener) Address() string                  { return "/mqtt" }
func (wsListener) Protocol() string                 { return "ws" }
func (wsListener) Close(closeAll listeners.CloseFn) { closeAll("ws") }

var upgrader = websocket.Upgrader{Subprotocols: []string{"mqtt"}, CheckOrigin: func(*http.Request) bool { return true }}

// connect hands an MQTT-over-WebSocket connection to the broker until it ends.
func (s *Service) connect(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(hijacker{w}, r, nil)
	if err != nil {
		return // Upgrade has answered
	}
	defer ws.Close()
	s.mq.EstablishConnection("ws", &wsConn{Conn: ws.NetConn(), ws: ws})
}

// hijacker reaches the connection through the gateway's response recorder:
// gorilla wants an http.Hijacker.
type hijacker struct{ http.ResponseWriter }

func (h hijacker) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return http.NewResponseController(h.ResponseWriter).Hijack()
}

// wsConn is a WebSocket as the stream the broker reads MQTT packets from
// (after mochi's own listener).
type wsConn struct {
	net.Conn
	ws *websocket.Conn
	r  io.Reader // the message being read
}

func (c *wsConn) Read(p []byte) (int, error) {
	for {
		if c.r == nil {
			kind, r, err := c.ws.NextReader()
			if err != nil {
				return 0, err
			}
			if kind != websocket.BinaryMessage {
				return 0, errors.New("MQTT over WebSocket takes binary messages")
			}
			c.r = r
		}
		n, err := c.r.Read(p)
		if err == io.EOF {
			c.r = nil
			if n == 0 {
				continue
			}
			err = nil
		}
		return n, err
	}
}

func (c *wsConn) Write(p []byte) (int, error) {
	if err := c.ws.WriteMessage(websocket.BinaryMessage, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

// ---- publish and retained messages ----

func (d *Data) publish(c *core.Ctx, r *http.Request) (any, error) {
	topic, q := r.PathValue("topic"), r.URL.Query()
	qos, _ := strconv.Atoi(q.Get("qos"))
	if topic == "" || strings.ContainsAny(topic, "+#") || qos < 0 || qos > 1 {
		return nil, invalid("Invalid topic %q or qos %q.", topic, q.Get("qos"))
	}
	return nil, d.s.mq.Publish(topic, core.Body(r), q.Get("retain") == "true", byte(qos))
}

func (d *Data) getRetained(c *core.Ctx, r *http.Request) (any, error) {
	pk, ok := d.s.mq.Topics.Retained.Get(r.PathValue("topic"))
	if !ok {
		return nil, notFound("No retained message exists for topic %s.", r.PathValue("topic"))
	}
	return map[string]any{"topic": pk.TopicName, "payload": pk.Payload, "qos": pk.FixedHeader.Qos, "lastModifiedTime": pk.Created * 1000}, nil
}

func (d *Data) listRetained(c *core.Ctx, r *http.Request) (any, error) {
	type summary struct {
		Topic            string `json:"topic"`
		PayloadSize      int    `json:"payloadSize"`
		Qos              byte   `json:"qos"`
		LastModifiedTime int64  `json:"lastModifiedTime"`
	}
	var all []summary
	for topic, pk := range d.s.mq.Topics.Retained.GetAll() {
		if !strings.HasPrefix(topic, "$SYS/") {
			all = append(all, summary{topic, len(pk.Payload), pk.FixedHeader.Qos, pk.Created * 1000})
		}
	}
	q := r.URL.Query()
	items, next := page(all, func(s summary) string { return s.Topic }, q.Get("nextToken"), q.Get("maxResults"))
	return map[string]any{"retainedTopics": items, "nextToken": next}, nil
}

// ---- device shadows ----

// Shadow is a device shadow.
type Shadow struct {
	State     map[string]map[string]any `json:"state"`    // desired, reported
	Metadata  map[string]map[string]any `json:"metadata"` // when each field changed
	Version   int64                     `json:"version"`
	Timestamp int64                     `json:"timestamp"`
}

// doc is the shadow document, with its delta; lock held.
func (sh *Shadow) doc() map[string]any {
	state := map[string]any{}
	for k, v := range sh.State {
		state[k] = v
	}
	if d := delta(sh.State["desired"], sh.State["reported"]); len(d) > 0 {
		state["delta"] = d
	}
	return map[string]any{"state": state, "metadata": sh.Metadata, "version": sh.Version, "timestamp": sh.Timestamp}
}

// shadowErr is a refused shadow request, answered on /rejected over MQTT.
type shadowErr struct {
	code int
	msg  string
}

func (e *shadowErr) Error() string { return e.msg }

// api is the error for the HTTP API.
func api(err error) error {
	var e *shadowErr
	if !errors.As(err, &e) {
		return err
	}
	code := map[int]string{400: "InvalidRequestException", 404: "ResourceNotFoundException", 409: "ConflictException"}[e.code]
	return core.Errorf(e.code, code, "%s", e.msg)
}

// shadowTopic parses a shadow request topic:
// $aws/things/<thing>/shadow[/name/<name>]/{update,get,delete}.
func shadowTopic(topic string) (thing, name, op string, ok bool) {
	rest, ok := strings.CutPrefix(topic, "$aws/things/")
	if !ok {
		return "", "", "", false
	}
	if thing, op, ok = strings.Cut(rest, "/shadow/"); !ok || thing == "" || strings.Contains(thing, "/") {
		return "", "", "", false
	}
	if named, ok := strings.CutPrefix(op, "name/"); ok {
		if name, op, _ = strings.Cut(named, "/"); name == "" {
			return "", "", "", false
		}
	}
	return thing, name, op, op == "update" || op == "get" || op == "delete"
}

// shadowPrefix is the topic prefix of a shadow's messages.
func shadowPrefix(thing, name string) string {
	if name == "" {
		return "$aws/things/" + thing + "/shadow/"
	}
	return "$aws/things/" + thing + "/shadow/name/" + name + "/"
}

// shadowRequest answers a shadow request published to the broker on its
// /accepted or /rejected topic.
func (s *Service) shadowRequest(thing, name, op string, payload []byte) {
	var req struct {
		ClientToken string `json:"clientToken"`
	}
	json.Unmarshal(payload, &req)
	var out []byte
	var err error
	switch op {
	case "update":
		_, err = s.updateShadow(thing, name, payload)
	case "get":
		out, err = s.shadowDoc(thing, name, req.ClientToken)
	case "delete":
		_, err = s.removeShadow(thing, name, req.ClientToken)
	}
	var e *shadowErr
	switch {
	case errors.As(err, &e):
		rej := map[string]any{"code": e.code, "message": e.msg, "timestamp": time.Now().Unix()}
		if req.ClientToken != "" {
			rej["clientToken"] = req.ClientToken
		}
		b, _ := json.Marshal(rej)
		s.mq.Publish(shadowPrefix(thing, name)+op+"/rejected", b, false, 0)
	case op == "get":
		s.mq.Publish(shadowPrefix(thing, name)+"get/accepted", out, false, 0)
	}
}

func (s *Service) shadowDoc(thing, name, token string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sh := s.st.Shadows[thing+"/"+name]
	if sh == nil {
		return nil, &shadowErr{404, "No shadow exists with name: '" + strings.TrimSuffix(thing+"~"+name, "~") + "'"}
	}
	doc := sh.doc()
	if token != "" {
		doc["clientToken"] = token
	}
	return json.Marshal(doc)
}

// updateShadow applies an update document and publishes its accepted, delta
// and documents messages; it returns the accepted document.
func (s *Service) updateShadow(thing, name string, payload []byte) ([]byte, error) {
	var req map[string]any
	if json.Unmarshal(payload, &req) != nil {
		return nil, &shadowErr{400, "Payload contains invalid json"}
	}
	state, ok := req["state"].(map[string]any)
	if !ok {
		return nil, &shadowErr{400, "Missing required node: state"}
	}
	for k, v := range state {
		if _, obj := v.(map[string]any); k != "desired" && k != "reported" || v != nil && !obj {
			return nil, &shadowErr{400, "State contains an invalid node: '" + k + "'"}
		}
	}
	token, _ := req["clientToken"].(string)
	now := time.Now().Unix()

	s.mu.Lock()
	sh := s.st.Shadows[thing+"/"+name]
	previous := []byte("null")
	if sh == nil {
		sh = &Shadow{State: map[string]map[string]any{}, Metadata: map[string]map[string]any{}}
	} else {
		previous, _ = json.Marshal(sh)
	}
	if v, ok := req["version"].(float64); ok && int64(v) != sh.Version {
		s.mu.Unlock()
		return nil, &shadowErr{409, "Version conflict"}
	}
	meta := map[string]any{}
	for section, v := range state {
		upd, _ := v.(map[string]any)
		meta[section] = stamp(upd, now)
		if upd == nil {
			delete(sh.State, section)
			delete(sh.Metadata, section)
			continue
		}
		if sh.State[section] == nil {
			sh.State[section], sh.Metadata[section] = map[string]any{}, map[string]any{}
		}
		merge(sh.State[section], sh.Metadata[section], upd, now)
	}
	sh.Version, sh.Timestamp = sh.Version+1, now
	s.st.Shadows[thing+"/"+name] = sh
	msg := func(m map[string]any) []byte {
		if token != "" {
			m["clientToken"] = token
		}
		b, _ := json.Marshal(m)
		return b
	}
	accepted := msg(map[string]any{"state": state, "metadata": meta, "version": sh.Version, "timestamp": now})
	var deltaMsg []byte
	if _, ok := state["desired"].(map[string]any); ok {
		if d := delta(sh.State["desired"], sh.State["reported"]); len(d) > 0 {
			dm := map[string]any{}
			for k := range d {
				dm[k] = sh.Metadata["desired"][k]
			}
			deltaMsg = msg(map[string]any{"state": d, "metadata": dm, "version": sh.Version, "timestamp": now})
		}
	}
	current, _ := json.Marshal(sh)
	documents := msg(map[string]any{"previous": json.RawMessage(previous), "current": json.RawMessage(current), "timestamp": now})
	s.mu.Unlock()

	prefix := shadowPrefix(thing, name) + "update/"
	s.mq.Publish(prefix+"accepted", accepted, false, 0)
	if deltaMsg != nil {
		s.mq.Publish(prefix+"delta", deltaMsg, false, 0)
	}
	s.mq.Publish(prefix+"documents", documents, false, 0)
	return accepted, nil
}

// merge applies an update to a shadow section: null deletes a field, an
// object merges into the one there, anything else replaces it. meta mirrors
// the section with the time each field changed.
func merge(dst, meta, upd map[string]any, now int64) {
	for k, v := range upd {
		switch v := v.(type) {
		case nil:
			delete(dst, k)
			delete(meta, k)
		case map[string]any:
			d, ok := dst[k].(map[string]any)
			if !ok {
				d = map[string]any{}
				dst[k] = d
			}
			m, ok := meta[k].(map[string]any)
			if _, leaf := m["timestamp"]; !ok || leaf {
				m = map[string]any{}
				meta[k] = m
			}
			merge(d, m, v, now)
		default:
			dst[k] = v
			meta[k] = map[string]any{"timestamp": now}
		}
	}
}

// stamp is the metadata of an update: when each field it names changed.
func stamp(upd map[string]any, now int64) map[string]any {
	out := map[string]any{}
	for k, v := range upd {
		if obj, ok := v.(map[string]any); ok {
			out[k] = stamp(obj, now)
		} else {
			out[k] = map[string]any{"timestamp": now}
		}
	}
	return out
}

// delta is what desired asks for that reported does not have.
func delta(desired, reported map[string]any) map[string]any {
	out := map[string]any{}
	for k, d := range desired {
		dm, dObj := d.(map[string]any)
		rm, rObj := reported[k].(map[string]any)
		if dObj && rObj {
			if sub := delta(dm, rm); len(sub) > 0 {
				out[k] = sub
			}
			continue
		}
		if r, ok := reported[k]; !ok || !reflect.DeepEqual(d, r) {
			out[k] = d
		}
	}
	return out
}

// removeShadow deletes a shadow and publishes /delete/accepted.
func (s *Service) removeShadow(thing, name, token string) ([]byte, error) {
	s.mu.Lock()
	sh := s.st.Shadows[thing+"/"+name]
	delete(s.st.Shadows, thing+"/"+name)
	s.mu.Unlock()
	if sh == nil {
		return nil, &shadowErr{404, "No shadow exists with name: '" + strings.TrimSuffix(thing+"~"+name, "~") + "'"}
	}
	out := map[string]any{"version": sh.Version, "timestamp": time.Now().Unix()}
	if token != "" {
		out["clientToken"] = token
	}
	b, _ := json.Marshal(out)
	s.mq.Publish(shadowPrefix(thing, name)+"delete/accepted", b, false, 0)
	return b, nil
}

func (d *Data) getShadow(c *core.Ctx, r *http.Request) (any, error) {
	b, err := d.s.shadowDoc(r.PathValue("thing"), r.URL.Query().Get("name"), "")
	return json.RawMessage(b), api(err)
}

func (d *Data) updateShadow(c *core.Ctx, r *http.Request) (any, error) {
	b, err := d.s.updateShadow(r.PathValue("thing"), r.URL.Query().Get("name"), core.Body(r))
	return json.RawMessage(b), api(err)
}

func (d *Data) deleteShadow(c *core.Ctx, r *http.Request) (any, error) {
	b, err := d.s.removeShadow(r.PathValue("thing"), r.URL.Query().Get("name"), "")
	return json.RawMessage(b), api(err)
}

func (d *Data) listShadows(c *core.Ctx, r *http.Request) (any, error) {
	thing, q := r.PathValue("thing"), r.URL.Query()
	var names []string
	d.s.mu.Lock()
	for k := range d.s.st.Shadows {
		if name, ok := strings.CutPrefix(k, thing+"/"); ok && name != "" {
			names = append(names, name)
		}
	}
	d.s.mu.Unlock()
	items, next := page(names, func(n string) string { return n }, q.Get("nextToken"), q.Get("pageSize"))
	return map[string]any{"results": items, "nextToken": next, "timestamp": time.Now().Unix()}, nil
}
