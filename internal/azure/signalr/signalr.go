// Package signalr emulates Azure SignalR Service in the default and serverless shapes that need no app
// server: clients negotiate (POST /client/negotiate?hub=<hub>), then connect over WebSocket with the
// SignalR JSON hub protocol; apps drive them through the REST API (/api/v1/hubs/<hub>/... and the
// 2022-11-01 /api/hubs/<hub>/... forms): send to everyone, a user, a connection or a group, manage
// group membership, check that connections, users and groups exist, and close connections.
//
// A resource is reached at https://<name>.service.signalr.localhost:7644, or on any host under
// /_azure/signalr/<name>. Resources appear on first use with the access key DefaultKey (SetKey changes
// it); negotiation tokens are HS256 JWTs signed with that key.
// ponytail: client invocations are not forwarded to an upstream, and only the JSON protocol runs over
// WebSockets (no SSE, long polling or MessagePack).
package signalr

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// DefaultKey is the access key of a resource nobody gave one.
const DefaultKey = "pogi-signalr-development-access-key"

// Service is the SignalR Service emulator.
type Service struct {
	mu      sync.Mutex
	st      state
	hubs    map[string]*hubState // resource/hub, lower-cased; connections are not persisted
	pending map[string]*pending  // negotiated connection token → what the connection will be
}

type state struct {
	Keys map[string]string `json:"keys"`          // resource name → access key
	ARM  armres.Store      `json:"arm,omitempty"` // resources as the resource manager has them
}

// pending is a connection that negotiated and has not connected yet.
type pending struct {
	id, user string
	expires  time.Time
}

type hubState struct {
	conns  map[string]*conn
	groups map[string]*group
}

type group struct {
	conns map[string]bool
	users map[string]bool
}

// New creates the SignalR service.
func New(h *core.Hub) *Service {
	s := &Service{}
	s.Reset()
	s.registerARM(h)
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "signalr", Cloud: core.Azure, Title: "SignalR Service", Protocol: "azure-json", Match: func(r *http.Request) bool {
		_, _, ok := target(r)
		return ok
	}}
}

// Reset clears the resources and drops every connection.
func (s *Service) Reset() {
	s.mu.Lock()
	hubs := s.hubs
	s.st, s.hubs, s.pending = state{Keys: map[string]string{}}, map[string]*hubState{}, map[string]*pending{}
	s.mu.Unlock()
	for _, h := range hubs {
		for _, c := range h.conns {
			c.close()
		}
	}
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

func (s *Service) Load(b []byte) error {
	var st state
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	if st.Keys == nil {
		st.Keys = map[string]string{}
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

// SetKey gives a resource the access key its clients' tokens are signed with.
func (s *Service) SetKey(resource, key string) {
	s.mu.Lock()
	s.st.Keys[resource] = key
	s.mu.Unlock()
}

// key is the access key of a resource, which comes to exist when first used.
func (s *Service) key(resource string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Keys[resource] == "" {
		s.st.Keys[resource] = DefaultKey
	}
	return s.st.Keys[resource]
}

// hub returns the state of a resource's hub; call with s.mu held.
func (s *Service) hub(resource, hub string) *hubState {
	k := strings.ToLower(resource + "/" + hub)
	h := s.hubs[k]
	if h == nil {
		h = &hubState{conns: map[string]*conn{}, groups: map[string]*group{}}
		s.hubs[k] = h
	}
	return h
}

// target finds the resource a request is for: the host <name>.service.signalr.localhost, or the path
// /_azure/signalr/<name>; path is what remains of the URL path, and prefix what came before it.
func target(r *http.Request) (name, path string, ok bool) {
	if rest, ok := strings.CutPrefix(r.URL.Path, "/_azure/signalr/"); ok {
		name, path, _ = strings.Cut(rest, "/")
		return name, "/" + path, name != ""
	}
	label, base := core.SplitHost(r.Host, "")
	if label != "" && (base == "service.signalr.localhost" || strings.HasSuffix(base, ".service.signalr.localhost")) {
		return label, r.URL.Path, true
	}
	// The paths of the client and server APIs on any host belong to the resource "default". They look
	// like S3 paths (buckets "client" and "api"), so SigV4 and presigned requests are S3's, and the
	// client API needs its hub parameter and the 2022 server API its api-version.
	q, p := r.URL.Query(), r.URL.Path
	if a := r.Header.Get("Authorization"); strings.HasPrefix(a, "AWS") || q.Has("X-Amz-Algorithm") || q.Has("AWSAccessKeyId") {
		return "", "", false
	}
	if (strings.HasPrefix(p, "/client/") || p == "/client") && q.Has("hub") || strings.HasPrefix(p, "/api/v1/hubs/") ||
		strings.HasPrefix(p, "/api/hubs/") && q.Has("api-version") {
		return "default", p, true
	}
	return "", "", false
}

func writeError(w http.ResponseWriter, c *core.Ctx, status int, code, msg string) {
	c.ErrCode = code
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"code": code, "message": msg})
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	name, path, _ := target(r)
	switch {
	case path == "/client/negotiate" || (r.Method == http.MethodPost && (path == "/client/" || path == "/client")):
		c.Op = "Negotiate"
		s.negotiate(w, r, c, name)
	case path == "/client/" || path == "/client":
		c.Op = "Connect"
		s.connect(w, r, c, name)
	case strings.HasPrefix(path, "/api/"):
		s.rest(w, r, c, name, path)
	default:
		writeError(w, c, http.StatusNotFound, "NotFound", "pogi serves the SignalR client and server APIs.")
	}
}

// ---- tokens ----

var b64 = base64.RawURLEncoding

func sign(key string, claims map[string]any) string {
	body, _ := json.Marshal(claims)
	msg := b64.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`)) + "." + b64.EncodeToString(body)
	m := hmac.New(sha256.New, []byte(key))
	m.Write([]byte(msg))
	return msg + "." + b64.EncodeToString(m.Sum(nil))
}

// claims returns the claims of an unexpired HS256 token signed with key.
func claims(key, token string) (map[string]any, bool) {
	p := strings.Split(token, ".")
	if len(p) != 3 {
		return nil, false
	}
	m := hmac.New(sha256.New, []byte(key))
	m.Write([]byte(p[0] + "." + p[1]))
	if sig, err := b64.DecodeString(p[2]); err != nil || !hmac.Equal(sig, m.Sum(nil)) {
		return nil, false
	}
	body, err := b64.DecodeString(p[1])
	if err != nil {
		return nil, false
	}
	var c map[string]any
	if json.Unmarshal(body, &c) != nil {
		return nil, false
	}
	if exp, ok := c["exp"].(float64); ok && time.Now().Unix() > int64(exp) {
		return nil, false
	}
	return c, true
}

// user is who a token is for: nameid, asrs.s.uid or sub.
func user(c map[string]any) string {
	for _, k := range []string{"asrs.s.uid", "nameid", "sub"} {
		if u, _ := c[k].(string); u != "" {
			return u
		}
	}
	return ""
}

func bearer(r *http.Request) string {
	if t, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer "); ok {
		return t
	}
	return r.URL.Query().Get("access_token")
}

// ---- negotiate ----

// negotiate has two steps, as in Azure: without a token it points the client at the service with an
// access token ({url, accessToken}); with one it makes the connection, as ASP.NET Core does.
func (s *Service) negotiate(w http.ResponseWriter, r *http.Request, c *core.Ctx, name string) {
	if r.Method != http.MethodPost {
		writeError(w, c, http.StatusMethodNotAllowed, "MethodNotAllowed", "Negotiate with POST.")
		return
	}
	hub := r.URL.Query().Get("hub")
	if hub == "" {
		writeError(w, c, http.StatusBadRequest, "BadRequest", "The hub query parameter is missing.")
		return
	}
	key := s.key(name)
	w.Header().Set("Content-Type", "application/json")
	if tok := bearer(r); tok != "" {
		cl, ok := claims(key, tok)
		if !ok {
			writeError(w, c, http.StatusUnauthorized, "Unauthorized", "The access token is not valid for this resource.")
			return
		}
		id, token := core.RandString(22, core.AlphaNum), core.RandString(32, core.AlphaNum)
		s.mu.Lock()
		for k, p := range s.pending { // forget connections that never came
			if time.Now().After(p.expires) {
				delete(s.pending, k)
			}
		}
		s.pending[token] = &pending{id: id, user: user(cl), expires: time.Now().Add(time.Minute)}
		s.mu.Unlock()
		json.NewEncoder(w).Encode(map[string]any{"negotiateVersion": 1, "connectionId": id, "connectionToken": token,
			"availableTransports": []map[string]any{{"transport": "WebSockets", "transferFormats": []string{"Text", "Binary"}}}})
		return
	}
	scheme, prefix := "http", ""
	if r.TLS != nil {
		scheme = "https"
	}
	if strings.HasPrefix(r.URL.Path, "/_azure/signalr/") {
		prefix = "/_azure/signalr/" + name
	}
	url := scheme + "://" + r.Host + prefix + "/client/?hub=" + hub
	claims := map[string]any{"aud": url, "iat": time.Now().Unix(), "nbf": time.Now().Unix(), "exp": time.Now().Add(time.Hour).Unix()}
	if u := r.URL.Query().Get("userId"); u != "" {
		claims["nameid"], claims["asrs.s.uid"] = u, u
	}
	json.NewEncoder(w).Encode(map[string]string{"url": url, "accessToken": sign(key, claims)})
}
