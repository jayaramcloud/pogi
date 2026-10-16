// Package armres is what the resource-manager Providers of the Azure
// services share: a store of resources as ARM returns them, and helpers for
// the endpoints, keys and in-process calls that Providers need.
package armres

import (
	"bytes"
	"crypto/sha512"
	"encoding/base64"
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/core"
)

// Store holds the ARM bodies of one service's resources by lower-cased ID.
// It has no lock: it is a field of the service's persisted state, and the
// service's own mutex guards it. Its zero value is ready to use.
type Store map[string]map[string]any

func key(id arm.ID) string { return strings.ToLower(id.String()) }

// Put stores a resource.
func (s *Store) Put(id arm.ID, r map[string]any) {
	if *s == nil {
		*s = Store{}
	}
	(*s)[key(id)] = r
}

// Get returns a copy of a resource.
func (s Store) Get(id arm.ID) (map[string]any, bool) {
	r, ok := s[key(id)]
	return Clone(r), ok
}

// Delete removes a resource and everything below it.
func (s Store) Delete(id arm.ID) {
	k := key(id)
	for name := range s {
		if name == k || strings.HasPrefix(name, k+"/") {
			delete(s, name)
		}
	}
}

// List returns copies of the resources of type typ directly under parent: a
// resource group, a subscription (ResourceGroup == "") or a parent resource.
func (s Store) List(parent arm.ID, typ string) []map[string]any {
	var out []map[string]any
	for _, k := range slices.Sorted(maps.Keys(s)) {
		id, ok := arm.ParseID(k)
		if !ok || !strings.EqualFold(id.Type(), typ) || !strings.EqualFold(id.Subscription, parent.Subscription) {
			continue
		}
		if parent.ResourceGroup != "" && !strings.EqualFold(id.Parent().String(), parent.String()) {
			continue
		}
		out = append(out, Clone(s[k]))
	}
	return out
}

// Clone deep-copies a JSON object.
func Clone(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	b, _ := json.Marshal(m)
	var out map[string]any
	json.Unmarshal(b, &out)
	return out
}

// Props returns body["properties"], never nil.
func Props(body map[string]any) map[string]any {
	if p, ok := body["properties"].(map[string]any); ok {
		return p
	}
	return map[string]any{}
}

// Defaults sets the keys of def that m lacks, and returns m.
func Defaults(m, def map[string]any) map[string]any {
	for k, v := range def {
		if cur, ok := m[k]; !ok || cur == nil {
			m[k] = v
		}
	}
	return m
}

// Envelope builds the stored resource of a PUT: the request body (sku, kind,
// identity, ...) with id, name and type set; top-level resources also get a
// location and tags. Properties are the body's.
func Envelope(id arm.ID, body map[string]any) map[string]any {
	r := Clone(body)
	if r == nil {
		r = map[string]any{}
	}
	r["id"], r["name"], r["type"] = id.String(), id.Name(), id.Type()
	if len(id.Types) == 1 {
		if s, _ := r["location"].(string); s == "" {
			r["location"] = "eastus"
		}
		if r["tags"] == nil {
			r["tags"] = map[string]any{}
		}
	}
	r["properties"] = Props(r)
	return r
}

// Str is m[k] as a string.
func Str(m map[string]any, k string) string { s, _ := m[k].(string); return s }

// Key is a deterministic 64-byte access key (base64) for a seed such as
// "storage/key1/<account>"; Providers use it where Azure would store a random key.
func Key(seed string) string {
	sum := sha512.Sum512([]byte("pogi:" + seed))
	return base64.StdEncoding.EncodeToString(sum[:])
}

// Send calls a service in-process, the way seeding does, with a bearer token
// (Key Vault wants one). host overrides the Host header when not empty; hdr
// are more headers, name then value.
func Send(h *core.Hub, svc, method, host, path string, body any, hdr ...string) (int, []byte) {
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, "http://localhost"+path, r)
	if host != "" {
		req.Host = host
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer pogi")
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	return h.DoService(svc, req)
}

// ServiceProvider serves the one "default" service of a storage account (blob,
// file, queue and table services): it always exists, and settings written to
// it are kept in st, guarded by mu. defaults fills the properties Azure returns.
func ServiceProvider(typ string, defaults func(map[string]any) map[string]any, mu sync.Locker, st *Store) arm.Provider {
	view := func(id arm.ID, stored map[string]any) map[string]any {
		r := Envelope(id, stored)
		r["properties"] = defaults(Props(r))
		if r["sku"] == nil && strings.HasSuffix(typ, "Services") { // the blob, file, queue and table services of an account
			r["sku"] = map[string]any{"name": "Standard_LRS", "tier": "Standard"}
		}
		return r
	}
	return arm.Provider{Type: typ,
		Put: func(c *core.Ctx, id arm.ID, body map[string]any) (map[string]any, error) {
			mu.Lock()
			defer mu.Unlock()
			st.Put(id, Envelope(id, body))
			return view(id, body), nil
		},
		Get: func(c *core.Ctx, id arm.ID) (map[string]any, error) {
			mu.Lock()
			defer mu.Unlock()
			stored, _ := st.Get(id)
			return view(id, stored), nil
		},
		Delete: func(*core.Ctx, arm.ID) error { return nil },
		List: func(c *core.Ctx, parent arm.ID) ([]map[string]any, error) {
			return []map[string]any{view(parent.Child(typ[strings.LastIndex(typ, "/")+1:], "default"), nil)}, nil
		},
	}
}

// Hostport is "<label>.<hostname>:<port>" for a service that listens on a TCP port of its own (the
// AMQP broker is key "amqp"): the label names the namespace the client connects to.
func Hostport(h *core.Hub, key, label string, def int) string {
	port := strconv.Itoa(def)
	if a, ok := h.Endpoint(key); ok {
		if i := strings.LastIndexByte(a, ':'); i >= 0 {
			port = a[i+1:]
		}
	}
	host := h.Cfg.Hostname
	if host == "" {
		host = "localhost"
	}
	return label + "." + host + ":" + port
}
