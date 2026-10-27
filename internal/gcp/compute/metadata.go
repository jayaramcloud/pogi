package compute

// The metadata server. An instance's container reaches it on a listener of its own
// (GCE_METADATA_HOST), which is how pogi knows who asks. On pogi's port it answers at
// /_gcp/metadata/<project>/<zone>/<instance>/computeMetadata/v1/..., and at the plain
// /computeMetadata/v1/... when exactly one instance exists.

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
)

func isMetadata(p string) bool {
	return strings.HasPrefix(p, "/computeMetadata/") || strings.HasPrefix(p, "/_gcp/metadata/")
}

// mdListener is the metadata server of one instance.
type mdListener struct {
	srv  *http.Server
	port string
}

func (l *mdListener) close() {
	if l != nil {
		l.srv.Close()
	}
}

// listener returns the port of the instance's metadata server, starting it; mu is held.
func (s *Service) listener(path string) string {
	if l := s.md[path]; l != nil {
		return l.port
	}
	ln, err := net.Listen("tcp", net.JoinHostPort(s.hub.Docker().ListenAddr(), "0")) // a container calls it
	if err != nil {
		return "0"
	}
	l := &mdListener{&http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { s.metadata(w, r, path) })},
		strconv.Itoa(ln.Addr().(*net.TCPAddr).Port)}
	go l.srv.Serve(ln)
	s.md[path] = l
	return l.port
}

// lines is a value that reads as its items one per line, and as an array in JSON.
type lines []string

// metadata serves one request; key is the instance path, empty when the request does not say.
func (s *Service) metadata(w http.ResponseWriter, r *http.Request, key string) {
	p := r.URL.Path
	if rest, ok := strings.CutPrefix(p, "/_gcp/metadata/"); ok {
		parts := strings.SplitN(rest, "/", 4)
		if len(parts) < 4 {
			http.NotFound(w, r)
			return
		}
		key, p = "projects/"+parts[0]+"/zones/"+parts[1]+"/instances/"+parts[2], "/"+parts[3]
	}
	h := w.Header()
	h.Set("Metadata-Flavor", "Google")
	h.Set("Server", "Metadata Server for VM")
	text := func(code int, s string) {
		h.Set("Content-Type", "text/html; charset=UTF-8")
		w.WriteHeader(code)
		io.WriteString(w, s)
	}
	if r.Header.Get("Metadata-Flavor") != "Google" && r.Header.Get("X-Google-Metadata-Request") != "True" {
		text(http.StatusForbidden, "Missing required header \"Metadata-Flavor: Google\"")
		return
	}
	rest, ok := strings.CutPrefix(p, "/computeMetadata/v1")
	if !ok || rest != "" && rest[0] != '/' {
		http.NotFound(w, r)
		return
	}
	path := strings.Trim(rest, "/")

	s.mu.Lock()
	if key == "" {
		for k, d := range s.st.Res {
			if strings.Contains(k, "/instances/") && d["kind"] == "compute#instance" {
				if key != "" {
					s.mu.Unlock()
					text(http.StatusNotFound, "several instances: ask /_gcp/metadata/<project>/<zone>/<instance>/computeMetadata/v1/"+path)
					return
				}
				key = k
			}
		}
	}
	inst := s.st.Res[key]
	if inst == nil {
		s.mu.Unlock()
		text(http.StatusNotFound, "no such instance")
		return
	}
	if gp, ok := strings.CutPrefix(path, "instance/guest-attributes/"); ok && gp != "" && (r.Method == http.MethodPut || r.Method == http.MethodDelete) {
		if r.Method == http.MethodDelete {
			delete(s.st.Guest[key], gp)
		} else {
			b, _ := io.ReadAll(r.Body)
			if s.st.Guest[key] == nil {
				s.st.Guest[key] = map[string]string{}
			}
			s.st.Guest[key][gp] = string(b)
		}
		s.mu.Unlock()
		text(http.StatusOK, "")
		return
	}
	tree := s.mdTree(key, inst)
	s.mu.Unlock()

	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		text(http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var node any = tree
	parts := []string{}
	if path != "" {
		parts = strings.Split(path, "/")
	}
	for i, part := range parts {
		m, _ := node.(map[string]any)
		if m == nil || m[part] == nil {
			text(http.StatusNotFound, "not found")
			return
		}
		node = m[part]
		if m, ok := node.(func(url string) (any, error)); ok { // token, identity
			v, err := m(r.URL.RawQuery)
			if err != nil || i != len(parts)-1 {
				text(http.StatusBadRequest, fmt.Sprint(err))
				return
			}
			node = v
		}
	}
	q := r.URL.Query()
	asJSON := q.Get("alt") == "json" || gapi.Bool(q.Get("recursive"))
	var body []byte
	switch v := node.(type) {
	case map[string]any:
		if asJSON {
			body, _ = json.Marshal(jsonTree(v, false))
		} else {
			var names []string
			for _, k := range core.SortedKeys(v) {
				if _, dir := v[k].(map[string]any); dir {
					k += "/"
				}
				names = append(names, k)
			}
			body = []byte(strings.Join(names, "\n"))
		}
	default:
		if asJSON || isJSON(v) {
			body, _ = json.Marshal(jsonTree(v, false))
		} else {
			body = []byte(plain(v))
		}
	}
	if asJSON || isJSON(node) {
		h.Set("Content-Type", "application/json")
	} else {
		h.Set("Content-Type", "application/text")
	}
	sum := md5.Sum(body)
	h.Set("ETag", hex.EncodeToString(sum[:]))
	w.Write(body)
}

// isJSON: tags and the token read as JSON, everything else as text.
func isJSON(v any) bool {
	switch v.(type) {
	case []any, tokenDoc:
		return true
	}
	return false
}

type tokenDoc map[string]any

func plain(v any) string {
	switch x := v.(type) {
	case lines:
		return strings.Join(x, "\n")
	case string:
		return x
	}
	return fmt.Sprint(v)
}

// jsonTree is the JSON form of a subtree: the hyphenated names become camelCase, and a directory whose names
// are all numbers becomes an array. Attribute names are the user's, and stay.
func jsonTree(v any, keep bool) any {
	switch x := v.(type) {
	case map[string]any:
		numbered := len(x) > 0
		for k := range x {
			if _, err := strconv.Atoi(k); err != nil {
				numbered = false
			}
		}
		if numbered && !keep {
			out := make([]any, len(x))
			for k, e := range x {
				i, _ := strconv.Atoi(k)
				if i < len(out) {
					out[i] = jsonTree(e, false)
				}
			}
			return out
		}
		out := map[string]any{}
		for k, e := range x {
			if _, fn := e.(func(string) (any, error)); fn {
				continue // token and identity are not part of the JSON view
			}
			name := k
			if !keep {
				name = camel(k)
			}
			out[name] = jsonTree(e, keep || k == "attributes" || k == "service-accounts")
		}
		return out
	case lines:
		return []string(x)
	}
	return v
}

func camel(s string) string {
	parts := strings.Split(s, "-")
	for i := 1; i < len(parts); i++ {
		if parts[i] != "" {
			parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
		}
	}
	return strings.Join(parts, "")
}

// mdTree is the metadata of an instance as a tree of directories (maps), values and the two computed
// leaves (functions of the query string); mu is held.
func (s *Service) mdTree(key string, inst map[string]any) map[string]any {
	p := strings.Split(key, "/") // projects, p, zones, z, instances, n
	num := strconv.FormatInt(numeric(p[1]), 10)
	attrs, common := map[string]any{}, map[string]any{}
	for _, it := range arr(obj(inst, "metadata"), "items") {
		m, _ := it.(map[string]any)
		attrs[str(m["key"])] = str(m["value"])
	}
	for _, it := range arr(obj(s.st.Proj[p[1]], "commonInstanceMetadata"), "items") {
		m, _ := it.(map[string]any)
		common[str(m["key"])] = str(m["value"])
	}
	nics := map[string]any{}
	for i, n := range arr(inst, "networkInterfaces") {
		m, _ := n.(map[string]any)
		acs := map[string]any{}
		for j, a := range arr(m, "accessConfigs") {
			ac, _ := a.(map[string]any)
			acs[strconv.Itoa(j)] = map[string]any{"external-ip": str(ac["natIP"]), "type": str(ac["type"])}
		}
		nics[strconv.Itoa(i)] = map[string]any{"ip": str(m["networkIP"]), "access-configs": acs, "mac": "42:01:0a:80:00:02",
			"network": "projects/" + num + "/networks/" + str(m["network"])[strings.LastIndexByte(str(m["network"]), '/')+1:]}
	}
	accounts := map[string]any{}
	for i, a := range arr(inst, "serviceAccounts") {
		sa, _ := a.(map[string]any)
		email, scopes := str(sa["email"]), lines{}
		for _, sc := range arr(sa, "scopes") {
			scopes = append(scopes, str(sc))
		}
		node := map[string]any{"email": email, "aliases": lines{"default"}, "scopes": scopes,
			"token": func(string) (any, error) {
				return tokenDoc{"access_token": gapi.AccessToken(email, strings.Join(scopes, ","), time.Hour), "expires_in": 3599, "token_type": "Bearer"}, nil
			},
			"identity": func(query string) (any, error) {
				aud := ""
				for _, kv := range strings.Split(query, "&") {
					if v, ok := strings.CutPrefix(kv, "audience="); ok {
						aud = v
					}
				}
				if aud == "" {
					return nil, fmt.Errorf("non-empty audience parameter required")
				}
				return gapi.OIDCToken(email, aud), nil
			}}
		if i == 0 {
			accounts["default"] = node
		}
		accounts[email] = node
	}
	tags := append([]any{}, arr(obj(inst, "tags"), "items")...)
	image := ""
	for _, d := range arr(inst, "disks") {
		if m, _ := d.(map[string]any); m != nil && m["boot"] == true {
			if im := str(s.st.Res[ref(str(m["source"]), "")]["sourceImage"]); im != "" {
				image = strings.TrimPrefix(im, api)
			}
		}
	}
	guest := map[string]any{}
	for k, v := range s.st.Guest[key] {
		ns, name, _ := strings.Cut(k, "/")
		if guest[ns] == nil {
			guest[ns] = map[string]any{}
		}
		guest[ns].(map[string]any)[name] = v
	}
	instance := map[string]any{"attributes": attrs, "cpu-platform": "Intel Broadwell", "description": str(inst["description"]),
		"hostname": p[5] + "." + p[3] + ".c." + p[1] + ".internal", "id": str(inst["id"]), "image": image,
		"machine-type":      "projects/" + num + "/machineTypes/" + str(inst["machineType"])[strings.LastIndexByte(str(inst["machineType"]), '/')+1:],
		"maintenance-event": "NONE", "name": p[5], "network-interfaces": nics, "preempted": "FALSE", "service-accounts": accounts,
		"tags": tags, "zone": "projects/" + num + "/zones/" + p[3]}
	if len(guest) > 0 {
		instance["guest-attributes"] = guest
	}
	return map[string]any{"instance": instance, "project": map[string]any{"project-id": p[1], "numeric-project-id": num, "attributes": common}}
}
