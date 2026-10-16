// Package acr emulates Azure Container Registry: the management plane of
// Microsoft.ContainerRegistry/registries (an arm.Provider) and the sign-in
// endpoints of `az acr login`. The registry itself is ECR's OCI endpoint
// (internal/aws/ecr, core.ImageRegistry): a registry claims the repository
// names under its own name, and is reached at
// <name>.azurecr.localhost:<ECR port>, or at localhost:<ECR port>/<name>/...
// Docker accepts either. A push creates its repository, as in Azure.
//
// ponytail: the registry takes any credentials, anonymous ones included; the
// admin user, tokens and exchanged refresh tokens are issued but not checked.
// Without the ECR service the registries are metadata only.
package acr

import (
	"cmp"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

const typ = "Microsoft.ContainerRegistry/registries"

const hostSuffix = ".azurecr.localhost"

type state struct {
	ARM  armres.Store         `json:"arm"`
	Pass map[string][2]string `json:"pass"` // admin passwords by lower-cased registry ID
}

// Service is the Container Registry emulator.
type Service struct {
	hub *core.Hub
	mu  sync.Mutex
	st  state
}

func New(h *core.Hub) *Service {
	s := &Service{hub: h}
	s.Reset()
	arm.Register(h, armres.Stored{Type: typ, Async: true, Mu: &s.mu, St: &s.st.ARM, Put: s.put, Delete: s.delete, View: s.view,
		Actions: map[string]func(*core.Ctx, arm.ID, map[string]any) (any, error){
			"listCredentials": s.listCredentials, "regenerateCredential": s.regenerateCredential,
		}}.Provider())
	arm.Register(h, arm.Provider{Type: typ + "/listUsages", List: s.listUsages})
	arm.Register(h, arm.Provider{Type: "Microsoft.ContainerRegistry", Actions: map[string]func(*core.Ctx, arm.ID, map[string]any) (any, error){
		"checkNameAvailability": s.checkName}})
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "azacr", Cloud: core.Azure, Title: "Container Registry", Protocol: "azure-json", Match: match}
}

// match claims the token endpoints of a registry's host; its registry paths, /v2/, are ECR's.
func match(r *http.Request) bool {
	h, _, err := net.SplitHostPort(r.Host)
	if err != nil {
		h = r.Host
	}
	return strings.HasSuffix(strings.ToLower(h), hostSuffix) && strings.HasPrefix(r.URL.Path, "/oauth2/")
}

var nameRe = regexp.MustCompile(`^[a-zA-Z0-9]{5,50}$`)

func (s *Service) claim(id arm.ID) {
	if r := core.Registry(s.hub); r != nil {
		r.Claim(strings.ToLower(id.Name()))
	}
}

func (s *Service) release(name string) {
	if r := core.Registry(s.hub); r != nil {
		r.Release(strings.ToLower(name))
	}
}

func pass() string {
	b := make([]byte, 24)
	rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

// put validates the registry and gives it admin passwords and a claim on its names in the registry.
func (s *Service) put(c *core.Ctx, id arm.ID, r map[string]any) error {
	if !nameRe.MatchString(id.Name()) {
		return core.Errorf(http.StatusBadRequest, "InvalidParameter", "The registry name '%s' is invalid: it must be 5 to 50 alphanumeric characters.", id.Name())
	}
	if l, _ := r["location"].(string); l == "" {
		return core.Errorf(http.StatusBadRequest, "InvalidParameter", "The location property is required for this definition.")
	}
	armres.Defaults(r, map[string]any{"sku": map[string]any{"name": "Basic", "tier": "Basic"}})
	if sku, ok := r["sku"].(map[string]any); ok {
		armres.Defaults(sku, map[string]any{"tier": sku["name"]})
	}
	p := armres.Props(r)
	delete(p, "loginServer")
	delete(p, "provisioningState")
	s.mu.Lock()
	defer s.mu.Unlock()
	k := strings.ToLower(id.String())
	if old, ok := s.st.ARM.Get(id); ok {
		p["creationDate"] = armres.Props(old)["creationDate"]
	} else {
		p["creationDate"] = time.Now().UTC().Format(time.RFC3339)
		s.st.Pass[k] = [2]string{pass(), pass()}
	}
	armres.Defaults(p, map[string]any{"adminUserEnabled": false, "publicNetworkAccess": "Enabled", "zoneRedundancy": "Disabled",
		"dataEndpointEnabled": false, "anonymousPullEnabled": false, "networkRuleBypassOptions": "AzureServices"})
	s.claim(id)
	return nil
}

func (s *Service) delete(c *core.Ctx, id arm.ID) {
	s.mu.Lock()
	delete(s.st.Pass, strings.ToLower(id.String()))
	s.mu.Unlock()
	s.release(id.Name())
}

// loginServer is the registry's host as clients reach it: on the registry port when there is one.
func (s *Service) loginServer(c *core.Ctx, name string) string {
	if p := s.hub.Cfg.ECRPort; p > 0 {
		return strings.ToLower(name) + hostSuffix + ":" + strconv.Itoa(p)
	}
	_, p, _ := net.SplitHostPort(cmp.Or(c.Host, armres.HostOf(s.hub.URL)))
	return strings.ToLower(name) + hostSuffix + ":" + cmp.Or(p, "7644")
}

func (s *Service) view(c *core.Ctx, r map[string]any) map[string]any {
	p := armres.Props(r)
	p["loginServer"], p["provisioningState"] = s.loginServer(c, armres.Str(r, "name")), "Succeeded"
	p["status"] = map[string]any{"displayStatus": "Online", "message": "The registry is ready.", "timestamp": p["creationDate"]}
	return r
}

func (s *Service) checkName(c *core.Ctx, id arm.ID, body map[string]any) (any, error) {
	name := armres.Str(body, "name")
	if !nameRe.MatchString(name) {
		return map[string]any{"nameAvailable": false, "reason": "Invalid", "message": "The registry name must be 5 to 50 alphanumeric characters."}, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for k := range s.st.ARM {
		if strings.HasSuffix(k, "/registries/"+strings.ToLower(name)) {
			return map[string]any{"nameAvailable": false, "reason": "AlreadyExists", "message": "The registry " + name + " is already in use."}, nil
		}
	}
	return map[string]any{"nameAvailable": true}, nil
}

// ---- credentials ----

func (s *Service) passwords(id arm.ID) ([2]string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.st.Pass[strings.ToLower(id.String())]
	return p, ok
}

func (s *Service) listCredentials(c *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
	p, ok := s.passwords(id)
	if !ok {
		return nil, arm.NotFound(id)
	}
	return map[string]any{"username": id.Name(), "passwords": []any{
		map[string]any{"name": "password", "value": p[0]}, map[string]any{"name": "password2", "value": p[1]}}}, nil
}

func (s *Service) regenerateCredential(c *core.Ctx, id arm.ID, body map[string]any) (any, error) {
	which := armres.Str(body, "name")
	if which != "password" && which != "password2" {
		return nil, core.Errorf(http.StatusBadRequest, "InvalidParameter", "The credential name must be 'password' or 'password2'.")
	}
	s.mu.Lock()
	k := strings.ToLower(id.String())
	p := s.st.Pass[k]
	p[map[string]int{"password": 0, "password2": 1}[which]] = pass()
	s.st.Pass[k] = p
	s.mu.Unlock()
	return s.listCredentials(c, id, nil)
}

// listUsages is a GET of .../registries/<name>/listUsages: the usage is the number of images, where Azure counts bytes.
func (s *Service) listUsages(c *core.Ctx, parent arm.ID) ([]map[string]any, error) {
	n := 0
	if r := core.Registry(s.hub); r != nil {
		n = len(r.Images(parent.Name()))
	}
	return []map[string]any{{"name": "Size", "limit": 10737418240, "currentValue": n, "unit": "Bytes"},
		{"name": "Webhooks", "limit": 2, "currentValue": 0, "unit": "Count"}}, nil
}

// ---- sign-in: az acr login ----

// ServeHTTP answers POST /oauth2/exchange (an Entra token for a refresh token) and /oauth2/token (a refresh token
// or the admin password for an access token). They are opaque: the registry does not check them.
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	c.Op = strings.TrimPrefix(r.URL.Path, "/oauth2/")
	if err := c.Intercept(); err != nil {
		e := core.AsError(err)
		http.Error(w, e.Msg, e.Status)
		return
	}
	r.ParseForm()
	name, _, _ := strings.Cut(strings.ToLower(r.Host), ".")
	var out map[string]string
	switch c.Op {
	case "exchange":
		out = map[string]string{"refresh_token": "pogi-acr." + name + "." + pass()}
	case "token":
		out = map[string]string{"access_token": "pogi-acr." + name + "." + pass()}
	default:
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

// ---- state and console ----

func (s *Service) Reset() {
	s.mu.Lock()
	for k := range s.st.ARM {
		if id, ok := arm.ParseID(k); ok {
			s.release(id.Name())
		}
	}
	s.st = state{Pass: map[string][2]string{}}
	s.mu.Unlock()
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
	if st.Pass == nil {
		st.Pass = map[string][2]string{}
	}
	s.mu.Lock()
	s.st = st
	for k := range st.ARM {
		if id, ok := arm.ParseID(k); ok {
			s.claim(id)
		}
	}
	s.mu.Unlock()
	return nil
}

func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := core.Table{Name: "Registries", Columns: []string{"Name", "Login server", "SKU", "Images"}}
	for _, k := range core.SortedKeys(s.st.ARM) {
		r := s.st.ARM[k]
		sku, _ := r["sku"].(map[string]any)
		n := 0
		if reg := core.Registry(s.hub); reg != nil {
			n = len(reg.Images(armres.Str(r, "name")))
		}
		t.Rows = append(t.Rows, []string{armres.Str(r, "name"), s.loginServer(&core.Ctx{Hub: s.hub}, armres.Str(r, "name")), armres.Str(sku, "name"), strconv.Itoa(n)})
	}
	return []core.Table{t}
}

// Inspect shows a registry with the images pushed to it.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.st.ARM {
		if strings.EqualFold(armres.Str(r, "name"), id) {
			out := armres.Clone(r)
			if reg := core.Registry(s.hub); reg != nil {
				out["images"] = reg.Images(id)
			}
			return out
		}
	}
	return nil
}
