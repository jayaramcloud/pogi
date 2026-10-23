// Package aks is the Azure Resource Manager management plane of Azure
// Kubernetes Service: Microsoft.ContainerService/managedClusters and their
// agentPools. With a container engine every cluster is a real Kubernetes API
// server, a k3s container (the runner EKS uses): the PUT answers Creating and
// the cluster turns Succeeded once its node is Ready, so the SDK's poller waits
// for it. Agent pools are metadata, as EKS node groups are. Without an engine
// every cluster is Succeeded at once and has no API server behind it.
//
// listClusterUserCredential and listClusterAdminCredential answer a kubeconfig
// with a static bearer token that the cluster's API server accepts through a
// TokenReview webhook (/_azure/aks/authn/<cluster id>). The cluster's fqdn is
// host:port of the API server, not an Azure name: clients prepend https://.
//
// The HTTP surface is internal/azure/arm's; this package registers one
// arm.Provider per resource type.
package aks

import (
	"cmp"
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/nrndev/pogi/internal/aws/eks"
	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
)

const (
	clusters = "Microsoft.ContainerService/managedClusters"
	pools    = clusters + "/agentPools"
	noDocker = "Docker is not available: the API works, but no Kubernetes API server runs behind this cluster"
	// ponytail: one version, the k3s one; a requested version is accepted and reported as this one
	version = "1.35.5"
)

var nameRE = regexp.MustCompile(`^[a-zA-Z0-9]([-_a-zA-Z0-9]{0,61}[a-zA-Z0-9])?$`)

type doc = map[string]any

// cluster is a managed cluster and pogi's side of it.
type cluster struct {
	Doc       doc            `json:"doc"`   // the resource as PUT, with the defaults; its properties have no agentPoolProfiles
	Pools     map[string]doc `json:"pools"` // by lower-cased name: the pool's properties, name among them
	State     string         `json:"state"` // provisioningState
	Container string         `json:"container,omitempty"`
	Token     string         `json:"token,omitempty"`  // k3s server token, never shown
	Bearer    string         `json:"bearer,omitempty"` // the token of its kubeconfigs
	Endpoint  string         `json:"endpoint,omitempty"`
	CA        string         `json:"ca,omitempty"` // base64 of the PEM
	Note      string         `json:"pogiNote,omitempty"`
	cancel    context.CancelFunc
}

type state struct {
	Clusters map[string]*cluster `json:"clusters"` // by lower-cased resource ID
}

// Service holds the clusters.
type Service struct {
	hub  *core.Hub
	ctx  context.Context // k3s work; ends at Close
	stop context.CancelFunc
	mu   sync.Mutex
	st   state
}

func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, st: state{Clusters: map[string]*cluster{}}}
	s.ctx, s.stop = context.WithCancel(context.Background())
	arm.Register(hub, arm.Provider{Type: clusters, Put: s.putCluster, Get: s.getCluster, Delete: s.deleteCluster, List: s.listClusters, Deleted: http.StatusNoContent, // the SDK takes 202 or 204 of a delete
		Actions: map[string]func(*core.Ctx, arm.ID, map[string]any) (any, error){
			"listClusterUserCredential":           s.credentials("clusterUser"),
			"listClusterAdminCredential":          s.credentials("clusterAdmin"),
			"listClusterMonitoringUserCredential": s.credentials("clusterMonitoringUser")}})
	arm.Register(hub, arm.Provider{Type: pools, Put: s.putPool, Get: s.getPool, Delete: s.deletePool, List: s.listPools, Deleted: http.StatusNoContent})
	hub.Mount("/_azure/aks/authn/", http.HandlerFunc(s.authn))
	return s
}

func (s *Service) Info() core.Info {
	// The ARM service routes to the providers; this one has no HTTP routes.
	return core.Info{Name: "azaks", Cloud: core.Azure, Title: "Azure Kubernetes Service", Protocol: "azure-json",
		Match: func(*http.Request) bool { return false }}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) }

func key(id arm.ID) string { return strings.ToLower(id.String()) }

func object(v any) doc { d, _ := v.(map[string]any); return d }
func str(v any) string { s, _ := v.(string); return s }

func badRequest(format string, a ...any) error {
	return core.Errorf(http.StatusBadRequest, "InvalidParameter", format, a...)
}

// ---- clusters ----

func (s *Service) putCluster(c *core.Ctx, id arm.ID, body map[string]any) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(id)
	cl := s.st.Clusters[k]
	created := cl == nil
	props := object(body["properties"])
	if created {
		if !nameRE.MatchString(id.Name()) {
			return nil, badRequest("The resource name '%s' is invalid: it must be 1-63 letters, digits, hyphens and underscores, starting and ending with a letter or digit.", id.Name())
		}
		loc := str(body["location"])
		if loc == "" {
			return nil, core.Errorf(http.StatusBadRequest, "LocationRequired", "The location property is required for this definition.")
		}
		cl = &cluster{Pools: map[string]doc{}, State: "Succeeded", Bearer: "pogi-aks-" + core.RandHex(16)}
		cl.Doc = arm.Resource(id, loc, nil, doc{
			"kubernetesVersion": version, "currentKubernetesVersion": version, "dnsPrefix": cmp.Or(str(props["dnsPrefix"]), id.Name()),
			"fqdn":              strings.ToLower(cmp.Or(str(props["dnsPrefix"]), id.Name())) + "-" + core.RandHex(4) + ".hcp." + loc + ".azmk8s.io",
			"nodeResourceGroup": "MC_" + id.ResourceGroup + "_" + id.Name() + "_" + loc, "enableRBAC": true, "maxAgentPools": 100,
			"networkProfile": doc{"networkPlugin": "kubenet", "loadBalancerSku": "standard", "podCidr": "10.244.0.0/16", "serviceCidr": "10.0.0.0/16", "dnsServiceIP": "10.0.0.10"}})
		cl.Doc["sku"] = doc{"name": "Base", "tier": "Free"}
	}
	for _, f := range []string{"tags", "sku", "identity"} {
		if v, ok := body[f]; ok {
			cl.Doc[f] = v
		}
	}
	cp := object(cl.Doc["properties"])
	for k, v := range props {
		switch k {
		case "agentPoolProfiles", "provisioningState", "powerState", "fqdn", "currentKubernetesVersion", "kubernetesVersion":
		default:
			cp[k] = v
		}
	}
	if profiles, ok := props["agentPoolProfiles"].([]any); ok {
		next := map[string]doc{}
		for i, p := range profiles {
			m := object(p)
			if !poolRE.MatchString(str(m["name"])) {
				return nil, badRequest("The agent pool name '%s' is invalid: it must be 1-12 lowercase letters and digits, starting with a letter.", str(m["name"]))
			}
			next[strings.ToLower(str(m["name"]))] = newPool(cl, m, i == 0)
		}
		cl.Pools = next
	}
	if created {
		s.st.Clusters[k] = cl
		if s.hub.Docker().Available() {
			cl.Container, cl.Token = docker.Name("aks", id.ResourceGroup+"-"+id.Name()), core.RandHex(16)
			s.boot(k, cl)
		} else {
			cl.Note = noDocker
			s.hub.Log.Warn(noDocker, "service", "azaks", "cluster", id.Name())
		}
	}
	return view(cl), nil
}

var poolRE = regexp.MustCompile(`^[a-z][a-z0-9]{0,11}$`)

// newPool is an agent pool's properties with the defaults AKS adds. Its node
// count is metadata: the k3s cluster has one node whatever the pools say.
func newPool(cl *cluster, p doc, first bool) doc {
	out := maps.Clone(p)
	for k, v := range map[string]any{"count": 1, "vmSize": "Standard_DS2_v2", "osType": "Linux", "osDiskSizeGB": 128, "type": "VirtualMachineScaleSets",
		"maxPods": 110, "enableAutoScaling": false, "mode": map[bool]string{true: "System", false: "User"}[first], "orchestratorVersion": version} {
		if _, ok := out[k]; !ok {
			out[k] = v
		}
	}
	out["currentOrchestratorVersion"], out["provisioningState"], out["powerState"] = version, "Succeeded", doc{"code": "Running"}
	out["nodeImageVersion"] = "AKSUbuntu-2204gen2containerd-pogi"
	return out
}

// view is the cluster as ARM shows it.
func view(cl *cluster) doc {
	props := maps.Clone(object(cl.Doc["properties"]))
	props["provisioningState"], props["powerState"] = cl.State, doc{"code": "Running"}
	profiles := []any{}
	for _, n := range core.SortedKeys(cl.Pools) {
		profiles = append(profiles, cl.Pools[n])
	}
	props["agentPoolProfiles"] = profiles
	if cl.Note != "" {
		props["pogiNote"] = cl.Note
	}
	out := maps.Clone(cl.Doc)
	out["properties"] = props
	return out
}

// boot runs the k3s container of a cluster in the background (restart: the one
// it has) and makes the cluster Succeeded once its node is Ready. Lock held.
func (s *Service) boot(k string, cl *cluster) { s.run(k, cl, false) }

func (s *Service) run(k string, cl *cluster, restart bool) {
	ctx, cancel := context.WithCancel(s.ctx)
	cl.cancel, cl.State = cancel, "Creating"
	d := s.hub.Docker()
	port := strconv.Itoa(s.hub.Cfg.Port) // before pogi listens (Load at startup) its URL is unset
	if u, err := url.Parse(s.hub.URL); err == nil && u.Port() != "" {
		port = u.Port()
	}
	// ponytail: an engine without a VM (Linux) cannot reach pogi on 127.0.0.1; kubectl
	// then gets Unauthorized until pogi listens where the container can reach it.
	r := &eks.K3s{Name: cl.Container, Image: eks.DefaultImage(), Service: "azaks", Resource: str(cl.Doc["name"]), Token: cl.Token,
		Webhook: "http://" + d.HostAddr() + ":" + port + "/_azure/aks/authn" + k}
	r.Watch, r.OnExit = s.ctx, func(note string) {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.st.Clusters[k] == cl && cl.State == "Succeeded" {
			cl.State, cl.Note = "Failed", note
			s.hub.Log.Warn("aks cluster stopped", "cluster", k, "note", note)
		}
	}
	core.Go("aks.run", func() {
		defer cancel()
		endpoint, ca, err := r.Run(ctx, d, restart)
		s.mu.Lock()
		defer s.mu.Unlock()
		switch {
		case s.st.Clusters[k] != cl: // deleted meanwhile
			s.drop(cl)
		case err != nil:
			cl.State, cl.Note = "Failed", docker.Plain(err)
			s.hub.Log.Warn("aks cluster failed", "cluster", k, "err", err)
		default:
			cl.State, cl.Note = "Succeeded", ""
			cl.Endpoint, cl.CA = strings.TrimPrefix(endpoint, "https://"), base64.StdEncoding.EncodeToString(ca)
			object(cl.Doc["properties"])["fqdn"] = cl.Endpoint
		}
	})
}

// drop stops the k3s work of a cluster that is gone and removes its container,
// unless a current cluster runs in it. Lock held.
func (s *Service) drop(cl *cluster) {
	if cl.cancel != nil {
		cl.cancel()
	}
	for _, x := range s.st.Clusters {
		if x.Container == cl.Container {
			return
		}
	}
	if cl.Container != "" {
		go s.hub.Docker().Remove(context.Background(), cl.Container)
	}
}

func (s *Service) getCluster(_ *core.Ctx, id arm.ID) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cl := s.st.Clusters[key(id)]
	if cl == nil {
		return nil, arm.NotFound(id)
	}
	return view(cl), nil
}

func (s *Service) listClusters(_ *core.Ctx, parent arm.ID) ([]map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prefix := strings.ToLower(parent.String()) + "/"
	var out []map[string]any
	for _, k := range core.SortedKeys(s.st.Clusters) {
		if strings.HasPrefix(k, prefix) {
			out = append(out, view(s.st.Clusters[k]))
		}
	}
	return out, nil
}

func (s *Service) deleteCluster(_ *core.Ctx, id arm.ID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cl := s.st.Clusters[key(id)]; cl != nil {
		delete(s.st.Clusters, key(id))
		s.drop(cl)
	}
	return nil
}

// ---- agent pools ----

func poolView(id arm.ID, p doc) doc {
	props := maps.Clone(p)
	delete(props, "name")
	return doc{"id": id.String(), "name": id.Name(), "type": pools, "properties": props}
}

func (s *Service) putPool(_ *core.Ctx, id arm.ID, body map[string]any) (map[string]any, error) {
	if !poolRE.MatchString(id.Name()) {
		return nil, badRequest("The agent pool name '%s' is invalid: it must be 1-12 lowercase letters and digits, starting with a letter.", id.Name())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cl := s.st.Clusters[key(id.Parent())]
	if cl == nil {
		return nil, arm.NotFound(id.Parent())
	}
	p := maps.Clone(cl.Pools[strings.ToLower(id.Name())])
	if p == nil {
		p = doc{}
	}
	maps.Copy(p, object(body["properties"]))
	p["name"] = id.Name()
	cl.Pools[strings.ToLower(id.Name())] = newPool(cl, p, len(cl.Pools) == 0)
	return poolView(id, cl.Pools[strings.ToLower(id.Name())]), nil
}

func (s *Service) getPool(_ *core.Ctx, id arm.ID) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cl := s.st.Clusters[key(id.Parent())]; cl != nil {
		if p := cl.Pools[strings.ToLower(id.Name())]; p != nil {
			return poolView(id, p), nil
		}
	}
	return nil, arm.NotFound(id)
}

func (s *Service) listPools(_ *core.Ctx, parent arm.ID) ([]map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cl := s.st.Clusters[key(parent)]
	if cl == nil {
		return nil, arm.NotFound(parent)
	}
	var out []map[string]any
	for _, n := range core.SortedKeys(cl.Pools) {
		out = append(out, poolView(parent.Child("agentPools", str(cl.Pools[n]["name"])), cl.Pools[n]))
	}
	return out, nil
}

func (s *Service) deletePool(_ *core.Ctx, id arm.ID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cl := s.st.Clusters[key(id.Parent())]; cl != nil {
		delete(cl.Pools, strings.ToLower(id.Name()))
	}
	return nil
}

// ---- credentials ----

// credentials answers the kubeconfigs of listCluster<kind>Credential. The user and
// admin ones are the same here: every holder of the token is cluster-admin.
// ponytail: no Entra tokens (kubelogin): a static bearer token; check Entra JWTs in the webhook to support them.
func (s *Service) credentials(kind string) func(*core.Ctx, arm.ID, map[string]any) (any, error) {
	return func(_ *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		cl := s.st.Clusters[key(id)]
		if cl == nil {
			return nil, arm.NotFound(id)
		}
		server := "https://" + cmp.Or(cl.Endpoint, str(object(cl.Doc["properties"])["fqdn"]))
		ca := ""
		if cl.CA != "" {
			ca = "    certificate-authority-data: " + cl.CA + "\n"
		}
		user := kind + "_" + id.ResourceGroup + "_" + id.Name()
		cfg := fmt.Sprintf("apiVersion: v1\nclusters:\n- cluster:\n%[1]s    server: %[2]s\n  name: %[3]s\ncontexts:\n- context:\n    cluster: %[3]s\n    user: %[4]s\n  name: %[3]s\n"+
			"current-context: %[3]s\nkind: Config\npreferences: {}\nusers:\n- name: %[4]s\n  user:\n    token: %[5]s\n", ca, server, id.Name(), user, cl.Bearer)
		return doc{"kubeconfigs": []any{doc{"name": kind, "value": base64.StdEncoding.EncodeToString([]byte(cfg))}}}, nil
	}
}

// authn is the TokenReview webhook of the clusters' API servers, at
// /_azure/aks/authn/<lower-cased cluster id>: it takes the cluster's own bearer token.
func (s *Service) authn(w http.ResponseWriter, r *http.Request) {
	var tr struct {
		APIVersion string `json:"apiVersion"`
		Spec       struct{ Token string }
	}
	json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&tr)
	k := "/" + strings.TrimPrefix(r.URL.Path, "/_azure/aks/authn/")
	s.mu.Lock()
	cl := s.st.Clusters[k]
	ok := cl != nil && cl.Bearer != "" && subtle.ConstantTimeCompare([]byte(tr.Spec.Token), []byte(cl.Bearer)) == 1
	s.mu.Unlock()
	status := doc{"authenticated": ok}
	if ok {
		status["user"] = doc{"username": "clusterUser", "uid": "clusterUser", "groups": []string{"system:masters"}}
	} else {
		status["error"] = "not the bearer token of this cluster's kubeconfig"
		s.hub.Log.Info("aks token rejected", "cluster", k)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(doc{"apiVersion": cmp.Or(tr.APIVersion, "authentication.k8s.io/v1"), "kind": "TokenReview", "status": status})
}

// ---- persistence, console, shutdown ----

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

// Load restores state. Clusters that ran on k3s start their containers again
// (persisted state leaves them behind on shutdown), or turn Failed when the
// engine or the container is gone.
func (s *Service) Load(b []byte) error {
	st := state{}
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	if st.Clusters == nil {
		st.Clusters = map[string]*cluster{}
	}
	engine := false
	for _, cl := range st.Clusters {
		if cl.Container != "" {
			engine = s.hub.Docker().Available()
			break
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	old := s.st
	s.st = st
	for k, cl := range st.Clusters {
		switch {
		case cl.Container == "":
		case engine:
			s.run(k, cl, true)
		default:
			cl.State, cl.Note = "Failed", "Docker is not available, so the k3s container of this cluster cannot start"
		}
	}
	for _, cl := range old.Clusters {
		s.drop(cl)
	}
	return nil
}

func (s *Service) Reset() { s.Load([]byte("{}")) }

// Close stops k3s work and removes the clusters' containers, unless state is
// persisted: the next run starts them again.
func (s *Service) Close() error {
	s.stop()
	if cfg := s.hub.Cfg; cfg.Persist || cfg.Storage != "" && cfg.Storage != "memory" {
		return nil
	}
	s.mu.Lock()
	var names []string
	for _, cl := range s.st.Clusters {
		if cl.Container != "" {
			names = append(names, cl.Container)
		}
	}
	s.mu.Unlock()
	for _, n := range names {
		s.hub.Docker().Remove(context.Background(), n)
	}
	return nil
}

// Resources lists the clusters for the console.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := core.Table{Name: "Clusters", Columns: []string{"Name", "State", "Version", "FQDN", "Agent pools", "Location", "Resource group"}}
	for _, k := range core.SortedKeys(s.st.Clusters) {
		cl := s.st.Clusters[k]
		id, _ := arm.ParseID(k)
		props := object(cl.Doc["properties"])
		t.Rows = append(t.Rows, []string{str(cl.Doc["name"]), cl.State, str(props["kubernetesVersion"]), str(props["fqdn"]), strconv.Itoa(len(cl.Pools)),
			str(cl.Doc["location"]), id.ResourceGroup})
	}
	return []core.Table{t}
}

// Inspect returns a cluster with pogi's side: its container and pogiNote (never its tokens).
func (s *Service) Inspect(_, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range core.SortedKeys(s.st.Clusters) {
		if cl := s.st.Clusters[k]; cl.Doc["name"] == id {
			b, _ := json.Marshal(doc{"cluster": view(cl), "container": cl.Container, "pogiNote": cl.Note})
			return json.RawMessage(b)
		}
	}
	return nil
}
