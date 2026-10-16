// Package opensearch emulates Amazon OpenSearch Service (REST-JSON,
// 2021-01-01) and the legacy Elasticsearch Service API (2015-01-01) over one
// store of domains. With Docker each domain runs one OpenSearch or
// Elasticsearch container, which answers on pogi's port for the domain's
// endpoint <name>-<id>.<region>.opensearch.localhost (.es.localhost for
// domains made through the legacy API); without Docker domains are metadata.
package opensearch

import (
	"bytes"
	"encoding/json"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/aws/rds"
	"github.com/nrndev/pogi/internal/core"
)

// Service is the OpenSearch emulator.
type Service struct {
	hub *core.Hub
	mux *http.ServeMux
	box *rds.Box
	mu  sync.Mutex
	st  state
}

type state struct {
	Domains map[string]*Domain `json:"domains"` // account/region/name
}

// Domain is a domain: its configuration as the API members in OpenSearch
// names (EngineVersion, ClusterConfig…), and pogi's server.
type Domain struct {
	Name    string         `json:"name"`
	Account string         `json:"account"`
	Region  string         `json:"region"`
	ID      string         `json:"id"`               // in the endpoint host
	Legacy  bool           `json:"legacy,omitempty"` // made through the Elasticsearch API
	Config  map[string]any `json:"config"`
	Tags    []rds.Tag      `json:"tags,omitempty"`
	Created time.Time      `json:"created"`
	Updated time.Time      `json:"updated"`
	Version int            `json:"version"`           // UpdateVersion, one more per change
	Change  string         `json:"change"`            // ChangeId of the last change
	Changed []string       `json:"changed,omitempty"` // the options it changed
	Pogi    rds.Server     `json:"pogi"`
}

const elastic = "docker.elastic.co/elasticsearch/elasticsearch:7.10.2"

// versions pogi runs, newest first, and their images. Every Elasticsearch
// version runs 7.10.2, the last open-source release. docker.images
// opensearch.opensearch and opensearch.elasticsearch in pogi.yaml override them.
// ponytail: no Elasticsearch 6.x or older; add a row with an image when asked.
var versions = []struct{ name, image string }{
	{"OpenSearch_3.1", "opensearchproject/opensearch:3.1.0"},
	{"OpenSearch_2.19", "opensearchproject/opensearch:2.19.6"},
	{"OpenSearch_2.17", "opensearchproject/opensearch:2.17.1"},
	{"OpenSearch_2.15", "opensearchproject/opensearch:2.15.0"},
	{"OpenSearch_2.13", "opensearchproject/opensearch:2.13.0"},
	{"OpenSearch_2.11", "opensearchproject/opensearch:2.11.1"},
	{"OpenSearch_2.9", "opensearchproject/opensearch:2.9.0"},
	{"OpenSearch_2.7", "opensearchproject/opensearch:2.7.0"},
	{"OpenSearch_2.5", "opensearchproject/opensearch:2.5.0"},
	{"OpenSearch_2.3", "opensearchproject/opensearch:2.3.0"},
	{"OpenSearch_1.3", "opensearchproject/opensearch:1.3.20"},
	{"OpenSearch_1.2", "opensearchproject/opensearch:1.2.4"},
	{"OpenSearch_1.1", "opensearchproject/opensearch:1.1.0"},
	{"OpenSearch_1.0", "opensearchproject/opensearch:1.0.1"},
	{"Elasticsearch_7.10", elastic}, {"Elasticsearch_7.9", elastic}, {"Elasticsearch_7.8", elastic},
	{"Elasticsearch_7.7", elastic}, {"Elasticsearch_7.4", elastic}, {"Elasticsearch_7.1", elastic},
}

// options are the configuration members a domain keeps, in OpenSearch names.
var options = []string{"EngineVersion", "ClusterConfig", "EBSOptions", "AccessPolicies", "IPAddressType", "SnapshotOptions",
	"VPCOptions", "CognitoOptions", "EncryptionAtRestOptions", "NodeToNodeEncryptionOptions", "AdvancedOptions",
	"LogPublishingOptions", "DomainEndpointOptions", "AdvancedSecurityOptions", "IdentityCenterOptions", "AutoTuneOptions",
	"OffPeakWindowOptions", "SoftwareUpdateOptions", "AIMLOptions", "DeploymentStrategyOptions"}

// health asks a container for its cluster health, waiting for yellow. The
// Box hangs up after 2 seconds without an answer, and the server holds the
// answer until the cluster is green or yellow, so the domain stays
// Processing until then.
const health = "GET /_cluster/health?wait_for_status=yellow&timeout=60s HTTP/1.0\r\n\r\n"

// New creates the OpenSearch service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, mux: http.NewServeMux(), st: state{Domains: map[string]*Domain{}}}
	s.box = rds.NewBox(hub, "opensearch", "failed", &s.mu, s.owns)
	s.routes()
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "opensearch", Cloud: core.AWS, Title: "OpenSearch", Signing: []string{"es"}, Match: isEndpoint, Protocol: "rest-json"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if isEndpoint(r) {
		s.proxy(w, r)
		return
	}
	s.mux.ServeHTTP(w, r)
}

type handler func(c *core.Ctx, r *http.Request, es bool) (any, error)

func (s *Service) route(pattern, op string, es bool, h handler) {
	s.mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		c := core.C(r)
		c.Op = op
		var out any
		err := c.Intercept()
		if err == nil {
			out, err = h(c, r, es)
		}
		if err != nil {
			core.RESTJSONError(w, c, err)
			return
		}
		core.RESTJSON(w, c, http.StatusOK, out)
	})
}

// routes serves both APIs: the same paths under another version and prefix,
// and "Elasticsearch" in some operation names.
func (s *Service) routes() {
	for _, a := range []struct{ base, kind, es string }{{"/2021-01-01", "/opensearch", ""}, {"/2015-01-01", "/es", "Elasticsearch"}} {
		d, es := a.base+a.kind+"/domain", a.es != ""
		for _, r := range []struct {
			pattern, op string
			h           handler
		}{
			{"POST " + d, "Create" + a.es + "Domain", s.create},
			{"GET " + d + "/{name}", "Describe" + a.es + "Domain", s.describe},
			{"DELETE " + d + "/{name}", "Delete" + a.es + "Domain", s.delete},
			{"POST " + d + "-info", "Describe" + a.es + "Domains", s.describeMany},
			{"GET " + d + "/{name}/config", "Describe" + a.es + "DomainConfig", s.describeConfig},
			{"POST " + d + "/{name}/config", "Update" + a.es + "DomainConfig", s.updateConfig},
			{"GET " + d + "/{name}/progress", "DescribeDomainChangeProgress", s.progress},
			{"GET " + a.base + "/domain", "ListDomainNames", s.listNames},
			{"GET " + a.base + a.kind + "/versions", "List" + a.es + "Versions", s.listVersions},
			{"GET " + a.base + a.kind + "/compatibleVersions", "GetCompatible" + a.es + "Versions", s.compatible},
			{"GET " + a.base + "/packages/{id}/domains", "ListDomainsForPackage", s.forPackage},
			{"POST " + a.base + "/tags", "AddTags", s.addTags},
			{"POST " + a.base + "/tags-removal", "RemoveTags", s.removeTags},
			{"GET " + a.base + "/tags", "ListTags", s.listTags},
			{"GET " + a.base + "/tags/{$}", "ListTags", s.listTags},
		} {
			s.route(r.pattern, r.op, es, r.h)
		}
	}
	s.route("/", "Unknown", false, func(_ *core.Ctx, r *http.Request, _ bool) (any, error) {
		return nil, core.Errorf(501, "NotImplemented", "pogi does not implement OpenSearch %s %s", r.Method, r.URL.Path)
	})
}

// ---- helpers ----

func key(account, region, name string) string { return account + "/" + region + "/" + name }

func invalid(format string, a ...any) error {
	return core.Errorf(400, "ValidationException", format, a...)
}

func notFound(name string) error {
	return core.Errorf(409, "ResourceNotFoundException", "Domain not found: %s", name)
}

// decode reads the request body into each of vs.
func decode(r *http.Request, vs ...any) error {
	b := core.Body(r)
	if len(b) == 0 {
		return nil
	}
	for _, v := range vs {
		if err := json.Unmarshal(b, v); err != nil {
			return invalid("Could not parse the request body: %v", err)
		}
	}
	return nil
}

// pick keeps the configuration members of a request, in OpenSearch names.
func pick(in map[string]any, es bool) map[string]any {
	if es {
		if v, ok := in["ElasticsearchVersion"].(string); ok {
			in["EngineVersion"] = v
			if !strings.HasPrefix(v, "OpenSearch_") {
				in["EngineVersion"] = "Elasticsearch_" + v
			}
		}
		if v, ok := in["ElasticsearchClusterConfig"]; ok {
			in["ClusterConfig"] = v
		}
	}
	out := map[string]any{}
	for _, k := range options {
		if v, ok := in[k]; ok && v != nil {
			out[k] = v
		}
	}
	if aso, ok := out["AdvancedSecurityOptions"].(map[string]any); ok {
		delete(aso, "MasterUserOptions") // the master password stays with the caller
	}
	return out
}

// merge sets src's options on dst: an object field by field, anything else
// whole. Maps are copied, never changed, so views made under the lock stay
// valid after it.
func merge(dst, src map[string]any) {
	for k, v := range src {
		if a, ok := dst[k].(map[string]any); ok {
			if b, ok := v.(map[string]any); ok {
				a = maps.Clone(a)
				maps.Copy(a, b)
				v = a
			}
		}
		dst[k] = v
	}
}

func image(version string) string {
	if i := slices.IndexFunc(versions, func(v struct{ name, image string }) bool { return v.name == version }); i >= 0 {
		return versions[i].image
	}
	return ""
}

// apiVersion names a version the way an API does: the Elasticsearch API
// leaves out the engine of Elasticsearch versions (7.10, OpenSearch_2.19).
func apiVersion(v string, es bool) string {
	if es {
		return strings.TrimPrefix(v, "Elasticsearch_")
	}
	return v
}

func (d *Domain) version() string { v, _ := d.Config["EngineVersion"].(string); return v }

func (d *Domain) engine() string { e, _, _ := strings.Cut(d.version(), "_"); return e }

func (d *Domain) arn() string { return core.ARN("es", d.Region, d.Account, "domain/"+d.Name) }

// host is the endpoint host of a domain.
func (d *Domain) host() string {
	kind := "opensearch"
	if d.Legacy {
		kind = "es"
	}
	return d.Name + "-" + d.ID + "." + d.Region + "." + kind + ".localhost"
}

// spec is the container of a domain: one node, no security plugin, a 512 MB heap.
func (d *Domain) spec() rds.Spec {
	sp := rds.Spec{Image: image(d.version()), Port: 9200, Host: d.Pogi.HostPort, Hello: health, Env: []string{"discovery.type=single-node"}}
	if d.engine() == "Elasticsearch" {
		sp.Env = append(sp.Env, "ES_JAVA_OPTS=-Xms512m -Xmx512m", "xpack.security.enabled=false", "xpack.ml.enabled=false")
	} else {
		sp.Env = append(sp.Env, "OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m", "DISABLE_SECURITY_PLUGIN=true", "DISABLE_INSTALL_DEMO_CONFIG=true")
	}
	return sp
}

func (s *Service) owns(srv *rds.Server) bool {
	for _, d := range s.st.Domains {
		if &d.Pogi == srv {
			return true
		}
	}
	return false
}

// domain finds a domain of the caller; lock held.
func (s *Service) domain(c *core.Ctx, name string) (*Domain, error) {
	if d := s.st.Domains[key(c.Account, c.Region, name)]; d != nil {
		return d, nil
	}
	return nil, notFound(name)
}

// mine lists the caller's domains by name; lock held.
func (s *Service) mine(c *core.Ctx) []*Domain {
	var out []*Domain
	for _, k := range core.SortedKeys(s.st.Domains) {
		if strings.HasPrefix(k, key(c.Account, c.Region, "")) {
			out = append(out, s.st.Domains[k])
		}
	}
	return out
}

// ---- API shapes ----

// options are d's configuration over the defaults AWS fills in, in the
// names of the API (es: Elasticsearch).
func (d *Domain) options(es bool) map[string]any {
	instance := "r6g.large.search"
	if d.Legacy {
		instance = "r5.large.elasticsearch"
	}
	o := map[string]any{
		"ClusterConfig": map[string]any{"InstanceType": instance, "InstanceCount": 1, "DedicatedMasterEnabled": false,
			"ZoneAwarenessEnabled": false, "WarmEnabled": false, "ColdStorageOptions": map[string]any{"Enabled": false}, "MultiAZWithStandbyEnabled": false},
		"EBSOptions":                  map[string]any{"EBSEnabled": true, "VolumeType": "gp2", "VolumeSize": 10},
		"AccessPolicies":              "",
		"IPAddressType":               "ipv4",
		"CognitoOptions":              map[string]any{"Enabled": false},
		"EncryptionAtRestOptions":     map[string]any{"Enabled": false},
		"NodeToNodeEncryptionOptions": map[string]any{"Enabled": false},
		"AdvancedOptions":             map[string]any{"override_main_response_version": "false", "rest.action.multi.allow_explicit_index": "true"},
		"LogPublishingOptions":        map[string]any{},
		"DomainEndpointOptions":       map[string]any{"EnforceHTTPS": false, "TLSSecurityPolicy": "Policy-Min-TLS-1-2-2019-07", "CustomEndpointEnabled": false},
		"AdvancedSecurityOptions":     map[string]any{"Enabled": false, "InternalUserDatabaseEnabled": false, "AnonymousAuthEnabled": false},
		"AutoTuneOptions":             map[string]any{"DesiredState": "DISABLED", "RollbackOnDisable": "NO_ROLLBACK", "UseOffPeakWindow": false},
		"OffPeakWindowOptions":        map[string]any{"Enabled": true, "OffPeakWindow": map[string]any{"WindowStartTime": map[string]any{"Hours": 22, "Minutes": 0}}},
		"SoftwareUpdateOptions":       map[string]any{"AutoSoftwareUpdateEnabled": false},
	}
	merge(o, d.Config)
	if es {
		o["ElasticsearchVersion"], o["ElasticsearchClusterConfig"] = apiVersion(d.version(), es), o["ClusterConfig"]
		delete(o, "EngineVersion")
		delete(o, "ClusterConfig")
	}
	return o
}

var software = map[string]any{"CurrentVersion": "", "NewVersion": "", "UpdateAvailable": false, "Cancellable": false, "UpdateStatus": "COMPLETED",
	"Description": "There is no software update available for this domain.", "AutomatedUpdateDate": 0, "OptionalDeployment": true}

// status is the DomainStatus of d as seen on host (the request's; its port
// goes in the endpoint). phase Creating or Deleting marks the answer to a
// create or a delete. Lock held.
func (d *Domain) status(host string, es bool, phase string) map[string]any {
	if phase == "" {
		phase = map[string]string{"creating": "Creating", "available": "Active"}[d.Pogi.State]
		if phase == "" {
			phase = "Isolated" // the container failed: see Inspect
		}
	}
	o := d.options(es)
	at, _ := o["AutoTuneOptions"].(map[string]any)
	o["AutoTuneOptions"] = map[string]any{"State": at["DesiredState"], "UseOffPeakWindow": at["UseOffPeakWindow"]}
	processing := phase == "Creating" || phase == "Deleting"
	maps.Copy(o, map[string]any{"DomainId": d.Account + "/" + d.Name, "DomainName": d.Name, "ARN": d.arn(), "Created": true,
		"Deleted": phase == "Deleting", "Processing": processing, "UpgradeProcessing": false, "DomainProcessingStatus": phase,
		"ServiceSoftwareOptions": software, "ChangeProgressDetails": map[string]any{"ChangeId": d.Change}, "ModifyingProperties": []any{}})
	if !processing {
		ep := d.host()
		if _, port, err := net.SplitHostPort(host); err == nil {
			ep += ":" + port
		}
		if o["VPCOptions"] != nil {
			o["Endpoints"] = map[string]string{"vpc": ep}
		} else {
			o["Endpoint"] = ep
		}
	}
	return o
}

// config is the DomainConfig of d: each option with its status. Lock held.
func (d *Domain) config(es bool) map[string]any {
	status := func(state any) map[string]any {
		return map[string]any{"CreationDate": core.Time(d.Created), "UpdateDate": core.Time(d.Updated), "UpdateVersion": d.Version,
			"State": state, "PendingDeletion": false}
	}
	out := map[string]any{"ChangeProgressDetails": map[string]any{"ChangeId": d.Change}, "ModifyingProperties": []any{}}
	o := d.options(es)
	for k, v := range o {
		out[k] = map[string]any{"Options": v, "Status": status("Active")}
	}
	if at, ok := o["AutoTuneOptions"].(map[string]any); ok { // its status is its state: ENABLED or DISABLED
		out["AutoTuneOptions"] = map[string]any{"Options": at, "Status": status(at["DesiredState"])}
	}
	return out
}

// ---- domains ----

var validName = regexp.MustCompile(`^[a-z][a-z0-9-]{2,27}$`)

func (s *Service) create(c *core.Ctx, r *http.Request, es bool) (any, error) {
	var in struct {
		DomainName string
		TagList    []rds.Tag
	}
	raw := map[string]any{}
	if err := decode(r, &in, &raw); err != nil {
		return nil, err
	}
	if !validName.MatchString(in.DomainName) {
		return nil, invalid("Domain name %q is not valid: it must start with a lowercase letter and have 3 to 28 lowercase letters, digits and hyphens.", in.DomainName)
	}
	cfg := pick(raw, es)
	if cfg["EngineVersion"] == nil {
		cfg["EngineVersion"] = versions[0].name // AWS's default: the newest OpenSearch
		if es {
			cfg["EngineVersion"] = "Elasticsearch_7.10" // ponytail: AWS's is 1.5, which pogi does not run
		}
	}
	if v, _ := cfg["EngineVersion"].(string); image(v) == "" {
		return nil, invalid("The engine version %v is not supported. ListVersions shows the supported versions.", cfg["EngineVersion"])
	}
	docker := s.box.Available()
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(c.Account, c.Region, in.DomainName)
	if s.st.Domains[k] != nil {
		return nil, core.Errorf(409, "ResourceAlreadyExistsException", "Domain %s already exists.", in.DomainName)
	}
	now := time.Now().UTC()
	d := &Domain{Name: in.DomainName, Account: c.Account, Region: c.Region, ID: core.RandHex(5), Legacy: es, Config: cfg,
		Tags: rds.AddTags(nil, in.TagList), Created: now, Updated: now, Version: 1, Change: core.UUID()}
	s.st.Domains[k] = d
	s.box.Serve(&d.Pogi, c.Account, c.Region, d.Name, 0, 0, d.spec, docker)
	return map[string]any{"DomainStatus": d.status(c.Host, es, "Creating")}, nil
}

func (s *Service) describe(c *core.Ctx, r *http.Request, es bool) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.domain(c, r.PathValue("name"))
	if err != nil {
		return nil, err
	}
	return map[string]any{"DomainStatus": d.status(c.Host, es, "")}, nil
}

func (s *Service) describeMany(c *core.Ctx, r *http.Request, es bool) (any, error) {
	var in struct{ DomainNames []string }
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	list := []any{}
	for _, n := range in.DomainNames {
		if d := s.st.Domains[key(c.Account, c.Region, n)]; d != nil {
			list = append(list, d.status(c.Host, es, ""))
		}
	}
	return map[string]any{"DomainStatusList": list}, nil
}

func (s *Service) delete(c *core.Ctx, r *http.Request, es bool) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.domain(c, r.PathValue("name"))
	if err != nil {
		return nil, err
	}
	out := d.status(c.Host, es, "Deleting")
	delete(s.st.Domains, key(d.Account, d.Region, d.Name))
	if d.Pogi.Container != "" {
		s.box.Remove(d.Pogi.Container)
	}
	return map[string]any{"DomainStatus": out}, nil
}

func (s *Service) describeConfig(c *core.Ctx, r *http.Request, es bool) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.domain(c, r.PathValue("name"))
	if err != nil {
		return nil, err
	}
	return map[string]any{"DomainConfig": d.config(es)}, nil
}

// updateConfig changes options in place: the domain never leaves Active.
// ponytail: the container keeps running as created; a new ClusterConfig or
// heap size is metadata only.
func (s *Service) updateConfig(c *core.Ctx, r *http.Request, es bool) (any, error) {
	var in struct{ DryRun bool }
	raw := map[string]any{}
	if err := decode(r, &in, &raw); err != nil {
		return nil, err
	}
	cfg := pick(raw, es)
	delete(cfg, "EngineVersion") // versions change through UpgradeDomain
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.domain(c, r.PathValue("name"))
	if err != nil {
		return nil, err
	}
	if len(cfg) > 0 && !in.DryRun {
		adv, replace := cfg["AdvancedOptions"]
		merge(d.Config, cfg)
		if replace { // AdvancedOptions is replaced whole, so removing a key removes it
			d.Config["AdvancedOptions"] = adv
		}
		d.Updated, d.Version, d.Change, d.Changed = time.Now().UTC(), d.Version+1, core.UUID(), slices.Sorted(maps.Keys(cfg))
	}
	out := map[string]any{"DomainConfig": d.config(es)}
	if in.DryRun {
		out["DryRunResults"] = map[string]any{"DeploymentType": "DynamicUpdate", "Message": "pogi applies every change in place."}
	}
	return out, nil
}

func (s *Service) progress(c *core.Ctx, r *http.Request, es bool) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.domain(c, r.PathValue("name"))
	if err != nil {
		return nil, err
	}
	status := "COMPLETED"
	if d.Pogi.State == "creating" {
		status = "PROCESSING"
	}
	return map[string]any{"ChangeProgressStatus": map[string]any{"ChangeId": d.Change, "StartTime": core.Time(d.Updated),
		"LastUpdatedTime": core.Time(d.Updated), "Status": status, "CompletedProperties": append([]string{}, d.Changed...), "PendingProperties": []string{},
		"TotalNumberOfStages": 0, "ChangeProgressStages": []any{}, "ConfigChangeStatus": "Completed", "InitiatedBy": "CUSTOMER"}}, nil
}

func (s *Service) listNames(c *core.Ctx, r *http.Request, es bool) (any, error) {
	engine := r.URL.Query().Get("engineType")
	s.mu.Lock()
	defer s.mu.Unlock()
	list := []map[string]string{}
	for _, d := range s.mine(c) {
		if engine == "" || d.engine() == engine {
			list = append(list, map[string]string{"DomainName": d.Name, "EngineType": d.engine()})
		}
	}
	return map[string]any{"DomainNames": list}, nil
}

// ---- versions ----

func (s *Service) listVersions(c *core.Ctx, r *http.Request, es bool) (any, error) {
	list := []string{}
	for _, v := range versions {
		list = append(list, apiVersion(v.name, es))
	}
	if es {
		return map[string]any{"ElasticsearchVersions": list}, nil
	}
	return map[string]any{"Versions": list}, nil
}

// compatible lists the versions each version (or a domain's) upgrades to.
// ponytail: every newer version; AWS allows one major version at a time.
func (s *Service) compatible(c *core.Ctx, r *http.Request, es bool) (any, error) {
	from := ""
	if name := r.URL.Query().Get("domainName"); name != "" {
		s.mu.Lock()
		d, err := s.domain(c, name)
		if err == nil {
			from = d.version()
		}
		s.mu.Unlock()
		if err != nil {
			return nil, err
		}
	}
	list := []any{}
	for i, v := range versions {
		if from != "" && v.name != from || from == "" && i == 0 {
			continue
		}
		to := []string{}
		for _, t := range slices.Backward(versions[:i]) {
			to = append(to, apiVersion(t.name, es))
		}
		list = append(list, map[string]any{"SourceVersion": apiVersion(v.name, es), "TargetVersions": to})
	}
	if es {
		return map[string]any{"CompatibleElasticsearchVersions": list}, nil
	}
	return map[string]any{"CompatibleVersions": list}, nil
}

func (s *Service) forPackage(c *core.Ctx, r *http.Request, es bool) (any, error) {
	return map[string]any{"DomainPackageDetailsList": []any{}}, nil // pogi has no packages
}

// ---- tags ----

// tagged finds the domain of an ARN; lock held.
func (s *Service) tagged(arn string) (*Domain, error) {
	svc, region, account, res, ok := core.ParseARN(arn)
	name, isDomain := strings.CutPrefix(res, "domain/")
	if !ok || svc != "es" || !isDomain {
		return nil, invalid("Invalid ARN: %s", arn)
	}
	if d := s.st.Domains[key(account, region, name)]; d != nil {
		return d, nil
	}
	return nil, notFound(name)
}

func (s *Service) addTags(c *core.Ctx, r *http.Request, es bool) (any, error) {
	var in struct {
		ARN     string
		TagList []rds.Tag
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.tagged(in.ARN)
	if err != nil {
		return nil, err
	}
	d.Tags = rds.AddTags(d.Tags, in.TagList)
	return nil, nil
}

func (s *Service) removeTags(c *core.Ctx, r *http.Request, es bool) (any, error) {
	var in struct {
		ARN     string
		TagKeys []string
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.tagged(in.ARN)
	if err != nil {
		return nil, err
	}
	d.Tags = rds.RemoveTags(d.Tags, in.TagKeys)
	return nil, nil
}

func (s *Service) listTags(c *core.Ctx, r *http.Request, es bool) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.tagged(r.URL.Query().Get("arn"))
	if err != nil {
		return nil, err
	}
	return map[string]any{"TagList": append([]rds.Tag{}, d.Tags...)}, nil
}

// ---- domain endpoints ----

// isEndpoint claims traffic for a domain endpoint on pogi's port.
func isEndpoint(r *http.Request) bool { return endpointHost(r) != "" }

// endpointHost is the domain endpoint host a request is for, or "".
func endpointHost(r *http.Request) string {
	label, base := core.SplitHost(strings.ToLower(r.Host), "")
	if label == "" || !strings.HasSuffix(base, ".opensearch.localhost") && !strings.HasSuffix(base, ".es.localhost") {
		return ""
	}
	return label + "." + base
}

// signing are the SigV4 headers clients add for service es.
var signing = []string{"Authorization", "X-Amz-Date", "X-Amz-Security-Token", "X-Amz-Content-Sha256"}

// proxy passes a request for a domain endpoint to the domain's container.
// ponytail: access policies are not enforced; the signature clients make for
// es is dropped and the request goes on unsigned.
func (s *Service) proxy(w http.ResponseWriter, r *http.Request) {
	host, c := endpointHost(r), core.C(r)
	c.Op = "ESHttp" + r.Method[:1] + strings.ToLower(r.Method[1:])
	s.mu.Lock()
	var d *Domain
	for _, x := range s.st.Domains {
		if x.host() == host {
			d = x
		}
	}
	var addr, why string
	status := http.StatusServiceUnavailable
	switch {
	case d == nil:
		status, why = http.StatusNotFound, "pogi: no OpenSearch domain has the endpoint "+host
	case d.Pogi.Container == "":
		why = "pogi: domain " + d.Name + " has no engine. " + d.Pogi.Note
	case d.Pogi.State == "creating":
		why = "pogi: domain " + d.Name + " is still being created (the first run pulls the image)"
	case d.Pogi.State != "available":
		why = "pogi: the engine of domain " + d.Name + " failed: " + d.Pogi.Note
	default:
		addr = s.hub.Docker().Addr(d.Pogi.HostPort)
	}
	s.mu.Unlock()
	if addr == "" {
		engineError(w, status, why)
		return
	}
	body := core.Body(r)
	(&httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(&url.URL{Scheme: "http", Host: addr})
			for _, h := range signing {
				pr.Out.Header.Del(h)
			}
			pr.Out.Body, pr.Out.ContentLength, pr.Out.TransferEncoding = http.NoBody, 0, nil
			if len(body) > 0 {
				pr.Out.Body, pr.Out.ContentLength = io.NopCloser(bytes.NewReader(body)), int64(len(body))
			}
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			engineError(w, http.StatusBadGateway, "pogi: the engine did not answer: "+err.Error())
		},
	}).ServeHTTP(w, r)
}

// engineError answers in the error shape of OpenSearch itself.
func engineError(w http.ResponseWriter, status int, reason string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"type": "pogi_exception", "reason": reason}, "status": status})
}

// ---- persistence and console ----

func (st state) containers() map[string]bool {
	out := map[string]bool{}
	for _, d := range st.Domains {
		if d.Pogi.Container != "" {
			out[d.Pogi.Container] = true
		}
	}
	return out
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

// Load restores state and reconnects to the containers by name (--persist
// leaves them behind on shutdown); containers of dropped domains go.
func (s *Service) Load(b []byte) error {
	st := state{Domains: map[string]*Domain{}}
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	if st.Domains == nil {
		st.Domains = map[string]*Domain{}
	}
	keep := st.containers()
	docker := len(keep) > 0 && s.box.Available()
	s.mu.Lock()
	old := s.st.containers()
	s.st = st
	for _, d := range st.Domains {
		s.box.Resume(&d.Pogi, d.spec(), docker)
	}
	s.mu.Unlock()
	for n := range old {
		if !keep[n] {
			s.box.Remove(n)
		}
	}
	return nil
}

func (s *Service) Reset() { s.Load([]byte("{}")) }

// Close removes the containers, except those persisted state resumes next run.
func (s *Service) Close() error {
	s.mu.Lock()
	keep := s.st.containers()
	s.mu.Unlock()
	s.box.Close(keep)
	return nil
}

// Resources lists the domains.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := core.Table{Name: "Domains", Columns: []string{"Name", "Engine", "State", "Endpoint", "Region", "Account"}}
	for _, k := range core.SortedKeys(s.st.Domains) {
		d := s.st.Domains[k]
		t.Rows = append(t.Rows, []string{d.Name, d.version(), d.Pogi.State, d.host(), d.Region, d.Account})
	}
	return []core.Table{t}
}

// Inspect returns a domain's status with pogi's side: container, state and pogiNote.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range core.SortedKeys(s.st.Domains) {
		if d := s.st.Domains[k]; d.Name == id {
			out := d.status(core.Hostport(s.hub.URL), false, "")
			out["Tags"], out["pogi"] = slices.Clone(d.Tags), d.Pogi
			return out
		}
	}
	return nil
}
