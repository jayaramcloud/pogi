package appservice

import (
	"net/http"
	"os"
	"strings"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// Resource-manager side. The Functions package registers Microsoft.Web/sites and
// serverfarms and hands the sites that are web apps to Sites(); this service
// registers the children of a site.

func (s *Service) registerARM() {
	arm.Register(s.hub, arm.Provider{Type: typConfig, Put: s.putChild, Get: s.getChild, List: s.listConfig,
		Actions: map[string]func(*core.Ctx, arm.ID, map[string]any) (any, error){"list": s.getAny}})
	arm.Register(s.hub, arm.Provider{Type: typPolicy, Put: s.putChild, Get: s.getChild,
		List: func(c *core.Ctx, parent arm.ID) ([]map[string]any, error) {
			return s.listOf(c, parent, typPolicy, []string{"ftp", "scm"})
		}})
}

// Sites is the provider the Functions package asks for the sites that are not function apps.
func (s *Service) Sites() arm.Provider {
	return arm.Provider{Type: typSite, Async: true, Put: s.putSite, Get: s.getSite, Delete: s.deleteSite, List: s.listSites,
		Actions: map[string]func(*core.Ctx, arm.ID, map[string]any) (any, error){
			"start": s.setState("Running"), "stop": s.setState("Stopped"), "restart": s.restart}}
}

// ---- sites ----

// computed are the properties of a site that the API fills in.
var computed = []string{"state", "defaultHostName", "hostNames", "enabledHostNames", "availabilityState", "usageState", "repositorySiteName",
	"outboundIpAddresses", "possibleOutboundIpAddresses", "customDomainVerificationId", "hostNameSslStates"}

func (s *Service) putSite(c *core.Ctx, id arm.ID, body map[string]any) (map[string]any, error) {
	if !nameRE.MatchString(id.Name()) {
		return nil, bad("InvalidResourceName", "Site name '%s' is invalid: use 2 to 60 letters, digits and hyphens, not starting or ending with a hyphen.", id.Name())
	}
	r := armres.Envelope(id, body)
	p := armres.Props(r)
	planID, ok := arm.ParseID(armres.Str(p, "serverFarmId"))
	if !ok || !strings.EqualFold(planID.Type(), typPlan) {
		return nil, bad("BadRequest", "The parameter properties.serverFarmId has an invalid value.")
	}
	var plan map[string]any
	if pp := arm.Lookup(s.hub, typPlan); pp != nil && pp.Get != nil {
		plan, _ = pp.Get(c, planID)
	}
	if plan == nil {
		return nil, bad("BadRequest", "Cannot find ServerFarm with name %s.", planID.Name())
	}
	if armres.Props(plan)["reserved"] == true {
		p["reserved"] = true
	}
	for _, k := range computed {
		delete(p, k)
	}
	armres.Defaults(r, map[string]any{"kind": "app"})
	cfg, _ := p["siteConfig"].(map[string]any)
	delete(p, "siteConfig")

	k := key(id)
	s.mu.Lock()
	for _, o := range s.st.ARM {
		if oid, _ := arm.ParseID(armres.Str(o, "id")); strings.EqualFold(armres.Str(o, "type"), typSite) && strings.EqualFold(oid.Name(), id.Name()) && key(oid) != k {
			s.mu.Unlock()
			return nil, core.Errorf(http.StatusConflict, "Conflict", "Website with given name %s already exists.", id.Name())
		}
	}
	if old, ok := s.st.ARM.Get(id); ok && armres.Props(old)["state"] == "Stopped" { // a PUT does not start a stopped app
		p["state"] = "Stopped"
	}
	s.st.ARM.Put(id, r)
	s.mergeSiteConfig(id, cfg)
	x := s.take(k)
	s.mu.Unlock()
	x.stop()
	s.warm(id)
	return s.view(c, armres.Clone(r)), nil
}

// mergeSiteConfig stores the siteConfig of a site body: app settings and connection strings go to
// their own resources, the rest to config/web; lock held.
func (s *Service) mergeSiteConfig(id arm.ID, cfg map[string]any) {
	if len(cfg) == 0 {
		return
	}
	cfg = armres.Clone(cfg)
	if l, ok := cfg["appSettings"].([]any); ok {
		set := map[string]any{}
		for _, x := range l {
			m, _ := x.(map[string]any)
			set[armres.Str(m, "name")] = armres.Str(m, "value")
		}
		s.store(id.Child("config", "appsettings"), set, false)
	}
	if l, ok := cfg["connectionStrings"].([]any); ok {
		set := map[string]any{}
		for _, x := range l {
			m, _ := x.(map[string]any)
			set[armres.Str(m, "name")] = map[string]any{"value": armres.Str(m, "connectionString"), "type": armres.Str(m, "type")}
		}
		s.store(id.Child("config", "connectionstrings"), set, false)
	}
	delete(cfg, "appSettings")
	delete(cfg, "connectionStrings")
	s.store(id.Child("config", "web"), cfg, true)
}

// store writes the properties of a child; merge keeps what the child had; lock held.
func (s *Service) store(id arm.ID, props map[string]any, merge bool) {
	r, ok := s.st.ARM.Get(id)
	if !ok || !merge {
		r = armres.Envelope(id, nil)
		delete(r, "properties")
		r["properties"] = map[string]any{}
	}
	for k, v := range props {
		armres.Props(r)[k] = v
	}
	s.st.ARM.Put(id, r)
}

// view adds what the API computes to a stored site.
func (s *Service) view(c *core.Ctx, r map[string]any) map[string]any {
	p, name := armres.Props(r), armres.Str(r, "name")
	host := armres.HostOf(armres.Labeled(c, "https", strings.ToLower(name)+".azurewebsites"))
	armres.Defaults(p, map[string]any{"enabled": true, "httpsOnly": false, "clientAffinityEnabled": true, "reserved": false, "isXenon": false,
		"hyperV": false, "redundancyMode": "None", "hostNamesDisabled": false, "publicNetworkAccess": "Enabled", "state": "Running"})
	p["availabilityState"], p["usageState"] = "Normal", "Normal"
	p["defaultHostName"], p["hostNames"], p["enabledHostNames"] = host, []any{host}, []any{host, strings.Replace(host, ".azurewebsites", ".scm.azurewebsites", 1)}
	p["outboundIpAddresses"], p["possibleOutboundIpAddresses"] = "127.0.0.1", "127.0.0.1"
	p["customDomainVerificationId"] = strings.ToUpper(armres.Key("web/" + armres.Str(r, "id")))[:64]
	p["repositorySiteName"] = name
	return r
}

func (s *Service) getSite(c *core.Ctx, id arm.ID) (map[string]any, error) {
	s.mu.Lock()
	r, ok := s.st.ARM.Get(id)
	s.mu.Unlock()
	if !ok || !strings.EqualFold(armres.Str(r, "type"), typSite) {
		return nil, arm.NotFound(id)
	}
	return s.view(c, r), nil
}

func (s *Service) deleteSite(c *core.Ctx, id arm.ID) error {
	k := key(id)
	s.mu.Lock()
	s.st.ARM.Delete(id)
	x := s.take(k)
	code := s.st.Code[k]
	delete(s.st.Code, k)
	delete(s.logs, k)
	delete(s.fails, k)
	s.mu.Unlock()
	x.stop()
	removeAll(code)
	return nil
}

func (s *Service) listSites(c *core.Ctx, parent arm.ID) ([]map[string]any, error) {
	s.mu.Lock()
	stored := s.st.ARM.List(parent, typSite)
	s.mu.Unlock()
	var out []map[string]any
	for _, r := range stored {
		out = append(out, s.view(c, r))
	}
	return out, nil
}

// setState is the start and stop actions.
func (s *Service) setState(state string) func(*core.Ctx, arm.ID, map[string]any) (any, error) {
	return func(c *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
		k := key(id)
		s.mu.Lock()
		r, ok := s.st.ARM.Get(id)
		if !ok {
			s.mu.Unlock()
			return nil, arm.NotFound(id)
		}
		armres.Props(r)["state"] = state
		s.st.ARM.Put(id, r)
		x := s.take(k)
		s.mu.Unlock()
		x.stop()
		if state == "Running" {
			s.warm(id)
		}
		return nil, nil
	}
}

// restart drops the container and starts a new one.
func (s *Service) restart(c *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
	s.mu.Lock()
	r, ok := s.st.ARM.Get(id)
	x := s.take(key(id))
	s.mu.Unlock()
	if !ok {
		return nil, arm.NotFound(id)
	}
	x.stop()
	if armres.Props(r)["state"] != "Stopped" {
		s.warm(id)
	}
	return nil, nil
}

// ---- children: config/<name> and basicPublishingCredentialsPolicies/<name> ----

// configNames are the configuration resources of a site that are served.
var configNames = []string{"web", "appsettings", "connectionstrings", "authsettings", "authsettingsV2", "logs", "slotConfigNames",
	"metadata", "azurestorageaccounts", "publishingcredentials", "backup"}

func known(typ, name string) bool {
	names := configNames
	if strings.EqualFold(typ, typPolicy) {
		names = []string{"ftp", "scm"}
	}
	for _, n := range names {
		if strings.EqualFold(n, name) {
			return true
		}
	}
	return false
}

// canon is name in the case the API uses.
func canon(name string) string {
	for _, n := range configNames {
		if strings.EqualFold(n, name) {
			return n
		}
	}
	return strings.ToLower(name)
}

func removeAll(dir string) {
	if dir != "" {
		os.RemoveAll(dir)
	}
}
