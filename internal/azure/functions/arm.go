package functions

import (
	"net/http"
	"strings"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// Resource-manager side: App Service plans (stored) and sites. A site whose
// kind says functionapp is a function app of the engine. Any other site is a
// web app: the azappservice service serves it when it is there, else it is
// stored only.
// ponytail: the sites' config sub-resources (appsettings, publishing
// credentials, ...) of a function app are not served; settings come from
// siteConfig.appSettings.

// webApps is what azappservice offers: the provider of the sites that are web apps.
type webApps interface{ Sites() arm.Provider }

func (s *Service) web() *arm.Provider {
	if w, ok := s.h.Service("azappservice").(webApps); ok {
		p := w.Sites()
		return &p
	}
	return nil
}

// webAction passes an action of a site (start, stop, restart) to the web apps, which answer
// "not found" for a site that is not theirs.
func (s *Service) webAction(name string) func(*core.Ctx, arm.ID, map[string]any) (any, error) {
	return func(c *core.Ctx, id arm.ID, body map[string]any) (any, error) {
		if w := s.web(); w != nil {
			if f := w.Actions[name]; f != nil {
				return f(c, id, body)
			}
		}
		return nil, arm.NotFound(id)
	}
}

func (s *Service) registerARM() {
	h := s.h
	// the SDKs take 200 or 202 from a PUT of a plan or a site, and 200 or 204 from a DELETE
	plan := armres.Stored{Type: "Microsoft.Web/serverfarms", Async: true, Mu: &s.mu, St: &s.st.ARM, View: func(c *core.Ctx, r map[string]any) map[string]any {
		kind := "app"
		if armres.Props(r)["reserved"] == true {
			kind = "linux"
		}
		armres.Defaults(r, map[string]any{"kind": kind, "sku": map[string]any{"name": "Y1", "tier": "Dynamic"}})
		p := armres.Props(r)
		armres.Defaults(p, map[string]any{"reserved": false, "perSiteScaling": false, "maximumElasticWorkerCount": 1, "zoneRedundant": false})
		p["provisioningState"], p["status"], p["numberOfSites"] = "Succeeded", "Ready", 0
		return r
	}}.Provider()
	plan.Created, plan.Deleted = http.StatusAccepted, http.StatusOK
	arm.Register(h, plan)
	arm.Register(h, arm.Provider{Type: "Microsoft.Web/sites", Async: true, Created: http.StatusAccepted, Deleted: http.StatusOK,
		Actions: map[string]func(*core.Ctx, arm.ID, map[string]any) (any, error){"start": s.webAction("start"), "stop": s.webAction("stop"), "restart": s.webAction("restart")}, Put: s.putSite, Get: s.getSite, Delete: s.deleteSite, List: s.listSites})
}

// ---- sites ----

func isFunctionApp(r map[string]any) bool {
	return strings.Contains(armres.Str(r, "kind"), "functionapp")
}

// runtimeOf is the worker runtime a site asks for: the FUNCTIONS_WORKER_RUNTIME setting, else the
// language of linuxFxVersion ("Python|3.11"), else node.
func runtimeOf(settings map[string]string, config map[string]any) string {
	if r := settings["FUNCTIONS_WORKER_RUNTIME"]; r != "" {
		return r
	}
	if fx := armres.Str(config, "linuxFxVersion"); fx != "" {
		return strings.Split(fx, "|")[0]
	}
	return "node"
}

// siteSettings reads siteConfig.appSettings, [{name, value}].
func siteSettings(config map[string]any) map[string]string {
	out := map[string]string{}
	l, _ := config["appSettings"].([]any)
	for _, x := range l {
		m, _ := x.(map[string]any)
		out[armres.Str(m, "name")] = armres.Str(m, "value")
	}
	return out
}

func siteView(c *core.Ctx, r map[string]any) map[string]any {
	p, name := armres.Props(r), armres.Str(r, "name")
	host := armres.URL(c, "https", name+".azurewebsites", "/_azure/functions/"+name)
	host = armres.HostOf(host)
	armres.Defaults(p, map[string]any{"enabled": true, "httpsOnly": false, "clientAffinityEnabled": true, "reserved": false,
		"hostNamesDisabled": false, "outboundIpAddresses": "127.0.0.1", "possibleOutboundIpAddresses": "127.0.0.1",
		"customDomainVerificationId": armres.Key("web/" + name)[:32], "publicNetworkAccess": "Enabled"})
	delete(p, "siteConfig") // GET does not return it; the settings are in config/appsettings
	p["state"], p["availabilityState"], p["usageState"] = "Running", "Normal", "Normal"
	p["defaultHostName"], p["hostNames"] = host, []any{host}
	p["repositorySiteName"] = name
	return r
}

func (s *Service) putSite(c *core.Ctx, id arm.ID, body map[string]any) (map[string]any, error) {
	r := armres.Envelope(id, body)
	if w := s.web(); w != nil && !isFunctionApp(r) {
		return w.Put(c, id, body)
	}
	if isFunctionApp(r) {
		config, _ := armres.Props(r)["siteConfig"].(map[string]any)
		settings := siteSettings(config)
		runtime := runtimeOf(settings, config)
		err := s.CreateApp(id.Name(), runtime, "", settings)
		if err != nil && core.AsError(err).Status == http.StatusConflict { // a PUT updates
			err = s.UpdateApp(id.Name(), runtime, "", settings)
		}
		if err != nil {
			return nil, err
		}
	}
	s.mu.Lock()
	s.st.ARM.Put(id, r)
	s.mu.Unlock()
	return siteView(c, armres.Clone(r)), nil
}

func (s *Service) getSite(c *core.Ctx, id arm.ID) (map[string]any, error) {
	s.mu.Lock()
	r, ok := s.st.ARM.Get(id)
	s.mu.Unlock()
	if ok {
		return siteView(c, r), nil
	}
	if w := s.web(); w != nil {
		return w.Get(c, id)
	}
	return nil, arm.NotFound(id)
}

func (s *Service) deleteSite(c *core.Ctx, id arm.ID) error {
	s.mu.Lock()
	r, ok := s.st.ARM.Get(id)
	s.st.ARM.Delete(id)
	s.mu.Unlock()
	if ok && isFunctionApp(r) {
		if err := s.DeleteApp(id.Name()); err != nil && core.AsError(err).Status != http.StatusNotFound {
			return err
		}
	}
	if w := s.web(); w != nil && !ok {
		return w.Delete(c, id)
	}
	return nil
}

func (s *Service) listSites(c *core.Ctx, parent arm.ID) ([]map[string]any, error) {
	s.mu.Lock()
	var out []map[string]any
	for _, r := range s.st.ARM.List(parent, "Microsoft.Web/sites") {
		out = append(out, siteView(c, r))
	}
	s.mu.Unlock()
	if w := s.web(); w != nil {
		more, err := w.List(c, parent)
		return append(out, more...), err
	}
	return out, nil
}
