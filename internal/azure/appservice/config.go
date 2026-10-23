package appservice

import (
	"maps"
	"strings"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// defaults are the properties the API returns for a configuration resource that nobody wrote.
func defaults(name string) map[string]any {
	switch name {
	case "web":
		return map[string]any{"numberOfWorkers": 1, "defaultDocuments": []any{"Default.htm", "Default.html", "Default.asp", "index.htm", "index.html", "iisstart.htm", "default.aspx", "index.php"},
			"netFrameworkVersion": "v4.0", "linuxFxVersion": "", "windowsFxVersion": nil, "requestTracingEnabled": false, "remoteDebuggingEnabled": false,
			"httpLoggingEnabled": false, "detailedErrorLoggingEnabled": false, "scmType": "None", "use32BitWorkerProcess": true, "webSocketsEnabled": false,
			"alwaysOn": false, "managedPipelineMode": "Integrated", "loadBalancing": "LeastRequests", "autoHealEnabled": false, "localMySqlEnabled": false,
			"ipSecurityRestrictions":              []any{map[string]any{"ipAddress": "Any", "action": "Allow", "priority": 2147483647, "name": "Allow all", "description": "Allow all access"}},
			"scmIpSecurityRestrictions":           []any{map[string]any{"ipAddress": "Any", "action": "Allow", "priority": 2147483647, "name": "Allow all", "description": "Allow all access"}},
			"ipSecurityRestrictionsDefaultAction": "Allow", "scmIpSecurityRestrictionsDefaultAction": "Allow", "scmIpSecurityRestrictionsUseMain": false,
			"http20Enabled": true, "minTlsVersion": "1.2", "scmMinTlsVersion": "1.2", "ftpsState": "FtpsOnly", "preWarmedInstanceCount": 0,
			"functionsRuntimeScaleMonitoringEnabled": false, "minimumElasticInstanceCount": 0, "publicNetworkAccess": "Enabled", "vnetRouteAllEnabled": false,
			"healthCheckPath": nil, "appCommandLine": "", "cors": nil}
	case "authsettings":
		return map[string]any{"enabled": false, "runtimeVersion": "~1", "unauthenticatedClientAction": nil, "tokenStoreEnabled": false, "allowedExternalRedirectUrls": nil,
			"defaultProvider": nil, "isAuthFromFile": "false"}
	case "authsettingsV2":
		return map[string]any{"platform": map[string]any{"enabled": false, "runtimeVersion": "~1"},
			"globalValidation":  map[string]any{"requireAuthentication": false, "unauthenticatedClientAction": "RedirectToLoginPage"},
			"identityProviders": map[string]any{}, "login": map[string]any{"routes": map[string]any{}, "tokenStore": map[string]any{"enabled": false}, "preserveUrlFragmentsForLogins": false},
			"httpSettings": map[string]any{"requireHttps": true, "routes": map[string]any{"apiPrefix": "/.auth"}, "forwardProxy": map[string]any{"convention": "NoProxy"}}}
	case "logs":
		return map[string]any{"applicationLogs": map[string]any{"fileSystem": map[string]any{"level": "Off"}},
			"httpLogs":              map[string]any{"fileSystem": map[string]any{"enabled": false, "retentionInMb": 35, "retentionInDays": 0}},
			"failedRequestsTracing": map[string]any{"enabled": false}, "detailedErrorMessages": map[string]any{"enabled": false}}
	case "slotConfigNames":
		return map[string]any{"connectionStringNames": nil, "appSettingNames": nil, "azureStorageConfigNames": nil}
	case "ftp", "scm":
		return map[string]any{"allow": true}
	}
	return map[string]any{} // appsettings, connectionstrings, metadata, azurestorageaccounts
}

// checkChild says the child is one that is served, of a web app that exists.
func (s *Service) checkChild(id arm.ID) error {
	s.mu.Lock()
	site, ok := s.st.ARM.Get(id.Parent())
	s.mu.Unlock()
	if !known(id.Type(), id.Name()) || !ok || !strings.EqualFold(armres.Str(site, "type"), typSite) {
		return arm.NotFound(id)
	}
	return nil
}

// childView builds a configuration resource as the API returns it.
func (s *Service) childView(c *core.Ctx, id arm.ID) (map[string]any, error) {
	if err := s.checkChild(id); err != nil {
		return nil, err
	}
	s.mu.Lock()
	stored, written := s.st.ARM.Get(id)
	s.mu.Unlock()
	name := canon(id.Name())
	if name == "backup" && !written { // a site has no backup until one is configured
		return nil, core.Errorf(404, "BackupNotFound", "The backup configuration of the site '%s' was not found.", id.Parent().Name())
	}
	props := defaults(name)
	maps.Copy(props, armres.Props(stored))
	if name == "publishingcredentials" {
		user, pass := "$"+id.Parent().Name(), armres.Key("publish/" + key(id.Parent()))[:60]
		scm := armres.Labeled(c, "https", strings.ToLower(id.Parent().Name())+".scm.azurewebsites")
		props = map[string]any{"publishingUserName": user, "publishingPassword": pass, "publishingPasswordHash": nil,
			"scmUri": strings.Replace(scm, "://", "://"+user+":"+pass+"@", 1)}
	}
	r := armres.Envelope(id, nil)
	r["properties"] = props
	r["kind"] = nil
	return r, nil
}

func (s *Service) getChild(c *core.Ctx, id arm.ID) (map[string]any, error) { return s.childView(c, id) }

func (s *Service) getAny(c *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
	r, err := s.childView(c, id)
	return r, err
}

func (s *Service) putChild(c *core.Ctx, id arm.ID, body map[string]any) (map[string]any, error) {
	if err := s.checkChild(id); err != nil {
		return nil, err
	}
	props := armres.Props(body)
	name := canon(id.Name())
	if name == "appsettings" {
		for k, v := range props {
			if _, ok := v.(string); !ok {
				return nil, bad("BadRequest", "The value of the app setting '%s' must be a string.", k)
			}
		}
	}
	restart := name == "web" || name == "appsettings" || name == "connectionstrings" // the app runs with these
	s.mu.Lock()
	s.store(id, armres.Clone(props), name == "web") // the site configuration merges, the rest replace
	var x *inst
	if restart {
		x = s.take(key(id.Parent()))
	}
	s.mu.Unlock()
	if restart {
		x.stop()
		s.warm(id.Parent())
	}
	return s.childView(c, id)
}

func (s *Service) listConfig(c *core.Ctx, parent arm.ID) ([]map[string]any, error) {
	return s.listOf(c, parent, typConfig, configNames)
}

func (s *Service) listOf(c *core.Ctx, parent arm.ID, typ string, names []string) ([]map[string]any, error) {
	var out []map[string]any
	for _, n := range names {
		id := parent.Child(typ[strings.LastIndexByte(typ, '/')+1:], n)
		if r, err := s.childView(c, id); err == nil {
			out = append(out, r)
		}
	}
	return out, nil
}
