package arm

import (
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// namespaces azurerm and the Azure CLI look up, with the types pogi itself
// answers for; every namespace reads as Registered.
var builtin = map[string][]string{
	"Microsoft.Resources":           {"resourceGroups", "deployments", "subscriptions"},
	"Microsoft.Authorization":       {"roleAssignments", "roleDefinitions", "locks"},
	"Microsoft.ManagedIdentity":     nil,
	"Microsoft.Storage":             nil,
	"Microsoft.KeyVault":            nil,
	"Microsoft.ServiceBus":          nil,
	"Microsoft.EventHub":            nil,
	"Microsoft.EventGrid":           nil,
	"Microsoft.DocumentDB":          nil,
	"Microsoft.Web":                 nil,
	"Microsoft.SignalRService":      nil,
	"Microsoft.AppConfiguration":    nil,
	"Microsoft.Communication":       nil,
	"Microsoft.Sql":                 nil,
	"Microsoft.DBforPostgreSQL":     nil,
	"Microsoft.DBforMySQL":          nil,
	"Microsoft.Cache":               nil,
	"Microsoft.OperationalInsights": nil,
	"Microsoft.Insights":            nil,
	"Microsoft.Network":             nil,
	"Microsoft.Compute":             nil,
	"Microsoft.ContainerService":    nil,
	"Microsoft.ContainerRegistry":   nil,
	"Microsoft.ContainerInstance":   nil,
	"Microsoft.App":                 nil,
	"Microsoft.ApiManagement":       nil,
}

func (s *Service) providers(sub string) []map[string]any {
	types := map[string][]string{}
	for ns, t := range builtin {
		types[ns] = append([]string(nil), t...)
	}
	for _, t := range Types(s.h) {
		ns, rt, ok := strings.Cut(t, "/")
		if !ok {
			continue
		}
		if !contains(types[ns], rt) {
			types[ns] = append(types[ns], rt)
		}
	}
	var out []map[string]any
	for _, ns := range core.SortedKeys(types) {
		out = append(out, s.providerObj(sub, ns, types[ns]))
	}
	return out
}

func (s *Service) provider(sub, ns string) map[string]any {
	var rts []string
	for _, t := range append(Types(s.h), builtinTypes(ns)...) {
		if n, rt, ok := strings.Cut(t, "/"); ok && strings.EqualFold(n, ns) && !contains(rts, rt) {
			rts = append(rts, rt)
		}
	}
	return s.providerObj(sub, ns, rts)
}

func builtinTypes(ns string) (out []string) {
	for n, ts := range builtin {
		if strings.EqualFold(n, ns) {
			for _, t := range ts {
				out = append(out, n+"/"+t)
			}
		}
	}
	return out
}

func contains(l []string, s string) bool {
	for _, x := range l {
		if strings.EqualFold(x, s) {
			return true
		}
	}
	return false
}

func (s *Service) providerObj(sub, ns string, types []string) map[string]any {
	var locs []string
	for _, r := range regions {
		locs = append(locs, r[1])
	}
	rts := []map[string]any{}
	for _, t := range types {
		versions := []string{"2022-09-01"}
		if p := Lookup(s.h, ns+"/"+t); p != nil && len(p.APIVersions) > 0 {
			versions = p.APIVersions
		}
		rts = append(rts, map[string]any{"resourceType": t, "locations": locs, "apiVersions": versions, "capabilities": "None"})
	}
	id := "/providers/" + ns
	if sub != "" {
		id = "/subscriptions/" + sub + id
	}
	return map[string]any{"id": id, "namespace": ns, "registrationState": "Registered", "registrationPolicy": "RegistrationRequired", "resourceTypes": rts}
}

// metadata is what `metadata_host` in azurerm and the Azure CLI's cloud
// registration read: pogi's own URLs for every endpoint. api-version
// 2019-05-01 and older spell the DNS suffixes with a leading dot.
func metadata(k *call) map[string]any {
	base := k.base + "/"
	host := k.r.Host
	dot := ""
	if v := k.r.URL.Query().Get("api-version"); v != "" && v < "2020" {
		dot = "."
	}
	return map[string]any{
		"name":            "AzureCloud",
		"portal":          base + "_pogi/",
		"resourceManager": base,
		"authentication": map[string]any{"loginEndpoint": base, "audiences": []string{base, "https://management.core.windows.net/", "https://management.azure.com/"},
			"tenant": "common", "identityProvider": "AAD"},
		"graph":                    base,
		"graphAudience":            base,
		"graphEndpoint":            base,
		"microsoftGraphResourceId": base,
		"media":                    "https://rest.media.azure.net",
		"batch":                    "https://batch.core.windows.net/",
		"sqlManagement":            "https://management.core.windows.net:8443/",
		"activeDirectoryDataLake":  "https://datalake.azure.net/",
		"logAnalyticsResourceId":   "https://api.loganalytics.io",
		"ossrdbmsResourceId":       "https://ossrdbms-aad.database.windows.net",
		"suffixes": map[string]any{
			"keyVaultDns":                  dot + "vault.localhost:" + port(host),
			"storage":                      dot + "localhost:" + port(host),
			"sqlServerHostname":            dot + "database.windows.net",
			"acrLoginServer":               dot + "azurecr.io",
			"mysqlServerEndpoint":          dot + "mysql.database.azure.com",
			"postgresqlServerEndpoint":     dot + "postgres.database.azure.com",
			"mariadbServerEndpoint":        dot + "mariadb.database.azure.com",
			"azureFrontDoorEndpointSuffix": dot + "azurefd.net",
		},
	}
}

func port(host string) string {
	if i := strings.LastIndexByte(host, ':'); i >= 0 && !strings.Contains(host[i:], "]") {
		return host[i+1:]
	}
	return "443"
}
