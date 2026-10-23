package web

import (
	"cmp"
	"embed"
	"encoding/base64"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"slices"
	"strconv"
	"strings"
)

// Service icons for the console and the Terraform diagram. The files in icons/ are the providers'
// own icons (AWS, Azure, Google Cloud), copied byte for byte (see icons/NOTICE: they are not under
// the MIT licence). A service with no official icon draws the fallback glyph of its kind.

//go:embed icons
var iconFiles embed.FS

// iconHandler serves the embedded icons at /_pogi/icons/.
func iconHandler() http.Handler {
	sub, _ := fs.Sub(iconFiles, "icons")
	h := http.StripPrefix("/_pogi/icons/", http.FileServerFS(sub))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=86400")
		h.ServeHTTP(w, r)
	})
}

// svcIcons (svcicons_table.go) is the one icon table, for the console and the Terraform diagram.

// categoryKind picks the fallback glyph of a service with no official icon, from its catalog category.
var categoryKind = map[string]string{
	"Storage": "storage", "Databases": "database", "Messaging & events": "messaging", "Compute & containers": "compute",
	"Networking": "network", "Security & identity": "security",
}

// serviceIcon finds the icon of a service: the file under icons/ ("" for none) and the kind of
// fallback glyph. The service is a registry id or a name the Terraform estimate uses. A name
// that is neither gets kind "" and the caller guesses.
func serviceIcon(cloud, service string) (file, kind string) {
	if f, ok := svcIcons[cloud+"/"+strings.ToLower(service)]; ok {
		return f, ""
	}
	if c, ok := coverage().Services[service]; ok && c.Cloud == cloud {
		return "", cmp.Or(categoryKind[c.Category], "other")
	}
	return "", ""
}

// tfIconTypes names the table key for the types whose second word is too coarse or not the service
// ("virtual", "key", "cloudwatch_event_rule"). The first prefix that matches wins.
var tfIconTypes = []struct{ prefix, key string }{
	{"aws_vpc", "aws/vpc"}, {"aws_subnet", "aws/vpc"}, {"aws_route53", "aws/route 53"}, {"aws_route", "aws/vpc"}, {"aws_internet_gateway", "aws/vpc"},
	{"aws_security_group", "aws/vpc"}, {"aws_network_acl", "aws/vpc"},
	{"aws_iam", "aws/iam"}, {"aws_lb", "aws/elastic load balancing"},
	{"aws_cloudwatch_event", "aws/eventbridge"}, {"aws_scheduler", "aws/eventbridge"}, {"aws_pipes", "aws/eventbridge"},
	{"aws_api_gateway", "aws/api gateway"}, {"aws_apigatewayv2", "aws/api gateway"},
	{"aws_wafv2", "aws/wafv2"}, {"aws_cloudwatch", "aws/cloudwatch"},
	{"azurerm_virtual_machine", "azure/virtual machines"}, {"azurerm_virtual_network", "azure/virtual network"},
	{"azurerm_subnet", "azure/subnet"}, {"azurerm_network_security", "azure/nsg"},
	{"azurerm_linux_web_app", "azure/app service"}, {"azurerm_windows_web_app", "azure/app service"},
	{"azurerm_container_registry", "azure/container registry"}, {"azurerm_container_group", "azure/container instances"},
	{"azurerm_container_app", "azure/container apps"}, {"azurerm_mssql", "azure/sql database"},
	{"azurerm_redis", "azure/redis cache"}, {"azurerm_servicebus", "azure/service bus"},
	{"azurerm_eventhub", "azure/event hubs"}, {"azurerm_eventgrid", "azure/event grid"},
	{"azurerm_key_vault", "azure/key vault"}, {"azurerm_api_management", "azure/api management"},
	{"azurerm_log_analytics", "azure/monitor"}, {"azurerm_monitor", "azure/monitor"},
}

// tfIconFor finds the icon file ("" for none) and the kind of fallback icon for a box.
func tfIconFor(cloud, service, typ string) (file, kind string) {
	key := service
	for _, o := range tfIconTypes {
		if strings.HasPrefix(typ, o.prefix) {
			cloud, key, _ = strings.Cut(o.key, "/")
			break
		}
	}
	if file, kind = serviceIcon(cloud, key); file != "" || kind != "" {
		return file, kind
	}
	return "", tfKindOf(typ)
}

// tfKindWords guesses the kind of a type that is not in the table, from its name. The first kind with a
// match wins.
// ponytail: a keyword guess; a type it misses draws the "other" icon. Add a table entry for it.
var tfKindWords = []struct {
	kind  string
	words []string
}{
	{"database", []string{"_db_", "database", "sql", "redis", "cache", "dynamo", "cosmos"}},
	{"storage", []string{"bucket", "storage", "disk", "volume", "blob", "backup", "snapshot", "_efs"}},
	{"messaging", []string{"queue", "topic", "pubsub", "event", "kinesis", "stream", "_mq", "servicebus", "notification"}},
	{"network", []string{"vpc", "subnet", "route", "network", "firewall", "gateway", "_nat", "dns", "load_balancer", "forwarding", "_lb", "address", "security_group"}},
	{"security", []string{"iam", "kms", "_key", "secret", "_role", "_policy", "cert", "identity", "waf", "security"}},
	{"compute", []string{"instance", "_vm", "virtual_machine", "function", "lambda", "container", "_node", "autoscaling", "cluster", "service_plan", "_app"}},
}

func tfKindOf(typ string) string {
	for _, k := range tfKindWords {
		for _, w := range k.words {
			if strings.Contains(typ, w) {
				return k.kind
			}
		}
	}
	return "other"
}

// tfKindGlyphs are the fallback icons, drawn on a 24 by 24 grid in the console's stroke style.
var tfKindGlyphs = map[string]string{
	"compute":   `<rect x="5" y="5" width="14" height="14" rx="2"/><rect x="9" y="9" width="6" height="6"/><path d="M9 2v3M15 2v3M9 19v3M15 19v3M2 9h3M2 15h3M19 9h3M19 15h3"/>`,
	"storage":   `<path d="M3 8l2-4h14l2 4v10a1 1 0 0 1-1 1H4a1 1 0 0 1-1-1z"/><path d="M3 8h18M9 12h6"/>`,
	"database":  `<ellipse cx="12" cy="6" rx="8" ry="3"/><path d="M4 6v12c0 1.7 3.6 3 8 3s8-1.3 8-3V6M4 12c0 1.7 3.6 3 8 3s8-1.3 8-3"/>`,
	"network":   `<circle cx="12" cy="5" r="2.5"/><circle cx="5" cy="19" r="2.5"/><circle cx="19" cy="19" r="2.5"/><path d="M12 7.5v4M12 11.5l-6 5.5M12 11.5l6 5.5"/>`,
	"messaging": `<rect x="3" y="5" width="18" height="14" rx="2"/><path d="M3 7l9 6 9-6"/>`,
	"security":  `<path d="M12 3l8 3v6c0 5-3.5 8-8 9-4.5-1-8-4-8-9V6z"/><path d="M9 12l2 2 4-4"/>`,
	"other":     `<path d="M12 3l8 4.5v9L12 21l-8-4.5v-9z"/><path d="M4 7.5l8 4.5 8-4.5M12 12v9"/>`,
}

// tfIconDefs is the <defs> content for the icons the boxes use: each distinct icon once. On the page an
// icon is an <image> that links the served file; in an export it embeds the file as a data URI, so
// the file stands alone. A kind's fallback is a <symbol>.
func (g *tfGraph) iconDefs(export bool) string {
	var files, kinds []string
	for _, n := range g.Nodes {
		if n.Frame != "" {
			continue
		}
		if f, k := n.icon(); f != "" {
			if !slices.Contains(files, f) {
				files = append(files, f)
			}
		} else if !slices.Contains(kinds, k) {
			kinds = append(kinds, k)
		}
	}
	slices.Sort(files)
	slices.Sort(kinds)
	var b strings.Builder
	for _, f := range files {
		href := "/_pogi/icons/" + f
		if export {
			var ok bool
			if href, ok = iconData(f); !ok {
				continue // a table entry without a file is caught by the tests
			}
		}
		b.WriteString(`<image id="` + tfIconID(f) + `" width="` + tfIconPx + `" height="` + tfIconPx + `" href="` + href + `"/>`)
	}
	for _, k := range kinds {
		b.WriteString(`<symbol id="tfk-` + k + `" viewBox="0 0 24 24" fill="none" stroke="var(--dim)" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round">` + tfKindGlyphs[k] + `</symbol>`)
	}
	return b.String()
}

// iconRef is the icon of a registry service: its file, or else the glyph kind to draw.
func iconRef(cloud, svc string) (file, kind string) {
	file, kind = serviceIcon(cloud, svc)
	if file == "" {
		kind = cmp.Or(kind, "other")
	}
	return file, kind
}

// iconData is an icon file as a data URI, for an SVG that must stand alone.
func iconData(file string) (string, bool) {
	data, err := iconFiles.ReadFile("icons/" + file)
	if err != nil {
		return "", false
	}
	return "data:image/svg+xml;base64," + base64.StdEncoding.EncodeToString(data), true
}

// svcIconHTML is a service's icon in the console: decorative, because its name is always beside
// it. The chip behind it and the cloud marker use theme tokens (see .si in pogi.css).
func svcIconHTML(cloud, svc string) template.HTML {
	file, kind := iconRef(cloud, svc)
	in := `<svg class="gl" viewBox="0 0 24 24" aria-hidden="true">` + tfKindGlyphs[kind] + `</svg>`
	if file != "" {
		in = `<img src="/_pogi/icons/` + file + `" alt="" width="16" height="16" loading="lazy">`
	}
	return template.HTML(`<span class="si ` + template.HTMLEscapeString(cloud) + `">` + in + `</span>`)
}

// svcIconSVG draws a service's icon inside an SVG (the Map) at x, y, size px square.
func svcIconSVG(cloud, svc string, x, y, size int) string {
	file, kind := iconRef(cloud, svc)
	at := fmt.Sprintf(` x="%d" y="%d" width="%d" height="%d"`, x, y, size, size)
	if file != "" {
		return `<image` + at + ` href="/_pogi/icons/` + file + `"/>`
	}
	return `<svg` + at + ` viewBox="0 0 24 24" fill="none" stroke="var(--dim)" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round">` + tfKindGlyphs[kind] + `</svg>`
}

const tfIconPx = "28"

func tfIconID(file string) string {
	return "tfi-" + strings.NewReplacer("/", "-", ".svg", "").Replace(file)
}

// icon is the icon of a box: a module box and any type with no kind gets the "other" icon.
func (n *tfNode) icon() (file, kind string) {
	if n.Collapsed {
		return "", "other"
	}
	return tfIconFor(n.Cloud, n.Service, n.Type)
}

// use draws the box's icon at x, y. It is decorative: the box's label already names the type.
func (n *tfNode) use(x, y int) string {
	at := ` x="` + strconv.Itoa(x) + `" y="` + strconv.Itoa(y) + `"`
	f, k := n.icon()
	if f != "" {
		return `<use href="#` + tfIconID(f) + `"` + at + ` aria-hidden="true"/>`
	}
	return `<use href="#tfk-` + k + `"` + at + ` width="` + tfIconPx + `" height="` + tfIconPx + `" aria-hidden="true"/>`
}
