package armres

import (
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// The addresses Providers hand out must work for whoever asked: Terraform on the
// host reaches pogi as localhost, where *.localhost names resolve, but one in a
// container reaches it as host.docker.internal, where they do not. So the
// endpoints follow the host the resource manager was called at.

// reqHost is "host:port" the request came to, or pogi's own address when there is no request (tests).
func reqHost(c *core.Ctx) string {
	if c.Host != "" {
		return c.Host
	}
	u, _ := url.Parse(c.Hub.URL)
	return u.Host
}

func hostname(hostport string) string {
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		return h
	}
	return hostport
}

// local reports whether *.localhost names resolve for a caller that came to host: loopback names and addresses.
func local(hostport string) bool {
	h := strings.Trim(hostname(hostport), "[]")
	return h == "localhost" || strings.HasSuffix(h, ".localhost") || net.ParseIP(h) != nil
}

// Base is pogi's main address as the caller reached it.
func Base(c *core.Ctx) string {
	if c.Host == "" {
		return c.Hub.URL
	}
	return "https://" + c.Host // the resource manager is spoken to over TLS
}

// URL is the address of a named resource whose service tells resources apart by host name or by a
// path: scheme://<label>.localhost:7644 for callers that resolve *.localhost, and
// scheme://<host>:<port><path> for the others. URL(c, "https", "orders.eventgrid", "/_azure/eventgrid/orders")
func URL(c *core.Ctx, scheme, label, path string) string {
	if host := reqHost(c); !local(host) {
		return scheme + "://" + host + path
	}
	return Labeled(c, scheme, label)
}

// Labeled is the address of a resource that only has a host-name form, scheme://<label>.localhost:7644.
// A caller that cannot resolve *.localhost (Terraform in a container) has to map the name to pogi
// itself, for example with docker's --add-host.
func Labeled(c *core.Ctx, scheme, label string) string {
	host := reqHost(c)
	name, port := c.Hub.Cfg.Hostname, ""
	if name == "" {
		name = "localhost"
	}
	if p := portOf(host); p != "" {
		port = ":" + p
	}
	return scheme + "://" + label + "." + name + port
}

func portOf(hostport string) string {
	_, p, _ := net.SplitHostPort(hostport)
	return p
}

// Addr is where a service's own port listens as the caller reaches it, "host:port"; def when it has none.
func Addr(c *core.Ctx, svc string, def int) string {
	port, host := strconv.Itoa(def), "localhost"
	if a, ok := c.Hub.Endpoint("port:" + svc); ok {
		host, port = hostname(a), portOf(a)
	}
	if c.Host != "" {
		host = hostname(c.Host)
	}
	return net.JoinHostPort(host, port)
}

// HostAccount is the storage account in a host name <account>.<component>.<suffix>, with component
// "blob", "queue" or "table": how the azurerm provider and the Azure CLI address the data plane.
func HostAccount(hostport, component string) (string, bool) {
	account, rest, ok := strings.Cut(hostname(hostport), ".")
	rest, _, _ = strings.Cut(rest, ".")
	return account, ok && rest == component && account != ""
}

// HostMatch is the Info.Match of a storage service on pogi's main port, which the account's
// primaryEndpoints name as https://<account>.<component>.localhost:7644; Azurite's own port keeps
// serving path-style URLs.
func HostMatch(component string) func(*http.Request) bool {
	return func(r *http.Request) bool {
		_, ok := HostAccount(r.Host, component)
		return ok
	}
}

// AccountPath returns the request with the account of a <account>.<component>.<suffix> host in front of
// its path, which is where the storage services, written for Azurite's path-style URLs, look for it.
func AccountPath(r *http.Request, component string) *http.Request {
	account, ok := HostAccount(r.Host, component)
	if !ok {
		return r
	}
	r = r.Clone(r.Context())
	r.URL.Path, r.URL.RawPath = "/"+account+r.URL.Path, ""
	return r
}

// HostOf is the "host:port" of a URL.
func HostOf(u string) string {
	p, _ := url.Parse(u)
	if p == nil {
		return ""
	}
	return p.Host
}
