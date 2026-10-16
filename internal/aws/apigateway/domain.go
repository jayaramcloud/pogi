package apigateway

import (
	"cmp"
	"net"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Custom domain names. A request whose Host is a registered domain name goes
// to the API its base path mapping names; see viaDomain.

// Domain is a v1 custom domain name (REST APIs); the wire shape doubles as state.
type Domain struct {
	DomainName                       string                 `json:"domainName"`
	DomainNameID                     string                 `json:"domainNameId,omitempty"`
	CertificateName                  string                 `json:"certificateName,omitempty"`
	CertificateArn                   string                 `json:"certificateArn,omitempty"`
	CertificateUploadDate            *core.Time             `json:"certificateUploadDate,omitempty"`
	RegionalDomainName               string                 `json:"regionalDomainName,omitempty"`
	RegionalHostedZoneID             string                 `json:"regionalHostedZoneId,omitempty"`
	RegionalCertificateName          string                 `json:"regionalCertificateName,omitempty"`
	RegionalCertificateArn           string                 `json:"regionalCertificateArn,omitempty"`
	DistributionDomainName           string                 `json:"distributionDomainName,omitempty"`
	DistributionHostedZoneID         string                 `json:"distributionHostedZoneId,omitempty"`
	EndpointConfiguration            *EndpointConfiguration `json:"endpointConfiguration,omitempty"`
	DomainNameStatus                 string                 `json:"domainNameStatus,omitempty"`
	SecurityPolicy                   string                 `json:"securityPolicy,omitempty"`
	MutualTlsAuthentication          any                    `json:"mutualTlsAuthentication,omitempty"`
	OwnershipVerificationCertificate string                 `json:"ownershipVerificationCertificateArn,omitempty"`
	Tags                             map[string]string      `json:"tags,omitempty"`
}

// BasePathMapping sends the requests under a base path of a domain to a stage.
type BasePathMapping struct {
	BasePath  string `json:"basePath"` // "(none)" is the root
	RestAPIID string `json:"restApiId"`
	Stage     string `json:"stage,omitempty"`
}

type domain struct {
	Domain
	Account  string                      `json:"account"`
	Region   string                      `json:"region"`
	Mappings map[string]*BasePathMapping `json:"mappings"` // by base path
}

// HTTPDomain is a v2 custom domain name (HTTP and WebSocket APIs).
type HTTPDomain struct {
	APIMappingSelectionExpression string            `json:"apiMappingSelectionExpression"`
	DomainName                    string            `json:"domainName"`
	DomainNameConfigurations      []DomainConfig    `json:"domainNameConfigurations,omitempty"`
	MutualTlsAuthentication       any               `json:"mutualTlsAuthentication,omitempty"`
	RoutingMode                   string            `json:"routingMode,omitempty"`
	Tags                          map[string]string `json:"tags,omitempty"`
}

type DomainConfig struct {
	APIGatewayDomainName string `json:"apiGatewayDomainName,omitempty"`
	CertificateArn       string `json:"certificateArn,omitempty"`
	CertificateName      string `json:"certificateName,omitempty"`
	DomainNameStatus     string `json:"domainNameStatus,omitempty"`
	EndpointType         string `json:"endpointType,omitempty"`
	HostedZoneID         string `json:"hostedZoneId,omitempty"`
	IPAddressType        string `json:"ipAddressType,omitempty"`
	SecurityPolicy       string `json:"securityPolicy,omitempty"`
}

// APIMapping is the v2 base path mapping; its key may hold slashes.
type APIMapping struct {
	APIID         string `json:"apiId"`
	APIMappingID  string `json:"apiMappingId"`
	APIMappingKey string `json:"apiMappingKey,omitempty"`
	Stage         string `json:"stage"`
}

type httpDomain struct {
	HTTPDomain
	Account  string                 `json:"account"`
	Region   string                 `json:"region"`
	Mappings map[string]*APIMapping `json:"mappings"` // by mapping id
}

const none = "(none)"

// domainTaken reports whether name is registered in either API version.
func (s *Service) domainTaken(name string) bool {
	return s.st.Domains[name] != nil || s.st.HTTPDomains[name] != nil
}

func (s *Service) domain(r *http.Request, c *core.Ctx) (*domain, error) {
	d := s.st.Domains[strings.ToLower(r.PathValue("domain"))]
	if d == nil || d.Account != c.Account || d.Region != c.Region {
		return nil, notFound("Invalid domain name identifier specified")
	}
	return d, nil
}

func (s *Service) httpDomain(r *http.Request, c *core.Ctx) (*httpDomain, error) {
	d := s.st.HTTPDomains[strings.ToLower(r.PathValue("domain"))]
	if d == nil || d.Account != c.Account || d.Region != c.Region {
		return nil, notFound("Invalid domain name identifier specified")
	}
	return d, nil
}

func (s *Service) domainRoutes() {
	const d = "/domainnames/{domain}"
	s.route("POST /domainnames", "CreateDomainName", func(r *http.Request, c *core.Ctx) (int, any, error) {
		in := &domain{Mappings: map[string]*BasePathMapping{}, Account: c.Account, Region: c.Region}
		if err := decode(r, &in.Domain); err != nil {
			return 0, nil, err
		}
		in.DomainName = strings.ToLower(in.DomainName)
		if in.DomainName == "" {
			return 0, nil, badRequest("Domain name is required")
		}
		if s.domainTaken(in.DomainName) {
			return 0, nil, conflict("Domain name already exists")
		}
		in.DomainNameID = newID()
		in.DomainNameStatus = "AVAILABLE"
		in.SecurityPolicy = cmp.Or(in.SecurityPolicy, "TLS_1_2")
		if in.EndpointConfiguration == nil || len(in.EndpointConfiguration.Types) == 0 {
			in.EndpointConfiguration = &EndpointConfiguration{Types: []string{"EDGE"}, IpAddressType: "ipv4"}
		}
		if slices.Contains(in.EndpointConfiguration.Types, "REGIONAL") {
			in.RegionalDomainName = "d-" + core.RandString(10, "abcdefghijklmnopqrstuvwxyz0123456789") + ".execute-api." + c.Region + ".amazonaws.com"
			in.RegionalHostedZoneID = "Z1UJRXOUMOOFQ8"
		} else {
			now := core.Time(time.Now())
			in.CertificateUploadDate = &now
			in.DistributionDomainName = "d" + core.RandString(13, "abcdefghijklmnopqrstuvwxyz0123456789") + ".cloudfront.net"
			in.DistributionHostedZoneID = "Z2FDTNDATAQYW2"
		}
		s.st.Domains[in.DomainName] = in
		return http.StatusCreated, &in.Domain, nil
	})
	s.route("GET /domainnames", "GetDomainNames", func(r *http.Request, c *core.Ctx) (int, any, error) {
		list := []*Domain{}
		for _, k := range core.SortedKeys(s.st.Domains) {
			if d := s.st.Domains[k]; d.Account == c.Account && d.Region == c.Region {
				list = append(list, &d.Domain)
			}
		}
		return 200, map[string]any{"item": list}, nil
	})
	s.route("GET "+d, "GetDomainName", func(r *http.Request, c *core.Ctx) (int, any, error) {
		dom, err := s.domain(r, c)
		if err != nil {
			return 0, nil, err
		}
		return 200, &dom.Domain, nil
	})
	s.route("PATCH "+d, "UpdateDomainName", func(r *http.Request, c *core.Ctx) (int, any, error) {
		dom, err := s.domain(r, c)
		if err != nil {
			return 0, nil, err
		}
		return 200, &dom.Domain, patch(r, &dom.Domain)
	})
	s.route("DELETE "+d, "DeleteDomainName", func(r *http.Request, c *core.Ctx) (int, any, error) {
		dom, err := s.domain(r, c)
		if err != nil {
			return 0, nil, err
		}
		delete(s.st.Domains, dom.DomainName)
		return http.StatusAccepted, nil, nil
	})

	const m = d + "/basepathmappings"
	s.route("POST "+m, "CreateBasePathMapping", func(r *http.Request, c *core.Ctx) (int, any, error) {
		dom, err := s.domain(r, c)
		if err != nil {
			return 0, nil, err
		}
		bm := &BasePathMapping{}
		if err := decode(r, bm); err != nil {
			return 0, nil, err
		}
		if bm.BasePath == "" {
			bm.BasePath = none
		}
		if err := s.checkMapping(dom, bm, ""); err != nil {
			return 0, nil, err
		}
		dom.Mappings[bm.BasePath] = bm
		return http.StatusCreated, bm, nil
	})
	s.route("GET "+m, "GetBasePathMappings", func(r *http.Request, c *core.Ctx) (int, any, error) {
		dom, err := s.domain(r, c)
		if err != nil {
			return 0, nil, err
		}
		return 200, items(dom.Mappings, "item"), nil
	})
	mapping := func(r *http.Request, c *core.Ctx) (*domain, *BasePathMapping, error) {
		dom, err := s.domain(r, c)
		if err != nil {
			return nil, nil, err
		}
		bm, err := find(dom.Mappings, r.PathValue("base"), "base path mapping")
		return dom, bm, err
	}
	s.route("GET "+m+"/{base}", "GetBasePathMapping", func(r *http.Request, c *core.Ctx) (int, any, error) {
		_, bm, err := mapping(r, c)
		return 200, bm, err
	})
	s.route("PATCH "+m+"/{base}", "UpdateBasePathMapping", func(r *http.Request, c *core.Ctx) (int, any, error) {
		dom, bm, err := mapping(r, c)
		if err != nil {
			return 0, nil, err
		}
		upd := clone(bm)
		if err := patch(r, upd); err != nil {
			return 0, nil, err
		}
		if upd.BasePath == "" {
			upd.BasePath = none
		}
		if err := s.checkMapping(dom, upd, bm.BasePath); err != nil {
			return 0, nil, err
		}
		delete(dom.Mappings, bm.BasePath)
		dom.Mappings[upd.BasePath] = upd
		return 200, upd, nil
	})
	s.route("DELETE "+m+"/{base}", "DeleteBasePathMapping", func(r *http.Request, c *core.Ctx) (int, any, error) {
		dom, bm, err := mapping(r, c)
		if err != nil {
			return 0, nil, err
		}
		delete(dom.Mappings, bm.BasePath)
		return http.StatusAccepted, nil, nil
	})
}

// checkMapping validates a mapping; self is the base path it is moving from.
func (s *Service) checkMapping(dom *domain, bm *BasePathMapping, self string) error {
	if bm.BasePath != self && dom.Mappings[bm.BasePath] != nil {
		return conflict("Base path already exists for this domain name")
	}
	a := s.st.Rest[bm.RestAPIID]
	switch {
	case a == nil || a.Account != dom.Account || a.Region != dom.Region:
		return notFound("Invalid REST API identifier specified %s:%s", dom.Account, bm.RestAPIID)
	case bm.Stage != "" && a.Stages[bm.Stage] == nil:
		return notFound("Invalid stage identifier specified")
	}
	return nil
}

func (s *Service) httpDomainRoutes() {
	const d = "/v2/domainnames/{domain}"
	s.route("POST /v2/domainnames", "CreateDomainName", func(r *http.Request, c *core.Ctx) (int, any, error) {
		in := &httpDomain{Mappings: map[string]*APIMapping{}, Account: c.Account, Region: c.Region}
		if err := decode(r, &in.HTTPDomain); err != nil {
			return 0, nil, err
		}
		in.DomainName = strings.ToLower(in.DomainName)
		if in.DomainName == "" {
			return 0, nil, badRequest("Domain name is required")
		}
		if s.domainTaken(in.DomainName) {
			return 0, nil, conflict("The domain name resource already exists.")
		}
		in.APIMappingSelectionExpression = "$request.basepath"
		in.RoutingMode = cmp.Or(in.RoutingMode, "API_MAPPING_ONLY")
		for i := range in.DomainNameConfigurations {
			dc := &in.DomainNameConfigurations[i]
			dc.EndpointType = cmp.Or(dc.EndpointType, "REGIONAL")
			dc.DomainNameStatus = "AVAILABLE"
			dc.SecurityPolicy = cmp.Or(dc.SecurityPolicy, "TLS_1_2")
			dc.IPAddressType = cmp.Or(dc.IPAddressType, "ipv4")
			dc.APIGatewayDomainName = "d-" + core.RandString(10, "abcdefghijklmnopqrstuvwxyz0123456789") + ".execute-api." + c.Region + ".amazonaws.com"
			dc.HostedZoneID = "Z1UJRXOUMOOFQ8"
		}
		s.st.HTTPDomains[in.DomainName] = in
		return http.StatusCreated, &in.HTTPDomain, nil
	})
	s.route("GET /v2/domainnames", "GetDomainNames", func(r *http.Request, c *core.Ctx) (int, any, error) {
		list := []*HTTPDomain{}
		for _, k := range core.SortedKeys(s.st.HTTPDomains) {
			if d := s.st.HTTPDomains[k]; d.Account == c.Account && d.Region == c.Region {
				list = append(list, &d.HTTPDomain)
			}
		}
		return 200, map[string]any{"items": list}, nil
	})
	s.route("GET "+d, "GetDomainName", func(r *http.Request, c *core.Ctx) (int, any, error) {
		dom, err := s.httpDomain(r, c)
		if err != nil {
			return 0, nil, err
		}
		return 200, &dom.HTTPDomain, nil
	})
	s.route("PATCH "+d, "UpdateDomainName", func(r *http.Request, c *core.Ctx) (int, any, error) {
		dom, err := s.httpDomain(r, c)
		if err != nil {
			return 0, nil, err
		}
		var in struct {
			DomainNameConfigurations []DomainConfig `json:"domainNameConfigurations"`
			MutualTlsAuthentication  any            `json:"mutualTlsAuthentication"`
			RoutingMode              string         `json:"routingMode"`
		}
		if err := decode(r, &in); err != nil {
			return 0, nil, err
		}
		for i := range in.DomainNameConfigurations { // keep the generated host names of configurations that stay
			dc := &in.DomainNameConfigurations[i]
			for _, old := range dom.DomainNameConfigurations {
				if old.EndpointType == cmp.Or(dc.EndpointType, "REGIONAL") {
					dc.APIGatewayDomainName, dc.HostedZoneID = old.APIGatewayDomainName, old.HostedZoneID
				}
			}
			dc.EndpointType = cmp.Or(dc.EndpointType, "REGIONAL")
			dc.DomainNameStatus = "AVAILABLE"
			dc.SecurityPolicy = cmp.Or(dc.SecurityPolicy, "TLS_1_2")
			dc.IPAddressType = cmp.Or(dc.IPAddressType, "ipv4")
		}
		if in.DomainNameConfigurations != nil {
			dom.DomainNameConfigurations = in.DomainNameConfigurations
		}
		if in.MutualTlsAuthentication != nil {
			dom.MutualTlsAuthentication = in.MutualTlsAuthentication
		}
		dom.RoutingMode = cmp.Or(in.RoutingMode, dom.RoutingMode)
		return 200, &dom.HTTPDomain, nil
	})
	s.route("DELETE "+d, "DeleteDomainName", func(r *http.Request, c *core.Ctx) (int, any, error) {
		dom, err := s.httpDomain(r, c)
		if err != nil {
			return 0, nil, err
		}
		delete(s.st.HTTPDomains, dom.DomainName)
		return http.StatusNoContent, nil, nil
	})

	const m = d + "/apimappings"
	check := func(dom *httpDomain, am *APIMapping, self string) error {
		for id, other := range dom.Mappings {
			if id != self && other.APIMappingKey == am.APIMappingKey {
				return conflict("The mapping key %q already exists for this domain name.", am.APIMappingKey)
			}
		}
		a := s.st.HTTP[am.APIID]
		switch {
		case a == nil || a.Account != dom.Account || a.Region != dom.Region:
			return notFound("Invalid API identifier specified %s:%s", dom.Account, am.APIID)
		case am.Stage == "":
			return badRequest("Stage is required")
		case a.Stages[am.Stage] == nil:
			return notFound("Invalid stage identifier specified")
		}
		return nil
	}
	s.route("POST "+m, "CreateApiMapping", func(r *http.Request, c *core.Ctx) (int, any, error) {
		dom, err := s.httpDomain(r, c)
		if err != nil {
			return 0, nil, err
		}
		am := &APIMapping{APIMappingID: newID()}
		if err := decode(r, am); err != nil {
			return 0, nil, err
		}
		am.APIMappingKey = strings.Trim(am.APIMappingKey, "/")
		if err := check(dom, am, ""); err != nil {
			return 0, nil, err
		}
		dom.Mappings[am.APIMappingID] = am
		return http.StatusCreated, am, nil
	})
	s.route("GET "+m, "GetApiMappings", func(r *http.Request, c *core.Ctx) (int, any, error) {
		dom, err := s.httpDomain(r, c)
		if err != nil {
			return 0, nil, err
		}
		return 200, items(dom.Mappings, "items"), nil
	})
	mapping := func(r *http.Request, c *core.Ctx) (*httpDomain, *APIMapping, error) {
		dom, err := s.httpDomain(r, c)
		if err != nil {
			return nil, nil, err
		}
		am, err := find(dom.Mappings, r.PathValue("id"), "ApiMapping")
		return dom, am, err
	}
	s.route("GET "+m+"/{id}", "GetApiMapping", func(r *http.Request, c *core.Ctx) (int, any, error) {
		_, am, err := mapping(r, c)
		return 200, am, err
	})
	s.route("PATCH "+m+"/{id}", "UpdateApiMapping", func(r *http.Request, c *core.Ctx) (int, any, error) {
		dom, am, err := mapping(r, c)
		if err != nil {
			return 0, nil, err
		}
		upd := clone(am)
		if err := decode(r, upd); err != nil {
			return 0, nil, err
		}
		upd.APIMappingKey = strings.Trim(upd.APIMappingKey, "/")
		if err := check(dom, upd, am.APIMappingID); err != nil {
			return 0, nil, err
		}
		*am = *upd
		return 200, am, nil
	})
	s.route("DELETE "+m+"/{id}", "DeleteApiMapping", func(r *http.Request, c *core.Ctx) (int, any, error) {
		dom, am, err := mapping(r, c)
		if err != nil {
			return 0, nil, err
		}
		delete(dom.Mappings, am.APIMappingID)
		return http.StatusNoContent, nil, nil
	})
}

// hostName is a Host header without its port, in lower case.
func hostName(host string) string {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	return strings.ToLower(host)
}

// customHost reports whether host is a registered custom domain name.
func (s *Service) customHost(host string) bool {
	host = hostName(host)
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.domainTaken(host)
}

// viaDomain resolves a call that arrived on a custom domain: it picks the
// base path mapping with the longest key that prefixes the path, then sets
// the API id and the path as the API's own host sees it (stage first).
// Lock held.
func (s *Service) viaDomain(x *call) error {
	host := hostName(x.r.Host)
	type target struct{ key, api, stage string }
	var cands []target
	if d := s.st.Domains[host]; d != nil {
		for _, m := range d.Mappings {
			cands = append(cands, target{strings.Trim(strings.TrimPrefix(m.BasePath, none), "/"), m.RestAPIID, m.Stage})
		}
	}
	if d := s.st.HTTPDomains[host]; d != nil {
		for _, m := range d.Mappings {
			cands = append(cands, target{m.APIMappingKey, m.APIID, m.Stage})
		}
	}
	p := strings.TrimPrefix(x.full, "/")
	var best *target
	for i, t := range cands {
		if (t.key == "" || p == t.key || strings.HasPrefix(p, t.key+"/")) && (best == nil || len(t.key) > len(best.key)) {
			best = &cands[i]
		}
	}
	if best == nil {
		return errForbidden
	}
	rest := "/" + strings.TrimPrefix(strings.TrimPrefix(p, best.key), "/")
	x.apiID, x.full = best.api, rest
	if best.stage != "" && best.stage != "$default" {
		x.full = "/" + best.stage + rest
	}
	return nil
}
