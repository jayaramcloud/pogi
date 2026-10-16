package apim

import (
	"net/http"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// Resource-manager side. Everything is kept as the resource manager has it, in one Store; the gateway reads the same
// bodies, so a change made through the management plane applies to the next request.

func (s *Service) registerARM() {
	reg := func(typ string, d armres.Stored, noContent ...string) {
		d.Type, d.Mu, d.St = ns+typ, &s.mu, &s.st.ARM
		p := d.Provider()
		p.NoContent = noContent
		arm.Register(s.h, p)
	}
	reg("", armres.Stored{Async: true, Put: s.putService, View: s.serviceView})
	reg("/apis", armres.Stored{View: apiView, Delete: s.deleteAPI})
	reg("/apis/operations", armres.Stored{})
	reg("/apis/schemas", armres.Stored{})
	reg("/backends", armres.Stored{View: func(_ *core.Ctx, r map[string]any) map[string]any {
		armres.Defaults(armres.Props(r), map[string]any{"protocol": "http"})
		return r
	}})
	reg("/products", armres.Stored{View: func(_ *core.Ctx, r map[string]any) map[string]any {
		armres.Defaults(armres.Props(r), map[string]any{"state": "notPublished", "subscriptionRequired": true})
		return r
	}})
	reg("/products/apis", armres.Stored{Put: s.putProductAPI, View: s.productAPIView})
	reg("/namedValues", armres.Stored{
		View: func(_ *core.Ctx, r map[string]any) map[string]any {
			if p := armres.Props(r); p["secret"] == true {
				delete(p, "value") // a secret's value only comes from listValue
			}
			return r
		},
		Actions: map[string]func(*core.Ctx, arm.ID, map[string]any) (any, error){
			"listValue": func(_ *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
				return map[string]any{"value": armres.Props(s.get(id))["value"]}, nil
			}}})
	reg("/subscriptions", armres.Stored{Put: s.putSubscription, View: subscriptionView,
		Actions: map[string]func(*core.Ctx, arm.ID, map[string]any) (any, error){
			"listSecrets": func(_ *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
				p := armres.Props(s.get(id))
				return map[string]any{"primaryKey": p["primaryKey"], "secondaryKey": p["secondaryKey"]}, nil
			},
			"regeneratePrimaryKey":   s.regenerate("primaryKey"),
			"regenerateSecondaryKey": s.regenerate("secondaryKey"),
		}}, "regeneratePrimaryKey", "regenerateSecondaryKey")
	for _, t := range []string{"/policies", "/apis/policies", "/apis/operations/policies", "/products/policies"} {
		reg(t, armres.Stored{Put: putPolicy, View: func(_ *core.Ctx, r map[string]any) map[string]any {
			armres.Defaults(armres.Props(r), map[string]any{"format": "xml"})
			return r
		}})
	}
}

func (s *Service) putService(_ *core.Ctx, id arm.ID, r map[string]any) error {
	p := armres.Props(r)
	armres.Defaults(p, map[string]any{"createdAtUtc": time.Now().UTC().Format(time.RFC3339)})
	master := id.Child("subscriptions", "master") // the all-access subscription every service has
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.st.ARM.Get(master); !ok {
		s.st.ARM.Put(master, armres.Envelope(master, map[string]any{"properties": map[string]any{
			"scope": id.String() + "/", "displayName": "Built-in all-access subscription", "state": "active",
			"primaryKey": core.RandHex(16), "secondaryKey": core.RandHex(16), "createdDate": time.Now().UTC().Format(time.RFC3339)}}))
	}
	return nil
}

func (s *Service) serviceView(c *core.Ctx, r map[string]any) map[string]any {
	name := armres.Str(r, "name")
	armres.Defaults(r, map[string]any{"sku": map[string]any{"name": "Developer", "capacity": 1}})
	gw := armres.URL(c, "https", name+".azure-api", "/_azure/apim/"+name)
	p := armres.Props(r)
	p["provisioningState"], p["targetProvisioningState"] = "Succeeded", ""
	p["gatewayUrl"], p["gatewayRegionalUrl"] = gw, gw
	p["hostnameConfigurations"] = []any{map[string]any{"type": "Proxy", "hostName": armres.HostOf(gw), "defaultSslBinding": true}}
	p["publicIPAddresses"] = []any{"127.0.0.1"}
	return r
}

func apiView(_ *core.Ctx, r map[string]any) map[string]any {
	p := armres.Props(r)
	armres.Defaults(p, map[string]any{"displayName": r["name"], "isCurrent": true, "apiRevision": "1", "subscriptionRequired": true,
		"protocols": []any{"https"}, "subscriptionKeyParameterNames": map[string]any{"header": "Ocp-Apim-Subscription-Key", "query": "subscription-key"}})
	return r
}

// deleteAPI takes the API out of its products.
func (s *Service) deleteAPI(_ *core.Ctx, id arm.ID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k := range s.st.ARM {
		if l, ok := arm.ParseID(k); ok && strings.EqualFold(l.Type(), ns+"/products/apis") && strings.EqualFold(l.Name(), id.Name()) &&
			strings.EqualFold(l.Parent().Parent().String(), id.Parent().String()) {
			delete(s.st.ARM, k)
		}
	}
}

// putProductAPI links an API to a product: PUT products/p/apis/a, with no body.
func (s *Service) putProductAPI(_ *core.Ctx, id arm.ID, _ map[string]any) error {
	api := id.Parent().Parent().Child("apis", id.Name())
	if s.get(api) == nil {
		return arm.NotFound(api)
	}
	return nil
}

// productAPIView shows a product's API as the API.
func (s *Service) productAPIView(c *core.Ctx, r map[string]any) map[string]any {
	l, _ := arm.ParseID(armres.Str(r, "id"))
	if api := s.get(l.Parent().Parent().Child("apis", l.Name())); api != nil {
		return apiView(c, api)
	}
	return r
}

func newKey() string { return core.RandHex(16) }

func (s *Service) putSubscription(_ *core.Ctx, id arm.ID, r map[string]any) error {
	p := armres.Props(r)
	s.mu.Lock()
	defer s.mu.Unlock()
	old, _ := s.st.ARM.Get(id)
	op := armres.Props(old)
	for _, k := range []string{"primaryKey", "secondaryKey", "createdDate"} { // a PUT keeps what it does not set
		if v, ok := op[k]; ok {
			armres.Defaults(p, map[string]any{k: v})
		}
	}
	armres.Defaults(p, map[string]any{"displayName": id.Name(), "state": "active", "allowTracing": true, "scope": id.Parent().String() + "/apis",
		"primaryKey": newKey(), "secondaryKey": newKey(), "createdDate": time.Now().UTC().Format(time.RFC3339)})
	if sc := armres.Str(p, "scope"); strings.HasPrefix(sc, "/") && !strings.Contains(sc, "/service/") { // Azure answers with the full path
		p["scope"] = id.Parent().String() + strings.TrimSuffix(sc, "/")
	}
	return nil
}

// subscriptionView leaves the keys out: they come from listSecrets.
func subscriptionView(_ *core.Ctx, r map[string]any) map[string]any {
	p := armres.Props(r)
	delete(p, "primaryKey")
	delete(p, "secondaryKey")
	return r
}

func (s *Service) regenerate(field string) func(*core.Ctx, arm.ID, map[string]any) (any, error) {
	return func(_ *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		if r, ok := s.st.ARM.Get(id); ok {
			armres.Props(r)[field] = newKey()
			s.st.ARM.Put(id, r)
		}
		return nil, nil
	}
}

// putPolicy checks that the policy document is XML with a <policies> root.
func putPolicy(_ *core.Ctx, id arm.ID, r map[string]any) error {
	if !strings.EqualFold(id.Name(), "policy") {
		return core.Errorf(http.StatusBadRequest, "ValidationError", "The policy name must be 'policy'.")
	}
	p := armres.Props(r)
	if f := armres.Str(p, "format"); strings.HasSuffix(f, "-link") {
		return nil
	}
	if _, err := parsePolicy(armres.Str(p, "value"), ""); err != nil {
		return core.Errorf(http.StatusBadRequest, "ValidationError", "The policy document is not valid: %v", err)
	}
	return nil
}
