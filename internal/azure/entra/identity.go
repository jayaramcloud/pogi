package entra

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/core"
)

// managed is the token endpoint of managed identities: IMDS
// (GET /metadata/identity/oauth2/token, header Metadata: true) and App
// Service (GET $IDENTITY_ENDPOINT, header X-IDENTITY-HEADER: $IDENTITY_HEADER).
// With no client_id, object_id or resource ID it is the system-assigned
// identity (there is one, shared by whoever asks); otherwise the
// user-assigned identity of that name.
// ponytail: the App Service header is not compared to IDENTITY_HEADER, any value will do.
func (s *Service) managed(w http.ResponseWriter, r *http.Request, c *core.Ctx) {
	c.Op = "GetManagedIdentityToken"
	if err := c.Intercept(); err != nil {
		oauthError(w, c, err.(*core.Error).Status, "temporarily_unavailable", 0, err.Error())
		return
	}
	imds := r.URL.Path == "/metadata/identity/oauth2/token"
	switch {
	case imds && r.Header.Get("Metadata") != "true":
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_request", "error_description": "Required metadata header not specified or not correct"})
		return
	case !imds && r.Header.Get("X-IDENTITY-HEADER") == "":
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_request", "error_description": "The X-IDENTITY-HEADER header is required."})
		return
	}
	q := r.URL.Query()
	resource := q.Get("resource")
	if resource == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_request", "error_description": "Required query variable 'resource' is missing"})
		return
	}
	sub := subject{tid: arm.TenantID, resource: resource, v1: true,
		clientID: uuidOf("system-assigned client"), oid: uuidOf("system-assigned principal")}
	if want := firstOf(q, "client_id", "object_id", "principal_id", "msi_res_id", "mi_res_id"); want != "" {
		ident := s.findIdentity(want)
		if ident == nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_request", "error_description": "Identity not found"})
			return
		}
		p, _ := ident["properties"].(map[string]any)
		sub.clientID, sub.oid, sub.mi = str(p["clientId"]), str(p["principalId"]), str(ident["id"])
	}
	now := time.Now()
	writeJSON(w, http.StatusOK, map[string]any{"access_token": s.access(sub), "refresh_token": "", "client_id": sub.clientID,
		"expires_in": strconv.Itoa(int(lifetime.Seconds())), "expires_on": strconv.FormatInt(now.Add(lifetime).Unix(), 10),
		"not_before": strconv.FormatInt(now.Unix(), 10), "resource": resource, "token_type": "Bearer"})
}

func firstOf(q map[string][]string, names ...string) string {
	for _, n := range names {
		if v := q[n]; len(v) > 0 && v[0] != "" {
			return v[0]
		}
	}
	return ""
}

// findIdentity looks a user-assigned identity up by client ID, principal
// ID or resource ID.
func (s *Service) findIdentity(want string) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, d := range s.st.Identities {
		p, _ := d["properties"].(map[string]any)
		if strings.EqualFold(str(p["clientId"]), want) || strings.EqualFold(str(p["principalId"]), want) || strings.EqualFold(str(d["id"]), want) {
			return d
		}
	}
	return nil
}

// identityProvider is Microsoft.ManagedIdentity/userAssignedIdentities in the
// Resource Manager: PUT mints the client and principal IDs the token
// endpoint then answers to.
func (s *Service) identityProvider() arm.Provider {
	key := func(id arm.ID) string { return strings.ToLower(id.String()) }
	return arm.Provider{Type: "Microsoft.ManagedIdentity/userAssignedIdentities",
		Put: func(c *core.Ctx, id arm.ID, body map[string]any) (map[string]any, error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			clientID, principalID := core.UUID(), core.UUID()
			if p, _ := s.st.Identities[key(id)]["properties"].(map[string]any); p != nil {
				clientID, principalID = str(p["clientId"]), str(p["principalId"])
			}
			loc := str(body["location"])
			if loc == "" {
				loc = "eastus"
			}
			doc := arm.Resource(id, loc, body["tags"], map[string]any{"tenantId": arm.TenantID, "principalId": principalID,
				"clientId": clientID, "isolationScope": "None"})
			s.st.Identities[key(id)] = doc
			return doc, nil
		},
		Get: func(c *core.Ctx, id arm.ID) (map[string]any, error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			if d, ok := s.st.Identities[key(id)]; ok {
				return d, nil
			}
			return nil, arm.NotFound(id)
		},
		Delete: func(c *core.Ctx, id arm.ID) error {
			s.mu.Lock()
			delete(s.st.Identities, key(id))
			s.mu.Unlock()
			return nil
		},
		List: func(c *core.Ctx, parent arm.ID) ([]map[string]any, error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			var out []map[string]any
			for _, k := range core.SortedKeys(s.st.Identities) {
				if strings.HasPrefix(k, strings.ToLower(parent.String())+"/") {
					out = append(out, s.st.Identities[k])
				}
			}
			return out, nil
		}}
}
