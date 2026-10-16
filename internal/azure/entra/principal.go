package entra

import (
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// Caller is who a bearer token is for, as the Azure authorizer needs to know it.
type Caller struct {
	OID    string // object ID: what role assignments name
	Name   string // a label: the managed identity's name, the app's client ID, the user's name
	Kind   string // user, servicePrincipal or managedIdentity
	Client string // client ID of an app or managed identity
	// Default is a caller pogi seeds and never checks: the admin user device and CLI logins sign in
	// as, and the shared system-assigned identity.
	Default bool
}

// CallerOf reads the caller from an Authorization header value. It is false for no token, a token
// pogi did not sign, and one that has expired.
func CallerOf(h *core.Hub, auth string) (Caller, bool) {
	scheme, tok, _ := strings.Cut(auth, " ")
	s, _ := h.Service("entra").(*Service)
	if s == nil || !strings.EqualFold(scheme, "Bearer") {
		return Caller{}, false
	}
	claims, ok := s.verify(tok)
	if !ok || str(claims["oid"]) == "" {
		return Caller{}, false
	}
	c := Caller{OID: str(claims["oid"]), Client: str(claims["appid"])}
	if c.Client == "" {
		c.Client = str(claims["azp"])
	}
	switch mi := str(claims["xms_mirid"]); {
	case mi != "":
		c.Kind, c.Name = "managedIdentity", mi[strings.LastIndexByte(mi, '/')+1:]
	case claims["idtyp"] == "app":
		c.Kind, c.Name = "servicePrincipal", c.Client
		c.Default = c.OID == uuidOf("system-assigned principal")
	default:
		c.Kind, c.Name = "user", str(claims["upn"])
		c.Default = c.Name == defaultUser
	}
	return c, true
}

// Known reports whether the caller was created by the user: an app or service principal in the
// directory, or a user-assigned managed identity. Tokens for a client ID the directory has never
// seen (any client ID signs in, unless signature validation is on) are not known.
func Known(h *core.Hub, c Caller) bool {
	s, _ := h.Service("entra").(*Service)
	if s == nil || c.Default {
		return false
	}
	switch c.Kind {
	case "managedIdentity":
		return s.findIdentity(c.OID) != nil
	case "servicePrincipal":
		if d := s.dir(); d != nil {
			oid, _, ok := d.App(c.Client)
			return ok && strings.EqualFold(oid, c.OID)
		}
	}
	return false
}
