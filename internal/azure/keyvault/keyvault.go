// Package keyvault emulates the Azure Key Vault data plane: secrets, keys
// (RSA, EC and oct, with real stdlib cryptography) and self-signed
// certificates, with versions, soft delete, recovery, purge and
// backup/restore. Vaults are named by host: https://<vault>.vault.localhost:7644
// is vault <vault>; any other host (https://localhost:7644) is "default".
// Clients set DisableChallengeResourceVerification and use any bearer-token
// credential: anonymous requests get Key Vault's authentication challenge and
// every token is accepted.
package keyvault

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// Service is the Key Vault emulator.
type Service struct {
	hub *core.Hub
	mux *http.ServeMux
	mu  sync.Mutex
	st  state
}

type state struct {
	Vaults map[string]Vault `json:"vaults"`
	ARM    armres.Store     `json:"arm,omitempty"` // vaults as the resource manager has them
}

// Vault maps each collection (secrets, keys, certificates) to its objects by
// lower-cased name: Key Vault names are case-insensitive.
type Vault map[string]map[string]*Object

// Object is a secret, key or certificate with its versions, oldest first.
type Object struct {
	Name     string          `json:"name"`
	Versions []*Version      `json:"versions"`
	Deleted  int64           `json:"deleted,omitempty"` // soft-deleted at, epoch seconds
	Managed  bool            `json:"managed,omitempty"` // the key or secret behind a certificate
	Rotation *rotationPolicy `json:"rotation,omitempty"`
	Policy   json.RawMessage `json:"policy,omitempty"` // certificate policy as created
}

// Version is one version of an object; the fields in use depend on the collection.
type Version struct {
	ID          string            `json:"id"`
	Attributes  Attributes        `json:"attributes"`
	Tags        map[string]string `json:"tags,omitempty"`
	ContentType string            `json:"contentType,omitempty"` // secrets, certificates
	Value       string            `json:"value,omitempty"`       // secrets
	Kty         string            `json:"kty,omitempty"`         // keys
	Ops         []string          `json:"key_ops,omitempty"`     // keys
	Private     []byte            `json:"private,omitempty"`     // keys: PKCS#8 DER, or the oct key itself
	Cer         []byte            `json:"cer,omitempty"`         // certificates: DER
}

// Attributes are a version's REST attributes; times are epoch seconds.
type Attributes struct {
	Enabled         bool   `json:"enabled"`
	NotBefore       int64  `json:"nbf,omitempty"`
	Expires         int64  `json:"exp,omitempty"`
	Created         int64  `json:"created"`
	Updated         int64  `json:"updated"`
	RecoveryLevel   string `json:"recoveryLevel"`
	RecoverableDays int    `json:"recoverableDays"`
}

// attrsIn is the writable part of Attributes in requests.
type attrsIn struct {
	Enabled   *bool  `json:"enabled"`
	NotBefore *int64 `json:"nbf"`
	Expires   *int64 `json:"exp"`
}

func (a *Attributes) apply(in attrsIn) {
	if in.Enabled != nil {
		a.Enabled = *in.Enabled
	}
	if in.NotBefore != nil {
		a.NotBefore = *in.NotBefore
	}
	if in.Expires != nil {
		a.Expires = *in.Expires
	}
	a.Updated = time.Now().Unix()
}

func (o *Object) latest() *Version { return o.Versions[len(o.Versions)-1] }

var nouns = map[string]string{"secrets": "Secret", "keys": "Key", "certificates": "Certificate"}

// New creates the Key Vault service.
func New(h *core.Hub) *Service {
	s := &Service{mux: http.NewServeMux()}
	s.Reset()
	s.registerARM(h)
	for coll, n := range nouns {
		s.handle("GET /"+coll, "Get"+n+"s", coll, s.list(false))
		s.handle("GET /"+coll+"/{name}", "Get"+n, coll, s.get)
		s.handle("GET /"+coll+"/{name}/{version}", "Get"+n, coll, s.get)
		s.handle("GET /"+coll+"/{name}/versions", "Get"+n+"Versions", coll, s.versions)
		s.handle("PATCH /"+coll+"/{name}", "Update"+n, coll, s.update)
		s.handle("PATCH /"+coll+"/{name}/{version}", "Update"+n, coll, s.update)
		s.handle("DELETE /"+coll+"/{name}", "Delete"+n, coll, s.softDelete)
		s.handle("GET /deleted"+coll, "GetDeleted"+n+"s", coll, s.list(true))
		s.handle("GET /deleted"+coll+"/{name}", "GetDeleted"+n, coll, s.getDeleted)
		s.handle("POST /deleted"+coll+"/{name}/recover", "RecoverDeleted"+n, coll, s.undelete)
		s.handle("DELETE /deleted"+coll+"/{name}", "PurgeDeleted"+n, coll, s.purge)
		if coll != "certificates" {
			s.handle("POST /"+coll+"/{name}/backup", "Backup"+n, coll, s.backup)
			s.handle("POST /"+coll+"/restore", "Restore"+n, coll, s.restore)
		}
	}
	s.handle("PUT /secrets/{name}", "SetSecret", "secrets", s.setSecret)
	s.handle("POST /keys/{name}/create", "CreateKey", "keys", s.createKey)
	s.handle("PUT /keys/{name}", "ImportKey", "keys", s.importKey)
	s.handle("POST /keys/{name}/rotate", "RotateKey", "keys", s.rotateKey)
	s.handle("GET /keys/{name}/rotationpolicy", "GetKeyRotationPolicy", "keys", s.getRotation)
	s.handle("PUT /keys/{name}/rotationpolicy", "UpdateKeyRotationPolicy", "keys", s.setRotation)
	for p, op := range map[string]string{"encrypt": "Encrypt", "decrypt": "Decrypt", "sign": "Sign", "verify": "Verify", "wrapkey": "WrapKey", "unwrapkey": "UnwrapKey"} {
		s.handle("POST /keys/{name}/"+p, op, "keys", s.keyOp) // the SDKs drop an empty version
		s.handle("POST /keys/{name}/{version}/"+p, op, "keys", s.keyOp)
	}
	s.handle("POST /rng", "GetRandomBytes", "", s.rng)
	s.handle("POST /certificates/{name}/create", "CreateCertificate", "certificates", s.createCert)
	s.handle("GET /certificates/{name}/pending", "GetCertificateOperation", "certificates", s.certOperation)
	s.handle("GET /certificates/{name}/policy", "GetCertificatePolicy", "certificates", s.certPolicy)
	s.handle("/", "", "", func(*call) (any, error) {
		return nil, badParameter("pogi does not implement this Key Vault operation.")
	})
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "keyvault", Cloud: core.Azure, Title: "Key Vault", Protocol: "azure-json", Match: match}
}

// match claims Key Vault data-plane paths that carry an api-version and are
// anonymous or bearer-authenticated (not AWS- or HMAC-signed).
func match(r *http.Request) bool {
	first, _, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	switch first {
	case "secrets", "deletedsecrets", "keys", "deletedkeys", "certificates", "deletedcertificates":
	default:
		if r.URL.Path != "/rng" {
			return false
		}
	}
	return r.URL.Query().Has("api-version") && (r.Header.Get("Authorization") == "" || bearer(r))
}

func bearer(r *http.Request) bool {
	scheme, _, _ := strings.Cut(r.Header.Get("Authorization"), " ")
	return strings.EqualFold(scheme, "Bearer")
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// SDKs ask for "/secrets/name/" when the version is empty (the latest).
	if p := path.Clean(r.URL.Path); p != r.URL.Path {
		r = r.Clone(r.Context())
		r.URL.Path, r.URL.RawPath = p, ""
	}
	s.mux.ServeHTTP(w, r)
}

// challenge answers anonymous requests; SDKs take the tenant and scope from it.
const challenge = `Bearer authorization="https://login.microsoftonline.com/00000000-0000-0000-0000-000000000000", ` +
	`resource="https://vault.azure.net", scope="https://vault.azure.net/.default"`

// call is one request: the object it names and what its handler needs.
type call struct {
	vault, base   string // vault name; scheme://host for object ids
	coll, op      string // collection; operation
	name, version string // from the path; version "" is the latest
	path          string
	query         url.Values
	body          []byte
	status        int
}

// handle registers an operation. Every one needs a bearer token (any will do).
// ponytail: one lock for the whole service, RSA key generation included;
// per-vault locks if that ever stalls anybody.
func (s *Service) handle(pattern, op, coll string, fn func(*call) (any, error)) {
	s.mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		ctx := core.C(r)
		ctx.Op = op
		if !bearer(r) {
			w.Header().Set("WWW-Authenticate", challenge)
			reply(w, ctx, 0, nil, core.Errorf(http.StatusUnauthorized, "Unauthorized", "AKV10000: Request is missing a Bearer or PoP token."))
			return
		}
		if err := ctx.Intercept(); err != nil {
			reply(w, ctx, 0, nil, err)
			return
		}
		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}
		c := &call{vault: vaultName(r.Host), base: scheme + "://" + r.Host, coll: coll, op: op,
			name: r.PathValue("name"), version: r.PathValue("version"),
			path: r.URL.Path, query: r.URL.Query(), body: core.Body(r), status: http.StatusOK}
		out, err := func() (any, error) {
			s.mu.Lock()
			defer s.mu.Unlock() // a panicking handler must not wedge the service
			return fn(c)
		}()
		reply(w, ctx, c.status, out, err)
	})
}

// reply writes a JSON result or a Key Vault error: {"error":{"code","message"}}.
func reply(w http.ResponseWriter, c *core.Ctx, status int, out any, err error) {
	if err != nil {
		e := core.AsError(err)
		c.ErrCode = e.Code
		status, out = e.Status, map[string]any{"error": map[string]string{"code": e.Code, "message": e.Msg}}
	}
	w.Header().Set("x-ms-request-id", c.RequestID)
	if status == http.StatusNoContent {
		w.WriteHeader(status)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(out)
}

// vaultName is the first label of "<vault>.vault.<…>" hosts, else "default".
func vaultName(host string) string {
	name, rest, _ := strings.Cut(strings.ToLower(host), ".")
	if strings.HasPrefix(rest, "vault.") {
		return name
	}
	return "default"
}

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{Vaults: map[string]Vault{}}
	s.mu.Unlock()
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

func (s *Service) Load(b []byte) error {
	var st state
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	if st.Vaults == nil {
		st.Vaults = map[string]Vault{}
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

func badParameter(format string, a ...any) error {
	return core.Errorf(http.StatusBadRequest, "BadParameter", format, a...)
}

func forbidden(format string, a ...any) error {
	return core.Errorf(http.StatusForbidden, "Forbidden", format, a...)
}

func notFound(coll, id string) error {
	n := nouns[coll]
	l := strings.ToLower(n)
	return core.Errorf(http.StatusNotFound, n+"NotFound", "A %s with (name/id) %s was not found in this key vault. If you recently deleted this %s "+
		"you may be able to recover it using the correct recovery command.", l, id, l)
}

func conflict(coll, name string) error {
	n := nouns[coll]
	return core.Errorf(http.StatusConflict, "Conflict", "%s %s is currently in a deleted but recoverable state, and its name cannot be reused; "+
		"in this state, the %s can only be recovered or purged.", n, name, strings.ToLower(n))
}

// b64 is the unpadded base64url Key Vault uses for binary values.
func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

// unb64 decodes base64url, tolerating padding and the standard alphabet.
func unb64(s string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(strings.TrimRight(strings.NewReplacer("+", "-", "/", "_").Replace(s), "="))
}

func (c *call) decode(v any) error {
	if len(c.body) == 0 {
		return nil
	}
	if err := json.Unmarshal(c.body, v); err != nil {
		return badParameter("Invalid request body: %v", err)
	}
	return nil
}

// id is an object id: base/coll/name[/version].
func (c *call) id(coll, name, version string) string {
	if version != "" {
		name += "/" + version
	}
	return c.base + "/" + coll + "/" + name
}

// find returns a live (or soft-deleted) object and its version, the latest
// when c.version is ""; lock held.
func (s *Service) find(c *call, deleted bool) (*Object, *Version, error) {
	o := s.st.Vaults[c.vault][c.coll][strings.ToLower(c.name)]
	if o == nil || (o.Deleted != 0) != deleted {
		return nil, nil, notFound(c.coll, c.name)
	}
	if c.version == "" {
		return o, o.latest(), nil
	}
	for _, v := range o.Versions {
		if strings.EqualFold(v.ID, c.version) {
			return o, v, nil
		}
	}
	return nil, nil, notFound(c.coll, c.name+"/"+c.version)
}

// objects returns a vault collection for writing, creating it; lock held.
func (s *Service) objects(vault, coll string) map[string]*Object {
	if s.st.Vaults[vault] == nil {
		s.st.Vaults[vault] = Vault{}
	}
	if s.st.Vaults[vault][coll] == nil {
		s.st.Vaults[vault][coll] = map[string]*Object{}
	}
	return s.st.Vaults[vault][coll]
}

var validName = regexp.MustCompile(`^[0-9a-zA-Z-]{1,127}$`)

// add appends v as the newest version of name, creating the object; lock held.
func (s *Service) add(vault, coll, name string, v *Version, a attrsIn) (*Object, error) {
	if !validName.MatchString(name) {
		return nil, badParameter("The request URI contains an invalid name: %s", name)
	}
	objs := s.objects(vault, coll)
	o := objs[strings.ToLower(name)]
	switch {
	case o == nil:
		o = &Object{Name: name}
		objs[strings.ToLower(name)] = o
	case o.Deleted != 0:
		return nil, conflict(coll, o.Name)
	}
	if v.ID == "" {
		v.ID = core.NewID()
	}
	v.Attributes = Attributes{Enabled: true, Created: time.Now().Unix(), RecoveryLevel: "Recoverable+Purgeable", RecoverableDays: 90}
	v.Attributes.apply(a)
	o.Versions = append(o.Versions, v)
	return o, nil
}

// cascade applies f to the key and secret behind a certificate; lock held.
func (s *Service) cascade(c *call, f func(objs map[string]*Object, key string)) {
	if c.coll != "certificates" {
		return
	}
	key := strings.ToLower(c.name)
	for _, coll := range []string{"keys", "secrets"} {
		if objs := s.st.Vaults[c.vault][coll]; objs[key] != nil && objs[key].Managed {
			f(objs, key)
		}
	}
}

// item renders a version as list operations do: the id carries the version
// only when versioned; soft-deleted objects get their recovery fields.
func (c *call) item(o *Object, v *Version, versioned bool) map[string]any {
	ver := ""
	if versioned {
		ver = v.ID
	}
	idKey := "id"
	if c.coll == "keys" {
		idKey = "kid"
	}
	m := map[string]any{idKey: c.id(c.coll, o.Name, ver), "attributes": v.Attributes}
	if len(v.Tags) > 0 {
		m["tags"] = v.Tags
	}
	if v.ContentType != "" {
		m["contentType"] = v.ContentType
	}
	if o.Managed {
		m["managed"] = true
	}
	if v.Cer != nil {
		m["x5t"] = thumbprint(v.Cer)
	}
	if o.Deleted != 0 {
		m["recoveryId"] = c.base + "/deleted" + c.coll + "/" + o.Name
		m["deletedDate"] = o.Deleted
		m["scheduledPurgeDate"] = o.Deleted + 90*24*3600
	}
	return m
}

// bundle renders a version as GET does; value adds a secret's value.
func (c *call) bundle(o *Object, v *Version, value bool) map[string]any {
	m := c.item(o, v, true)
	switch c.coll {
	case "secrets":
		if value {
			m["value"] = v.Value
		}
		if o.Managed {
			m["kid"] = c.id("keys", o.Name, v.ID)
		}
	case "keys":
		m["key"] = jwk(m["kid"].(string), v)
		delete(m, "kid")
	case "certificates":
		m["kid"], m["sid"], m["cer"] = c.id("keys", o.Name, v.ID), c.id("secrets", o.Name, v.ID), v.Cer
		m["policy"] = c.policy(o)
	}
	return m
}

// page returns one page of items; nextLink carries the next offset.
func (c *call) page(items []any) map[string]any {
	skip, _ := strconv.Atoi(c.query.Get("$skiptoken"))
	size, _ := strconv.Atoi(c.query.Get("maxresults"))
	if size < 1 || size > 25 {
		size = 25
	}
	skip = max(0, min(skip, len(items)))
	end := min(skip+size, len(items))
	out := map[string]any{"value": items[skip:end], "nextLink": nil}
	if end < len(items) {
		q := url.Values{"api-version": {c.query.Get("api-version")}, "$skiptoken": {strconv.Itoa(end)}, "maxresults": {strconv.Itoa(size)}}
		out["nextLink"] = c.base + c.path + "?" + q.Encode()
	}
	return out
}

// list pages a collection's live (or soft-deleted) objects by name.
func (s *Service) list(deleted bool) func(*call) (any, error) {
	return func(c *call) (any, error) {
		objs := s.st.Vaults[c.vault][c.coll]
		items := []any{}
		for _, k := range core.SortedKeys(objs) {
			if o := objs[k]; (o.Deleted != 0) == deleted {
				items = append(items, c.item(o, o.latest(), false))
			}
		}
		return c.page(items), nil
	}
}

func (s *Service) versions(c *call) (any, error) {
	items := []any{}
	if o, _, err := s.find(c, false); err == nil {
		for _, v := range o.Versions {
			items = append(items, c.item(o, v, true))
		}
	}
	return c.page(items), nil
}

func (s *Service) get(c *call) (any, error) {
	o, v, err := s.find(c, false)
	if err != nil {
		return nil, err
	}
	if c.coll == "secrets" && !v.Attributes.Enabled {
		return nil, forbidden("Operation get is not allowed on a disabled secret.")
	}
	return c.bundle(o, v, true), nil
}

func (s *Service) getDeleted(c *call) (any, error) {
	o, v, err := s.find(c, true)
	if err != nil {
		return nil, err
	}
	return c.bundle(o, v, false), nil
}

func (s *Service) update(c *call) (any, error) {
	var in struct {
		ContentType *string           `json:"contentType"`
		Attributes  attrsIn           `json:"attributes"`
		Tags        map[string]string `json:"tags"`
		KeyOps      []string          `json:"key_ops"`
	}
	if err := c.decode(&in); err != nil {
		return nil, err
	}
	o, v, err := s.find(c, false)
	if err != nil {
		return nil, err
	}
	if in.ContentType != nil {
		v.ContentType = *in.ContentType
	}
	if in.Tags != nil {
		v.Tags = in.Tags
	}
	if in.KeyOps != nil {
		v.Ops = in.KeyOps
	}
	v.Attributes.apply(in.Attributes)
	return c.bundle(o, v, false), nil
}

// softDelete keeps the object, all versions, for recovery or purge.
// ponytail: nothing purges on scheduledPurgeDate; soft-deleted objects stay until purged.
func (s *Service) softDelete(c *call) (any, error) {
	o, v, err := s.find(c, false)
	if err != nil {
		return nil, err
	}
	o.Deleted = time.Now().Unix()
	s.cascade(c, func(objs map[string]*Object, k string) { objs[k].Deleted = o.Deleted })
	return c.bundle(o, v, false), nil
}

func (s *Service) undelete(c *call) (any, error) {
	o, v, err := s.find(c, true)
	if err != nil {
		return nil, err
	}
	o.Deleted = 0
	s.cascade(c, func(objs map[string]*Object, k string) { objs[k].Deleted = 0 })
	return c.bundle(o, v, false), nil
}

func (s *Service) purge(c *call) (any, error) {
	if _, _, err := s.find(c, true); err != nil {
		return nil, err
	}
	delete(s.st.Vaults[c.vault][c.coll], strings.ToLower(c.name))
	s.cascade(c, func(objs map[string]*Object, k string) { delete(objs, k) })
	c.status = http.StatusNoContent
	return nil, nil
}

// backupBlob is the opaque backup blob: plain JSON, as pogi has no HSM to seal it.
type backupBlob struct {
	Coll   string  `json:"coll"`
	Object *Object `json:"object"`
}

func (s *Service) backup(c *call) (any, error) {
	o, _, err := s.find(c, false)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(backupBlob{c.coll, o})
	return map[string]string{"value": b64(b)}, err
}

func (s *Service) restore(c *call) (any, error) {
	var in struct{ Value string }
	if err := c.decode(&in); err != nil {
		return nil, err
	}
	var b backupBlob
	raw, err := unb64(in.Value)
	if err != nil || json.Unmarshal(raw, &b) != nil || b.Coll != c.coll || b.Object == nil ||
		len(b.Object.Versions) == 0 || slices.Contains(b.Object.Versions, nil) || !validName.MatchString(b.Object.Name) {
		return nil, badParameter("Backup blob contains invalid or corrupt version.")
	}
	o, objs := b.Object, s.objects(c.vault, c.coll)
	if objs[strings.ToLower(o.Name)] != nil {
		return nil, core.Errorf(http.StatusConflict, "Conflict", "%s %s already exists; restore needs the name to be free.", nouns[c.coll], o.Name)
	}
	o.Deleted = 0
	objs[strings.ToLower(o.Name)] = o
	return c.bundle(o, o.latest(), false), nil
}

func (s *Service) setSecret(c *call) (any, error) {
	var in struct {
		Value       *string           `json:"value"`
		ContentType string            `json:"contentType"`
		Attributes  attrsIn           `json:"attributes"`
		Tags        map[string]string `json:"tags"`
	}
	if err := c.decode(&in); err != nil {
		return nil, err
	}
	if in.Value == nil {
		return nil, badParameter("The secret value is required.")
	}
	o, err := s.add(c.vault, "secrets", c.name, &Version{Value: *in.Value, ContentType: in.ContentType, Tags: in.Tags}, in.Attributes)
	if err != nil {
		return nil, err
	}
	return c.bundle(o, o.latest(), true), nil
}

// Resources lists live objects; soft-deleted ones are hidden as in Azure.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	tables := map[string]*core.Table{
		"secrets":      {Name: "Secrets", Columns: []string{"Vault", "Name", "Versions", "Enabled", "Updated"}},
		"keys":         {Name: "Keys", Columns: []string{"Vault", "Name", "Type", "Versions"}},
		"certificates": {Name: "Certificates", Columns: []string{"Vault", "Name", "Versions", "Expires"}},
	}
	for _, vault := range core.SortedKeys(s.st.Vaults) {
		for coll, t := range tables {
			objs := s.st.Vaults[vault][coll]
			for _, k := range core.SortedKeys(objs) {
				o := objs[k]
				if o.Deleted != 0 {
					continue
				}
				l, n := o.latest(), strconv.Itoa(len(o.Versions))
				switch coll {
				case "secrets":
					t.Rows = append(t.Rows, []string{vault, o.Name, n, strconv.FormatBool(l.Attributes.Enabled), time.Unix(l.Attributes.Updated, 0).UTC().Format(time.DateTime)})
				case "keys":
					t.Rows = append(t.Rows, []string{vault, o.Name, l.Kty, n})
				default:
					t.Rows = append(t.Rows, []string{vault, o.Name, n, time.Unix(l.Attributes.Expires, 0).UTC().Format(time.DateTime)})
				}
			}
		}
	}
	return []core.Table{*tables["secrets"], *tables["keys"], *tables["certificates"]}
}

// Inspect returns metadata, never secret values or key material: of one
// object when id is "vault/name", else of every object of the table in the
// vault named by id (the tables' first column).
func (s *Service) Inspect(table, id string) any {
	coll := strings.ToLower(table)
	vault, name, one := strings.Cut(id, "/")
	s.mu.Lock()
	defer s.mu.Unlock()
	objs := s.st.Vaults[vault][coll]
	var out []any
	for _, k := range core.SortedKeys(objs) {
		if one && k != strings.ToLower(name) {
			continue
		}
		o := objs[k]
		var vs []Version
		for _, v := range o.Versions {
			m := *v
			m.Value, m.Private, m.Cer = "", nil, nil
			vs = append(vs, m)
		}
		info := map[string]any{"vault": vault, "name": o.Name, "versions": vs, "managed": o.Managed, "deleted": o.Deleted}
		if one {
			return info
		}
		out = append(out, info)
	}
	if out == nil {
		return nil
	}
	return out
}
