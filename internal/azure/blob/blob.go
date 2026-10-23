// Package blob emulates Azure Blob Storage on Azurite's port (10000):
// containers, block/append/page blobs, block lists, ranges, metadata,
// properties, tags, leases, snapshots, copies and CORS. Connect with
// "UseDevelopmentStorage=true" or a connection string whose BlobEndpoint is
// http://127.0.0.1:10000/devstoreaccount1. Shared Key and SAS are accepted
// without signature checks.
package blob

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// Service is the Blob Storage emulator.
type Service struct {
	hub     *core.Hub
	mu      sync.Mutex
	st      state
	lastTag int64
}

type state struct {
	Accounts map[string]*Account `json:"accounts"`
	ARM      armres.Store        `json:"arm,omitempty"` // storage accounts as the resource manager has them
}

// Account is a storage account (devstoreaccount1 by default).
type Account struct {
	Containers map[string]*Container `json:"containers"`
	Props      string                `json:"props,omitempty"` // service properties XML
}

// Container holds blobs.
type Container struct {
	Name     string            `json:"name"`
	Created  time.Time         `json:"created"`
	Modified time.Time         `json:"modified"`
	ETag     string            `json:"etag"`
	Meta     map[string]string `json:"meta,omitempty"`
	Public   string            `json:"public,omitempty"`
	ACL      string            `json:"acl,omitempty"`
	Lease    Lease             `json:"lease"`
	Blobs    map[string]*Blob  `json:"blobs"`
}

// Blob is a block, append or page blob.
type Blob struct {
	Name        string            `json:"name"`
	Type        string            `json:"type"`
	Data        []byte            `json:"data,omitempty"`
	Props       map[string]string `json:"props,omitempty"` // Content-Type, Content-MD5, …
	Meta        map[string]string `json:"meta,omitempty"`
	Tags        map[string]string `json:"tags,omitempty"`
	Created     time.Time         `json:"created"`
	Modified    time.Time         `json:"modified"`
	ETag        string            `json:"etag"`
	Tier        string            `json:"tier,omitempty"`
	Committed   []Block           `json:"committed,omitempty"`
	Uncommitted map[string][]byte `json:"uncommitted,omitempty"`
	Appends     int               `json:"appends,omitempty"`
	Lease       Lease             `json:"lease"`
	Snapshots   map[string][]byte `json:"snapshots,omitempty"`
	Copy        map[string]string `json:"copy,omitempty"`    // x-ms-copy-* headers
	Staged      bool              `json:"staged,omitempty"`  // only uncommitted blocks so far
	Pending     map[int64][]byte  `json:"pending,omitempty"` // Data Lake: appended chunks by position, until a flush commits them
	POSIX       map[string]string `json:"posix,omitempty"`   // Data Lake: owner, group, permissions and acl
}

// Block is a committed block.
type Block struct {
	ID   string `json:"id"`
	Size int    `json:"size"`
}

// Lease is a blob or container lease.
type Lease struct {
	ID       string    `json:"id,omitempty"`
	Until    time.Time `json:"until,omitzero"` // zero means infinite while leased
	Duration int       `json:"duration,omitempty"`
	Broken   bool      `json:"broken,omitempty"`
}

func (l *Lease) active() bool {
	return l.ID != "" && !l.Broken && (l.Until.IsZero() || time.Now().Before(l.Until))
}

func (l *Lease) state() string {
	switch {
	case l.ID == "":
		return "available"
	case l.Broken:
		return "broken"
	case !l.active():
		return "expired"
	}
	return "leased"
}

// Port is Azurite's blob port.
const Port = 10000

// New creates the Blob Storage service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub}
	s.Reset()
	s.registerARM()
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "azblob", Cloud: core.Azure, Title: "Blob Storage", Protocol: "azure-xml", Match: armres.HostMatch("blob")}
}

func (s *Service) Port() int { return Port }

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{Accounts: map[string]*Account{}}
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
	if st.Accounts == nil {
		st.Accounts = map[string]*Account{}
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

func (s *Service) etag() string {
	t := time.Now().UnixNano()
	if t <= s.lastTag {
		t = s.lastTag + 1
	}
	s.lastTag = t
	return fmt.Sprintf(`"0x%X"`, t)
}

// azError writes the Azure Storage XML error with x-ms-error-code.
func azError(w http.ResponseWriter, r *http.Request, c *core.Ctx, err error) {
	e := core.AsError(err)
	c.ErrCode = e.Code
	w.Header().Set("x-ms-error-code", e.Code)
	if r.Method == http.MethodHead || e.Status == http.StatusNotModified {
		w.WriteHeader(e.Status) // no Content-Type: the SDK would parse the empty body as XML (the Functions host fails on that)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(e.Status)
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="utf-8"?><Error><Code>` + e.Code + `</Code><Message>`)
	xml.EscapeText(&b, []byte(e.Msg+"\nRequestId:"+c.RequestID+"\nTime:"+time.Now().UTC().Format("2006-01-02T15:04:05.0000000Z")))
	b.WriteString(`</Message></Error>`)
	io.WriteString(w, b.String())
}

var (
	errNoContainer = func() error { return core.Errorf(404, "ContainerNotFound", "The specified container does not exist.") }
	errNoBlob      = func() error { return core.Errorf(404, "BlobNotFound", "The specified blob does not exist.") }
	errCondition   = func() error {
		return core.Errorf(412, "ConditionNotMet", "The condition specified using HTTP conditional header(s) is not met.")
	}
)

type handler func() error

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	r = armres.AccountPath(r, "blob")
	c := core.C(r)
	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 3)
	account, container, name := parts[0], "", ""
	if len(parts) > 1 {
		container = parts[1]
	}
	if len(parts) > 2 {
		name = parts[2]
	}
	h := w.Header()
	h.Set("x-ms-request-id", c.RequestID)
	h.Set("x-ms-version", or(r.Header.Get("x-ms-version"), "2025-01-05"))
	h.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	if id := r.Header.Get("x-ms-client-request-id"); id != "" {
		h.Set("x-ms-client-request-id", id)
	}
	if r.Header.Get("Origin") != "" && s.cors(w, r, account) {
		return
	}
	op, fn := s.route(w, r, c, account, container, name)
	c.Op = op
	if fn == nil {
		azError(w, r, c, core.Errorf(400, "UnsupportedHttpVerb", "pogi does not implement %s %s", r.Method, r.URL.RequestURI()))
		return
	}
	if err := c.Intercept(); err != nil {
		azError(w, r, c, err)
		return
	}
	if err := fn(); err != nil {
		azError(w, r, c, err)
	}
}

func (s *Service) route(w http.ResponseWriter, r *http.Request, c *core.Ctx, account, container, name string) (string, handler) {
	q := r.URL.Query()
	comp, restype, m := q.Get("comp"), q.Get("restype"), r.Method
	get := m == http.MethodGet || m == http.MethodHead
	switch {
	case container == "":
		switch {
		case get && comp == "list":
			return "ListContainers", func() error { return s.listContainers(w, r, account) }
		case restype == "service" && comp == "properties" && get:
			return "GetServiceProperties", func() error { return s.serviceProps(w, account, nil) }
		case restype == "service" && comp == "properties" && m == http.MethodPut:
			return "SetServiceProperties", func() error { return s.serviceProps(w, account, core.Body(r)) }
		case restype == "account" && comp == "properties":
			return "GetAccountInfo", func() error {
				w.Header().Set("x-ms-sku-name", "Standard_RAGRS")
				w.Header().Set("x-ms-account-kind", "StorageV2")
				w.Header().Set("x-ms-is-hns-enabled", strconv.FormatBool(s.hns(account)))
				w.WriteHeader(http.StatusOK)
				return nil
			}
		case restype == "service" && comp == "stats":
			return "GetServiceStats", func() error {
				w.Header().Set("Content-Type", "application/xml")
				io.WriteString(w, `<?xml version="1.0" encoding="utf-8"?><StorageServiceStats><GeoReplication><Status>live</Status><LastSyncTime>`+time.Now().UTC().Format(http.TimeFormat)+`</LastSyncTime></GeoReplication></StorageServiceStats>`)
				return nil
			}
		}
	case name == "":
		if restype != "container" {
			return "", nil
		}
		switch {
		case m == http.MethodPut && comp == "":
			return "CreateContainer", func() error { return s.createContainer(w, r, account, container) }
		case m == http.MethodPut && comp == "metadata":
			return "SetContainerMetadata", func() error {
				return s.withContainer(account, container, func(ct *Container) error {
					ct.Meta, ct.ETag, ct.Modified = meta(r.Header), s.etag(), time.Now().UTC()
					return containerHeaders(w, ct, http.StatusOK)
				})
			}
		case m == http.MethodPut && comp == "acl":
			return "SetContainerACL", func() error {
				return s.withContainer(account, container, func(ct *Container) error {
					ct.Public, ct.ACL, ct.ETag = r.Header.Get("x-ms-blob-public-access"), string(core.Body(r)), s.etag()
					return containerHeaders(w, ct, http.StatusOK)
				})
			}
		case m == http.MethodPut && comp == "lease":
			return "LeaseContainer", func() error {
				return s.withContainer(account, container, func(ct *Container) error { return lease(w, r, &ct.Lease, http.StatusOK) })
			}
		case get && comp == "acl":
			return "GetContainerACL", func() error {
				return s.withContainer(account, container, func(ct *Container) error {
					if ct.Public != "" {
						w.Header().Set("x-ms-blob-public-access", ct.Public)
					}
					w.Header().Set("ETag", ct.ETag)
					w.Header().Set("Content-Type", "application/xml")
					io.WriteString(w, or(ct.ACL, `<?xml version="1.0" encoding="utf-8"?><SignedIdentifiers />`))
					return nil
				})
			}
		case get && comp == "list":
			return "ListBlobs", func() error { return s.listBlobs(w, r, account, container) }
		case get:
			return "GetContainerProperties", func() error {
				return s.withContainer(account, container, func(ct *Container) error { return containerHeaders(w, ct, http.StatusOK) })
			}
		case m == http.MethodDelete:
			return "DeleteContainer", func() error { return s.deleteContainer(w, r, account, container) }
		}
	default:
		switch {
		case m == http.MethodPut && comp == "block":
			return "StageBlock", func() error { return s.stageBlock(w, r, account, container, name) }
		case m == http.MethodPut && comp == "blocklist":
			return "CommitBlockList", func() error { return s.commitBlocks(w, r, account, container, name) }
		case m == http.MethodPut && comp == "appendblock":
			return "AppendBlock", func() error { return s.appendBlock(w, r, account, container, name) }
		case m == http.MethodPut && comp == "page":
			return "PutPage", func() error { return s.putPage(w, r, account, container, name) }
		case m == http.MethodPut && comp == "metadata":
			return "SetBlobMetadata", func() error {
				return s.write(w, r, account, container, name, http.StatusOK, func(b *Blob) error { b.Meta = meta(r.Header); return nil })
			}
		case m == http.MethodPut && comp == "properties":
			return "SetBlobProperties", func() error {
				return s.write(w, r, account, container, name, http.StatusOK, func(b *Blob) error { b.Props = blobProps(r.Header, b.Props, true); return nil })
			}
		case m == http.MethodPut && comp == "tags":
			return "SetBlobTags", func() error { return s.setTags(w, r, account, container, name) }
		case m == http.MethodPut && comp == "lease":
			return "LeaseBlob", func() error {
				return s.withBlob(account, container, name, func(_ *Container, b *Blob) error { return lease(w, r, &b.Lease, http.StatusOK) })
			}
		case m == http.MethodPut && comp == "snapshot":
			return "CreateSnapshot", func() error { return s.snapshot(w, r, account, container, name) }
		case m == http.MethodPut && comp == "tier":
			return "SetBlobTier", func() error {
				return s.withBlob(account, container, name, func(_ *Container, b *Blob) error {
					b.Tier = r.Header.Get("x-ms-access-tier")
					w.WriteHeader(http.StatusOK)
					return nil
				})
			}
		case m == http.MethodPut && comp == "" && r.Header.Get("x-ms-copy-source") != "" && r.Header.Get("x-ms-blob-type") == "":
			return "CopyBlob", func() error { return s.copyBlob(w, r, c, account, container, name, http.StatusAccepted) }
		case m == http.MethodPut && comp == "":
			return "PutBlob", func() error { return s.putBlob(w, r, c, account, container, name) }
		case get && comp == "blocklist":
			return "GetBlockList", func() error { return s.getBlockList(w, r, account, container, name) }
		case get && comp == "tags":
			return "GetBlobTags", func() error { return s.getTags(w, account, container, name) }
		case get && comp == "pagelist":
			return "GetPageRanges", func() error { return s.pageRanges(w, account, container, name) }
		case get:
			op := "GetBlob"
			if m == http.MethodHead {
				op = "GetBlobProperties"
			}
			return op, func() error { return s.getBlob(w, r, account, container, name) }
		case m == http.MethodDelete:
			return "DeleteBlob", func() error { return s.deleteBlob(w, r, account, container, name) }
		}
	}
	return "", nil
}

func or(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

// ---- containers ----

func (s *Service) account(name string) *Account {
	a := s.st.Accounts[name]
	if a == nil {
		a = &Account{Containers: map[string]*Container{}}
		s.st.Accounts[name] = a
	}
	return a
}

func (s *Service) withContainer(account, name string, f func(*Container) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ct := s.account(account).Containers[name]
	if ct == nil {
		return errNoContainer()
	}
	return f(ct)
}

func meta(h http.Header) map[string]string {
	var m map[string]string
	for k, v := range h {
		if name, ok := strings.CutPrefix(strings.ToLower(k), "x-ms-meta-"); ok {
			if m == nil {
				m = map[string]string{}
			}
			m[name] = v[0]
		}
	}
	return m
}

func containerHeaders(w http.ResponseWriter, ct *Container, status int) error {
	h := w.Header()
	h.Set("ETag", ct.ETag)
	h.Set("Last-Modified", ct.Modified.Format(http.TimeFormat))
	for k, v := range ct.Meta {
		h.Set("x-ms-meta-"+k, v)
	}
	h.Set("x-ms-lease-status", map[bool]string{true: "locked", false: "unlocked"}[ct.Lease.active()])
	h.Set("x-ms-lease-state", ct.Lease.state())
	h.Set("x-ms-has-immutability-policy", "false")
	h.Set("x-ms-has-legal-hold", "false")
	h.Set("x-ms-default-encryption-scope", "$account-encryption-key")
	h.Set("x-ms-deny-encryption-scope-override", "false")
	if ct.Public != "" {
		h.Set("x-ms-blob-public-access", ct.Public)
	}
	w.WriteHeader(status)
	return nil
}

// containerName is Azure's rule: 3 to 63 lowercase letters, digits and single hyphens, starting and ending
// with a letter or digit. Azure's own $root, $web and $logs are allowed too.
var containerName = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)

func checkContainerName(name string) error {
	if name == "$root" || name == "$web" || name == "$logs" {
		return nil
	}
	if len(name) < 3 || len(name) > 63 || !containerName.MatchString(name) {
		return core.Errorf(400, "InvalidResourceName", "The specified resource name contains invalid characters or is the wrong length: use 3 to 63 lowercase letters, digits and single hyphens.")
	}
	return nil
}

func (s *Service) createContainer(w http.ResponseWriter, r *http.Request, account, name string) error {
	if err := checkContainerName(name); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.account(account)
	if a.Containers[name] != nil {
		return core.Errorf(409, "ContainerAlreadyExists", "The specified container already exists.")
	}
	now := time.Now().UTC()
	ct := &Container{Name: name, Created: now, Modified: now, ETag: s.etag(), Meta: meta(r.Header), Public: r.Header.Get("x-ms-blob-public-access"), Blobs: map[string]*Blob{}}
	a.Containers[name] = ct
	return containerHeaders(w, ct, http.StatusCreated)
}

func (s *Service) deleteContainer(w http.ResponseWriter, r *http.Request, account, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.account(account)
	ct := a.Containers[name]
	if ct == nil {
		return errNoContainer()
	}
	if ct.Lease.active() && r.Header.Get("x-ms-lease-id") != ct.Lease.ID {
		return core.Errorf(412, "LeaseIdMissing", "There is currently a lease on the container and no lease ID was specified in the request.")
	}
	delete(a.Containers, name)
	w.WriteHeader(http.StatusAccepted)
	return nil
}

func (s *Service) listContainers(w http.ResponseWriter, r *http.Request, account string) error {
	q := r.URL.Query()
	prefix, marker := q.Get("prefix"), q.Get("marker")
	max := 5000
	if n, err := strconv.Atoi(q.Get("maxresults")); err == nil && n > 0 {
		max = n
	}
	withMeta := strings.Contains(q.Get("include"), "metadata")
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.account(account)
	var b strings.Builder
	fmt.Fprintf(&b, `<?xml version="1.0" encoding="utf-8"?><EnumerationResults ServiceEndpoint="http://%s/%s/">`, r.Host, account)
	b.WriteString("<Prefix>" + esc(prefix) + "</Prefix>")
	if marker != "" {
		b.WriteString("<Marker>" + esc(marker) + "</Marker>")
	}
	b.WriteString("<MaxResults>" + strconv.Itoa(max) + "</MaxResults><Containers>")
	next, n := "", 0
	for _, name := range core.SortedKeys(a.Containers) {
		if !strings.HasPrefix(name, prefix) || name < marker {
			continue
		}
		if n == max {
			next = name
			break
		}
		ct := a.Containers[name]
		b.WriteString("<Container><Name>" + esc(name) + "</Name><Properties><Last-Modified>" + ct.Modified.Format(http.TimeFormat) +
			"</Last-Modified><Etag>" + ct.ETag + "</Etag><LeaseStatus>" + map[bool]string{true: "locked", false: "unlocked"}[ct.Lease.active()] +
			"</LeaseStatus><LeaseState>" + ct.Lease.state() + "</LeaseState><HasImmutabilityPolicy>false</HasImmutabilityPolicy><HasLegalHold>false</HasLegalHold>")
		if ct.Public != "" {
			b.WriteString("<PublicAccess>" + ct.Public + "</PublicAccess>")
		}
		b.WriteString("</Properties>")
		if withMeta {
			b.WriteString(metaXML(ct.Meta))
		}
		b.WriteString("</Container>")
		n++
	}
	b.WriteString("</Containers><NextMarker>" + esc(next) + "</NextMarker></EnumerationResults>")
	w.Header().Set("Content-Type", "application/xml")
	io.WriteString(w, b.String())
	return nil
}

func metaXML(m map[string]string) string {
	var b strings.Builder
	b.WriteString("<Metadata>")
	for _, k := range core.SortedKeys(m) {
		b.WriteString("<" + k + ">" + esc(m[k]) + "</" + k + ">")
	}
	b.WriteString("</Metadata>")
	return b.String()
}

func esc(s string) string {
	var b strings.Builder
	xml.EscapeText(&b, []byte(s))
	return b.String()
}

func (s *Service) serviceProps(w http.ResponseWriter, account string, body []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.account(account)
	if body != nil {
		a.Props = string(body)
		w.WriteHeader(http.StatusAccepted)
		return nil
	}
	w.Header().Set("Content-Type", "application/xml")
	io.WriteString(w, or(a.Props, `<?xml version="1.0" encoding="utf-8"?><StorageServiceProperties><Logging><Version>1.0</Version><Delete>false</Delete><Read>false</Read><Write>false</Write><RetentionPolicy><Enabled>false</Enabled></RetentionPolicy></Logging><HourMetrics><Version>1.0</Version><Enabled>false</Enabled><RetentionPolicy><Enabled>false</Enabled></RetentionPolicy></HourMetrics><MinuteMetrics><Version>1.0</Version><Enabled>false</Enabled><RetentionPolicy><Enabled>false</Enabled></RetentionPolicy></MinuteMetrics><Cors /><DefaultServiceVersion>2025-01-05</DefaultServiceVersion></StorageServiceProperties>`))
	return nil
}

// ---- blobs ----

var propHeaders = map[string]string{"x-ms-blob-content-type": "Content-Type", "x-ms-blob-content-encoding": "Content-Encoding",
	"x-ms-blob-content-language": "Content-Language", "x-ms-blob-content-md5": "Content-MD5",
	"x-ms-blob-content-disposition": "Content-Disposition", "x-ms-blob-cache-control": "Cache-Control"}

// blobProps reads x-ms-blob-content-* (and, for Put Blob, the plain headers).
func blobProps(h http.Header, cur map[string]string, replace bool) map[string]string {
	out := map[string]string{}
	if !replace {
		for k, v := range cur {
			out[k] = v
		}
	}
	for hdr, prop := range propHeaders {
		if v := h.Get(hdr); v != "" {
			out[prop] = v
		}
	}
	return out
}

func (s *Service) withBlob(account, container, name string, f func(*Container, *Blob) error) error {
	return s.withContainer(account, container, func(ct *Container) error {
		b := ct.Blobs[name]
		if b == nil {
			return errNoBlob()
		}
		return f(ct, b)
	})
}

// checkWrite enforces conditional headers and leases for a write; lock held.
func checkWrite(h http.Header, b *Blob) error {
	if b == nil {
		if im := h.Get("If-Match"); im != "" && im != "*" {
			return errCondition()
		}
		return nil
	}
	if b.Lease.active() {
		switch id := h.Get("x-ms-lease-id"); {
		case id == "":
			return core.Errorf(412, "LeaseIdMissing", "There is currently a lease on the blob and no lease ID was specified in the request.")
		case id != b.Lease.ID:
			return core.Errorf(412, "LeaseIdMismatchWithBlobOperation", "The lease ID specified did not match the lease ID for the blob.")
		}
	}
	if im := h.Get("If-Match"); im != "" && im != "*" && im != b.ETag {
		return errCondition()
	}
	if inm := h.Get("If-None-Match"); inm == "*" || inm != "" && inm == b.ETag {
		return core.Errorf(409, "BlobAlreadyExists", "The specified blob already exists.")
	}
	if t, err := http.ParseTime(h.Get("If-Unmodified-Since")); err == nil && b.Modified.Truncate(time.Second).After(t) {
		return errCondition()
	}
	if t, err := http.ParseTime(h.Get("If-Modified-Since")); err == nil && !b.Modified.Truncate(time.Second).After(t) {
		return errCondition()
	}
	return nil
}

// write changes an existing blob's attributes.
func (s *Service) write(w http.ResponseWriter, r *http.Request, account, container, name string, status int, f func(*Blob) error) error {
	return s.withBlob(account, container, name, func(_ *Container, b *Blob) error {
		if err := checkWrite(r.Header, b); err != nil {
			return err
		}
		if err := f(b); err != nil {
			return err
		}
		b.ETag, b.Modified = s.etag(), time.Now().UTC()
		w.Header().Set("ETag", b.ETag)
		w.Header().Set("Last-Modified", b.Modified.Format(http.TimeFormat))
		w.Header().Set("x-ms-request-server-encrypted", "true")
		w.WriteHeader(status)
		return nil
	})
}

func md5b64(b []byte) string {
	h := md5.Sum(b)
	return base64.StdEncoding.EncodeToString(h[:])
}

func (s *Service) putBlob(w http.ResponseWriter, r *http.Request, c *core.Ctx, account, container, name string) error {
	typ := or(r.Header.Get("x-ms-blob-type"), "BlockBlob")
	if r.Header.Get("x-ms-copy-source") != "" { // Put Blob From URL
		return s.copyBlob(w, r, c, account, container, name, http.StatusCreated)
	}
	data := core.Body(r)
	if want := r.Header.Get("Content-MD5"); want != "" && want != md5b64(data) {
		return core.Errorf(400, "Md5Mismatch", "The MD5 value specified in the request did not match with the MD5 value calculated by the server.")
	}
	props := blobProps(r.Header, nil, true)
	if typ == "BlockBlob" {
		if ct := r.Header.Get("Content-Type"); props["Content-Type"] == "" && ct != "" {
			props["Content-Type"] = ct
		}
		if props["Content-MD5"] == "" {
			props["Content-MD5"] = md5b64(data)
		}
	}
	switch typ {
	case "AppendBlob":
		data = []byte{}
	case "PageBlob":
		size, err := strconv.ParseInt(r.Header.Get("x-ms-blob-content-length"), 10, 64)
		if err != nil && r.Header.Get("x-ms-blob-content-length") != "" || size < 0 || size%512 != 0 {
			return core.Errorf(400, "InvalidHeaderValue", "x-ms-blob-content-length must be a multiple of 512")
		}
		// the page blob is allocated at its full size, so an unchecked header could exhaust memory
		if err := s.tooBig(name, size); err != nil {
			return err
		}
		data = make([]byte, size)
	}
	var ev *blobEvent
	err := s.withContainer(account, container, func(ct *Container) error {
		cur := ct.Blobs[name]
		if err := checkWrite(r.Header, cur); err != nil {
			return err
		}
		now := time.Now().UTC()
		b := &Blob{Name: name, Type: typ, Data: data, Props: props, Meta: meta(r.Header), Tags: parseTags(r.Header.Get("x-ms-tags")),
			Created: now, Modified: now, ETag: s.etag(), Tier: r.Header.Get("x-ms-access-tier")}
		if cur != nil {
			b.Created, b.Lease, b.Snapshots = cur.Created, cur.Lease, cur.Snapshots
		}
		ct.Blobs[name] = b
		ev = created("PutBlob", b)
		h := w.Header()
		h.Set("ETag", b.ETag)
		h.Set("Last-Modified", b.Modified.Format(http.TimeFormat))
		if typ == "BlockBlob" {
			h.Set("Content-MD5", props["Content-MD5"])
		}
		h.Set("x-ms-request-server-encrypted", "true")
		w.WriteHeader(http.StatusCreated)
		return nil
	})
	if err == nil {
		s.emit(core.C(r).Trace, account, container, name, ev)
	}
	return err
}

func parseTags(s string) map[string]string {
	if s == "" {
		return nil
	}
	vals, _ := url.ParseQuery(s)
	out := map[string]string{}
	for k := range vals {
		out[k] = vals.Get(k)
	}
	return out
}

func (s *Service) stageBlock(w http.ResponseWriter, r *http.Request, account, container, name string) error {
	id := r.URL.Query().Get("blockid")
	if id == "" {
		return core.Errorf(400, "InvalidQueryParameterValue", "Value for one of the query parameters specified in the request URI is invalid.")
	}
	data := core.Body(r)
	if want := r.Header.Get("Content-MD5"); want != "" && want != md5b64(data) {
		return core.Errorf(400, "Md5Mismatch", "The MD5 value specified in the request did not match with the MD5 value calculated by the server.")
	}
	return s.withContainer(account, container, func(ct *Container) error {
		b := ct.Blobs[name]
		if b == nil { // staging creates an uncommitted blob
			now := time.Now().UTC()
			b = &Blob{Name: name, Type: "BlockBlob", Created: now, Modified: now, ETag: s.etag(), Staged: true}
			ct.Blobs[name] = b
		}
		if b.Uncommitted == nil {
			b.Uncommitted = map[string][]byte{}
		}
		b.Uncommitted[id] = data
		w.Header().Set("Content-MD5", md5b64(data))
		w.Header().Set("x-ms-request-server-encrypted", "true")
		w.WriteHeader(http.StatusCreated)
		return nil
	})
}

func (s *Service) commitBlocks(w http.ResponseWriter, r *http.Request, account, container, name string) error {
	var list struct {
		Items []struct {
			XMLName xml.Name
			ID      string `xml:",chardata"`
		} `xml:",any"`
	}
	if err := xml.Unmarshal(core.Body(r), &list); err != nil {
		return core.Errorf(400, "InvalidXmlDocument", "XML specified is not syntactically valid.")
	}
	var ev *blobEvent
	err := s.withContainer(account, container, func(ct *Container) error {
		b := ct.Blobs[name]
		if err := checkWrite(r.Header, liveOrNil(b)); err != nil {
			return err
		}
		committed := map[string][]byte{}
		if b != nil {
			offset := 0
			for _, blk := range b.Committed {
				committed[blk.ID] = b.Data[offset : offset+blk.Size]
				offset += blk.Size
			}
		}
		var data []byte
		var blocks []Block
		for _, it := range list.Items {
			var chunk []byte
			var ok bool
			switch it.XMLName.Local {
			case "Latest":
				if chunk, ok = uncommitted(b, it.ID); !ok {
					chunk, ok = committed[it.ID]
				}
			case "Committed":
				chunk, ok = committed[it.ID]
			case "Uncommitted":
				chunk, ok = uncommitted(b, it.ID)
			}
			if !ok {
				return core.Errorf(400, "InvalidBlockList", "The specified block list is invalid.")
			}
			if err := s.tooBig(name, int64(len(data)+len(chunk))); err != nil {
				return err
			}
			data = append(data, chunk...)
			blocks = append(blocks, Block{it.ID, len(chunk)})
		}
		now := time.Now().UTC()
		nb := &Blob{Name: name, Type: "BlockBlob", Data: data, Committed: blocks, Props: blobProps(r.Header, nil, true), Meta: meta(r.Header),
			Tags: parseTags(r.Header.Get("x-ms-tags")), Created: now, Modified: now, ETag: s.etag(), Tier: r.Header.Get("x-ms-access-tier")}
		if b != nil && !b.Staged {
			nb.Created, nb.Lease, nb.Snapshots = b.Created, b.Lease, b.Snapshots
		}
		ct.Blobs[name] = nb
		ev = created("PutBlockList", nb)
		w.Header().Set("ETag", nb.ETag)
		w.Header().Set("Last-Modified", nb.Modified.Format(http.TimeFormat))
		w.Header().Set("x-ms-request-server-encrypted", "true")
		w.WriteHeader(http.StatusCreated)
		return nil
	})
	if err == nil {
		s.emit(core.C(r).Trace, account, container, name, ev)
	}
	return err
}

// tooBig refuses a blob that would grow past the upload limit (a block list or appends add up).
func (s *Service) tooBig(name string, size int64) error {
	if limit := s.hub.MaxUpload(); size > limit {
		return core.Errorf(413, "RequestBodyTooLarge", "%s", s.hub.TooBig("blob", name, size, limit))
	}
	return nil
}

func uncommitted(b *Blob, id string) ([]byte, bool) {
	if b == nil || b.Uncommitted == nil {
		return nil, false
	}
	d, ok := b.Uncommitted[id]
	return d, ok
}

// liveOrNil hides blobs that only exist as staged blocks.
func liveOrNil(b *Blob) *Blob {
	if b == nil || b.Staged {
		return nil
	}
	return b
}

func (s *Service) getBlockList(w http.ResponseWriter, r *http.Request, account, container, name string) error {
	which := or(r.URL.Query().Get("blocklisttype"), "committed")
	return s.withBlob(account, container, name, func(_ *Container, b *Blob) error {
		var out strings.Builder
		out.WriteString(`<?xml version="1.0" encoding="utf-8"?><BlockList>`)
		if which == "committed" || which == "all" {
			out.WriteString("<CommittedBlocks>")
			for _, blk := range b.Committed {
				fmt.Fprintf(&out, "<Block><Name>%s</Name><Size>%d</Size></Block>", blk.ID, blk.Size)
			}
			out.WriteString("</CommittedBlocks>")
		}
		if which == "uncommitted" || which == "all" {
			out.WriteString("<UncommittedBlocks>")
			for _, id := range core.SortedKeys(b.Uncommitted) {
				fmt.Fprintf(&out, "<Block><Name>%s</Name><Size>%d</Size></Block>", id, len(b.Uncommitted[id]))
			}
			out.WriteString("</UncommittedBlocks>")
		}
		out.WriteString("</BlockList>")
		if !b.Staged {
			w.Header().Set("ETag", b.ETag)
			w.Header().Set("Last-Modified", b.Modified.Format(http.TimeFormat))
		}
		w.Header().Set("x-ms-blob-content-length", strconv.Itoa(len(b.Data)))
		w.Header().Set("Content-Type", "application/xml")
		io.WriteString(w, out.String())
		return nil
	})
}

func (s *Service) appendBlock(w http.ResponseWriter, r *http.Request, account, container, name string) error {
	data := core.Body(r)
	return s.withBlob(account, container, name, func(_ *Container, b *Blob) error {
		if b.Type != "AppendBlob" {
			return core.Errorf(409, "InvalidBlobType", "The blob type is invalid for this operation.")
		}
		if err := checkWrite(r.Header, b); err != nil {
			return err
		}
		if p := r.Header.Get("x-ms-blob-condition-appendpos"); p != "" && p != strconv.Itoa(len(b.Data)) {
			return core.Errorf(412, "AppendPositionConditionNotMet", "The append position condition specified was not met.")
		}
		if m, err := strconv.Atoi(r.Header.Get("x-ms-blob-condition-maxsize")); err == nil && len(b.Data)+len(data) > m {
			return core.Errorf(412, "MaxBlobSizeConditionNotMet", "The max blob size condition specified was not met.")
		}
		if err := s.tooBig(name, int64(len(b.Data)+len(data))); err != nil {
			return err
		}
		offset := len(b.Data)
		b.Data = append(append([]byte(nil), b.Data...), data...)
		b.Appends++
		b.ETag, b.Modified = s.etag(), time.Now().UTC()
		h := w.Header()
		h.Set("ETag", b.ETag)
		h.Set("Last-Modified", b.Modified.Format(http.TimeFormat))
		h.Set("x-ms-blob-append-offset", strconv.Itoa(offset))
		h.Set("x-ms-blob-committed-block-count", strconv.Itoa(b.Appends))
		h.Set("x-ms-request-server-encrypted", "true")
		w.WriteHeader(http.StatusCreated)
		return nil
	})
}

func (s *Service) putPage(w http.ResponseWriter, r *http.Request, account, container, name string) error {
	rg := or(r.Header.Get("x-ms-range"), r.Header.Get("Range"))
	data := core.Body(r)
	return s.withBlob(account, container, name, func(_ *Container, b *Blob) error {
		if b.Type != "PageBlob" {
			return core.Errorf(409, "InvalidBlobType", "The blob type is invalid for this operation.")
		}
		start, end, ok := parseRange(rg, len(b.Data))
		if !ok || start%512 != 0 || (end+1)%512 != 0 {
			return core.Errorf(416, "InvalidPageRange", "The page range specified is invalid.")
		}
		buf := append([]byte(nil), b.Data...)
		if r.Header.Get("x-ms-page-write") == "clear" {
			clear(buf[start : end+1])
		} else {
			copy(buf[start:end+1], data)
		}
		b.Data = buf
		b.ETag, b.Modified = s.etag(), time.Now().UTC()
		w.Header().Set("ETag", b.ETag)
		w.Header().Set("Last-Modified", b.Modified.Format(http.TimeFormat))
		w.Header().Set("x-ms-blob-sequence-number", "0")
		w.WriteHeader(http.StatusCreated)
		return nil
	})
}

// pageRanges reports 512-byte pages that hold any non-zero byte.
func (s *Service) pageRanges(w http.ResponseWriter, account, container, name string) error {
	return s.withBlob(account, container, name, func(_ *Container, b *Blob) error {
		var out strings.Builder
		out.WriteString(`<?xml version="1.0" encoding="utf-8"?><PageList>`)
		start := -1
		for p := 0; p <= len(b.Data); p += 512 {
			used := p < len(b.Data) && !bytes.Equal(b.Data[p:min(p+512, len(b.Data))], make([]byte, min(512, len(b.Data)-p)))
			switch {
			case used && start < 0:
				start = p
			case !used && start >= 0:
				fmt.Fprintf(&out, "<PageRange><Start>%d</Start><End>%d</End></PageRange>", start, p-1)
				start = -1
			}
		}
		out.WriteString("</PageList>")
		w.Header().Set("ETag", b.ETag)
		w.Header().Set("x-ms-blob-content-length", strconv.Itoa(len(b.Data)))
		w.Header().Set("Content-Type", "application/xml")
		io.WriteString(w, out.String())
		return nil
	})
}

func parseRange(h string, size int) (int, int, bool) {
	spec, ok := strings.CutPrefix(h, "bytes=")
	if !ok {
		return 0, 0, false
	}
	a, b, _ := strings.Cut(spec, "-")
	start, err := strconv.Atoi(a)
	if err != nil || start >= size && size > 0 {
		return 0, 0, false
	}
	end := size - 1
	if e, err := strconv.Atoi(b); err == nil && e < end {
		end = e
	}
	return start, end, end >= start
}

// checkSnapshot refuses a snapshot id that is not a timestamp, as Azure does; a valid id that does not exist is a 404.
func checkSnapshot(snap string) error {
	if _, err := time.Parse(time.RFC3339Nano, snap); snap != "" && err != nil {
		return core.Errorf(400, "InvalidQueryParameterValue", "Value for one of the query parameters specified in the request URI is invalid.")
	}
	return nil
}

func (s *Service) getBlob(w http.ResponseWriter, r *http.Request, account, container, name string) error {
	snap := r.URL.Query().Get("snapshot")
	if err := checkSnapshot(snap); err != nil {
		return err
	}
	var b Blob
	err := s.withBlob(account, container, name, func(_ *Container, p *Blob) error {
		if p.Staged {
			return errNoBlob()
		}
		b = *p
		if snap != "" {
			d, ok := p.Snapshots[snap]
			if !ok {
				return errNoBlob()
			}
			b.Data = d
		}
		return nil
	})
	if err != nil {
		return err
	}
	h := w.Header()
	h.Set("ETag", b.ETag)
	h.Set("Last-Modified", b.Modified.Format(http.TimeFormat))
	mod := b.Modified.Truncate(time.Second)
	if im := r.Header.Get("If-Match"); im != "" && im != "*" && im != b.ETag {
		return errCondition()
	}
	if t, err := http.ParseTime(r.Header.Get("If-Unmodified-Since")); err == nil && mod.After(t) {
		return errCondition()
	}
	if inm := r.Header.Get("If-None-Match"); inm != "" && (inm == "*" || inm == b.ETag) {
		return core.Errorf(304, "ConditionNotMet", "Not modified")
	}
	if t, err := http.ParseTime(r.Header.Get("If-Modified-Since")); err == nil && !mod.After(t) {
		return core.Errorf(304, "ConditionNotMet", "Not modified")
	}
	data, status := b.Data, http.StatusOK
	if rg := or(r.Header.Get("x-ms-range"), r.Header.Get("Range")); rg != "" && len(b.Data) > 0 {
		start, end, ok := parseRange(rg, len(b.Data))
		if !ok {
			h.Set("Content-Range", fmt.Sprintf("bytes */%d", len(b.Data)))
			return core.Errorf(416, "InvalidRange", "The range specified is invalid for the current size of the resource.")
		}
		data, status = data[start:end+1], http.StatusPartialContent
		h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(b.Data)))
		if r.Header.Get("x-ms-range-get-content-md5") == "true" {
			h.Set("Content-MD5", md5b64(data))
		}
	}
	for k, v := range b.Props {
		if k == "Content-MD5" && status != http.StatusOK {
			h.Set("x-ms-blob-content-md5", v)
			continue
		}
		h.Set(k, v)
	}
	if h.Get("Content-Type") == "" {
		h.Set("Content-Type", "application/octet-stream")
	}
	for k, v := range b.Meta {
		h.Set("x-ms-meta-"+k, v)
	}
	for k, v := range b.Copy {
		h.Set(k, v)
	}
	h.Set("Content-Length", strconv.Itoa(len(data)))
	h.Set("Accept-Ranges", "bytes")
	h.Set("x-ms-blob-type", b.Type)
	h.Set("x-ms-creation-time", b.Created.Format(http.TimeFormat))
	h.Set("x-ms-lease-status", map[bool]string{true: "locked", false: "unlocked"}[b.Lease.active()])
	h.Set("x-ms-lease-state", b.Lease.state())
	if b.Lease.active() {
		h.Set("x-ms-lease-duration", map[bool]string{true: "infinite", false: "fixed"}[b.Lease.Until.IsZero()])
	}
	h.Set("x-ms-server-encrypted", "true")
	h.Set("x-ms-access-tier", or(b.Tier, "Hot"))
	if b.Tier == "" {
		h.Set("x-ms-access-tier-inferred", "true")
	}
	if len(b.Tags) > 0 {
		h.Set("x-ms-tag-count", strconv.Itoa(len(b.Tags)))
	}
	switch b.Type {
	case "AppendBlob":
		h.Set("x-ms-blob-committed-block-count", strconv.Itoa(b.Appends))
	case "PageBlob":
		h.Set("x-ms-blob-sequence-number", "0")
	}
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		w.Write(data)
	}
	return nil
}

func (s *Service) deleteBlob(w http.ResponseWriter, r *http.Request, account, container, name string) error {
	if err := checkSnapshot(r.URL.Query().Get("snapshot")); err != nil {
		return err
	}
	var ev *blobEvent
	err := s.withContainer(account, container, func(ct *Container) error {
		b := ct.Blobs[name]
		if b == nil {
			return errNoBlob()
		}
		if snap := r.URL.Query().Get("snapshot"); snap != "" {
			if _, ok := b.Snapshots[snap]; !ok {
				return errNoBlob()
			}
			delete(b.Snapshots, snap)
			w.WriteHeader(http.StatusAccepted)
			return nil
		}
		if err := checkWrite(r.Header, b); err != nil {
			return err
		}
		switch r.Header.Get("x-ms-delete-snapshots") {
		case "only":
			b.Snapshots = nil
		case "include", "":
			if len(b.Snapshots) > 0 && r.Header.Get("x-ms-delete-snapshots") == "" {
				return core.Errorf(409, "SnapshotsPresent", "This operation is not permitted because the blob has snapshots.")
			}
			delete(ct.Blobs, name)
			ev = &blobEvent{api: "DeleteBlob", blobType: b.Type, deleted: true}
		}
		w.WriteHeader(http.StatusAccepted)
		return nil
	})
	if err == nil {
		s.emit(core.C(r).Trace, account, container, name, ev)
	}
	return err
}

func (s *Service) snapshot(w http.ResponseWriter, r *http.Request, account, container, name string) error {
	return s.withBlob(account, container, name, func(_ *Container, b *Blob) error {
		id := time.Now().UTC().Format("2006-01-02T15:04:05.0000000Z")
		if b.Snapshots == nil {
			b.Snapshots = map[string][]byte{}
		}
		b.Snapshots[id] = b.Data
		w.Header().Set("x-ms-snapshot", id)
		w.Header().Set("ETag", b.ETag)
		w.Header().Set("Last-Modified", b.Modified.Format(http.TimeFormat))
		w.WriteHeader(http.StatusCreated)
		return nil
	})
}

func (s *Service) setTags(w http.ResponseWriter, r *http.Request, account, container, name string) error {
	var in struct {
		Tags []struct {
			Key   string `xml:"Key"`
			Value string `xml:"Value"`
		} `xml:"TagSet>Tag"`
	}
	if err := xml.Unmarshal(core.Body(r), &in); err != nil {
		return core.Errorf(400, "InvalidXmlDocument", "XML specified is not syntactically valid.")
	}
	return s.withBlob(account, container, name, func(_ *Container, b *Blob) error {
		b.Tags = map[string]string{}
		for _, t := range in.Tags {
			b.Tags[t.Key] = t.Value
		}
		w.WriteHeader(http.StatusNoContent)
		return nil
	})
}

func (s *Service) getTags(w http.ResponseWriter, account, container, name string) error {
	return s.withBlob(account, container, name, func(_ *Container, b *Blob) error {
		var out strings.Builder
		out.WriteString(`<?xml version="1.0" encoding="utf-8"?><Tags><TagSet>`)
		for _, k := range core.SortedKeys(b.Tags) {
			out.WriteString("<Tag><Key>" + esc(k) + "</Key><Value>" + esc(b.Tags[k]) + "</Value></Tag>")
		}
		out.WriteString("</TagSet></Tags>")
		w.Header().Set("Content-Type", "application/xml")
		io.WriteString(w, out.String())
		return nil
	})
}

// copyBlob copies synchronously from a blob URL on this server (any host) or fetches an external URL.
func (s *Service) copyBlob(w http.ResponseWriter, r *http.Request, c *core.Ctx, account, container, name string, status int) error {
	src := r.Header.Get("x-ms-copy-source")
	u, err := url.Parse(src)
	if err != nil || u.Host == "" || u.Scheme != "http" && u.Scheme != "https" {
		return core.Errorf(400, "InvalidHeaderValue", "The value for one of the HTTP headers is not in the correct format.")
	}
	if err := checkSnapshot(u.Query().Get("snapshot")); err != nil {
		return err
	}
	var data []byte
	var props, md map[string]string
	parts := strings.SplitN(strings.TrimPrefix(u.Path, "/"), "/", 3)
	s.mu.Lock()
	if len(parts) == 3 {
		if a := s.st.Accounts[parts[0]]; a != nil {
			if ct := a.Containers[parts[1]]; ct != nil {
				if b := ct.Blobs[parts[2]]; b != nil {
					data, props, md = b.Data, b.Props, b.Meta
					if snap := u.Query().Get("snapshot"); snap != "" {
						data = b.Snapshots[snap]
					}
				}
			}
		}
	}
	s.mu.Unlock()
	if data == nil {
		res, err := (&http.Client{Timeout: time.Minute}).Get(src) // a source URL that never answers must not hold the copy forever
		if err != nil || res.StatusCode >= 300 {
			if err == nil {
				res.Body.Close()
			}
			return core.Errorf(404, "CannotVerifyCopySource", "The specified copy source could not be read.")
		}
		data, _ = io.ReadAll(res.Body)
		res.Body.Close()
		res.Body.Close()
		props = map[string]string{"Content-Type": res.Header.Get("Content-Type")}
	}
	if m := meta(r.Header); m != nil {
		md = m
	}
	id := core.UUID()
	var ev *blobEvent
	err = s.withContainer(account, container, func(ct *Container) error {
		if err := checkWrite(r.Header, liveOrNil(ct.Blobs[name])); err != nil {
			return err
		}
		now := time.Now().UTC()
		b := &Blob{Name: name, Type: "BlockBlob", Data: data, Props: props, Meta: md, Created: now, Modified: now, ETag: s.etag(),
			Copy: map[string]string{"x-ms-copy-id": id, "x-ms-copy-status": "success", "x-ms-copy-source": src,
				"x-ms-copy-progress": fmt.Sprintf("%d/%d", len(data), len(data)), "x-ms-copy-completion-time": now.Format(http.TimeFormat)}}
		ct.Blobs[name] = b
		ev = created("CopyBlob", b)
		w.Header().Set("ETag", b.ETag)
		w.Header().Set("Last-Modified", b.Modified.Format(http.TimeFormat))
		w.Header().Set("x-ms-copy-id", id)
		w.Header().Set("x-ms-copy-status", "success")
		w.WriteHeader(status)
		return nil
	})
	if err == nil {
		s.emit(core.C(r).Trace, account, container, name, ev)
	}
	return err
}

func (s *Service) listBlobs(w http.ResponseWriter, r *http.Request, account, container string) error {
	q := r.URL.Query()
	prefix, delim, marker, include := q.Get("prefix"), q.Get("delimiter"), q.Get("marker"), q.Get("include")
	max := 5000
	if n, err := strconv.Atoi(q.Get("maxresults")); err == nil && n > 0 {
		max = n
	}
	return s.withContainer(account, container, func(ct *Container) error {
		var b strings.Builder
		fmt.Fprintf(&b, `<?xml version="1.0" encoding="utf-8"?><EnumerationResults ServiceEndpoint="http://%s/%s/" ContainerName="%s">`, r.Host, account, esc(container))
		b.WriteString("<Prefix>" + esc(prefix) + "</Prefix>")
		if marker != "" {
			b.WriteString("<Marker>" + esc(marker) + "</Marker>")
		}
		b.WriteString("<MaxResults>" + strconv.Itoa(max) + "</MaxResults>")
		if delim != "" {
			b.WriteString("<Delimiter>" + esc(delim) + "</Delimiter>")
		}
		b.WriteString("<Blobs>")
		names := make([]string, 0, len(ct.Blobs))
		for n := range ct.Blobs {
			names = append(names, n)
		}
		sort.Strings(names)
		next, count, lastPrefix := "", 0, ""
		for _, n := range names {
			bl := ct.Blobs[n]
			if !strings.HasPrefix(n, prefix) || n < marker || bl.Staged && !strings.Contains(include, "uncommittedblobs") {
				continue
			}
			if count == max {
				next = n
				break
			}
			if delim != "" {
				if i := strings.Index(n[len(prefix):], delim); i >= 0 {
					if p := n[:len(prefix)+i+len(delim)]; p != lastPrefix {
						b.WriteString("<BlobPrefix><Name>" + esc(p) + "</Name></BlobPrefix>")
						lastPrefix = p
						count++
					}
					continue
				}
			}
			if strings.Contains(include, "snapshots") {
				for _, id := range core.SortedKeys(bl.Snapshots) {
					b.WriteString(blobXML(bl, id, len(bl.Snapshots[id]), include))
				}
			}
			b.WriteString(blobXML(bl, "", len(bl.Data), include))
			count++
		}
		b.WriteString("</Blobs><NextMarker>" + esc(next) + "</NextMarker></EnumerationResults>")
		w.Header().Set("Content-Type", "application/xml")
		io.WriteString(w, b.String())
		return nil
	})
}

func blobXML(bl *Blob, snapshot string, size int, include string) string {
	var b strings.Builder
	b.WriteString("<Blob><Name>" + esc(bl.Name) + "</Name>")
	if snapshot != "" {
		b.WriteString("<Snapshot>" + snapshot + "</Snapshot>")
	}
	b.WriteString("<Properties><Creation-Time>" + bl.Created.Format(http.TimeFormat) + "</Creation-Time><Last-Modified>" +
		bl.Modified.Format(http.TimeFormat) + "</Last-Modified><Etag>" + bl.ETag + "</Etag><Content-Length>" + strconv.Itoa(size) + "</Content-Length>")
	for _, k := range []string{"Content-Type", "Content-Encoding", "Content-Language", "Content-MD5", "Content-Disposition", "Cache-Control"} {
		b.WriteString("<" + k + ">" + esc(bl.Props[k]) + "</" + k + ">")
	}
	b.WriteString("<BlobType>" + bl.Type + "</BlobType><AccessTier>" + or(bl.Tier, "Hot") + "</AccessTier>")
	if bl.Tier == "" {
		b.WriteString("<AccessTierInferred>true</AccessTierInferred>")
	}
	b.WriteString("<LeaseStatus>" + map[bool]string{true: "locked", false: "unlocked"}[bl.Lease.active()] + "</LeaseStatus><LeaseState>" +
		bl.Lease.state() + "</LeaseState><ServerEncrypted>true</ServerEncrypted>")
	if len(bl.Tags) > 0 {
		b.WriteString("<TagCount>" + strconv.Itoa(len(bl.Tags)) + "</TagCount>")
	}
	b.WriteString("</Properties>")
	if strings.Contains(include, "metadata") {
		b.WriteString(metaXML(bl.Meta))
	}
	if strings.Contains(include, "tags") && len(bl.Tags) > 0 {
		b.WriteString("<Tags><TagSet>")
		for _, k := range core.SortedKeys(bl.Tags) {
			b.WriteString("<Tag><Key>" + esc(k) + "</Key><Value>" + esc(bl.Tags[k]) + "</Value></Tag>")
		}
		b.WriteString("</TagSet></Tags>")
	}
	b.WriteString("</Blob>")
	return b.String()
}

// lease implements acquire, renew, change, release and break.
func lease(w http.ResponseWriter, r *http.Request, l *Lease, status int) error {
	h := r.Header
	action, id := h.Get("x-ms-lease-action"), h.Get("x-ms-lease-id")
	conflict := func() error {
		return core.Errorf(409, "LeaseAlreadyPresent", "There is already a lease present.")
	}
	switch action {
	case "acquire":
		if l.active() && h.Get("x-ms-proposed-lease-id") != l.ID {
			return conflict()
		}
		d, _ := strconv.Atoi(h.Get("x-ms-lease-duration"))
		if d != -1 && (d < 15 || d > 60) {
			return core.Errorf(400, "InvalidHeaderValue", "x-ms-lease-duration must be -1 or between 15 and 60")
		}
		*l = Lease{ID: or(h.Get("x-ms-proposed-lease-id"), core.UUID()), Duration: d}
		if d > 0 {
			l.Until = time.Now().Add(time.Duration(d) * time.Second)
		}
		status = http.StatusCreated
	case "renew":
		if l.ID != id {
			return core.Errorf(409, "LeaseIdMismatchWithLeaseOperation", "The lease ID specified did not match the lease ID for the blob.")
		}
		if l.Duration > 0 {
			l.Until = time.Now().Add(time.Duration(l.Duration) * time.Second)
		}
		l.Broken = false
	case "change":
		if l.ID != id {
			return core.Errorf(409, "LeaseIdMismatchWithLeaseOperation", "The lease ID specified did not match the lease ID for the blob.")
		}
		l.ID = h.Get("x-ms-proposed-lease-id")
	case "release":
		if l.ID != id {
			return core.Errorf(409, "LeaseIdMismatchWithLeaseOperation", "The lease ID specified did not match the lease ID for the blob.")
		}
		*l = Lease{}
	case "break":
		if l.ID == "" {
			return core.Errorf(409, "LeaseNotPresentWithLeaseOperation", "There is currently no lease on the blob.")
		}
		l.Broken = true
		w.Header().Set("x-ms-lease-time", "0")
		status = http.StatusAccepted
	default:
		return core.Errorf(400, "InvalidHeaderValue", "unknown x-ms-lease-action %q", action)
	}
	if l.ID != "" && action != "break" {
		w.Header().Set("x-ms-lease-id", l.ID)
	}
	w.WriteHeader(status)
	return nil
}

// cors answers preflights from the account's CORS rules (allow-all when none).
func (s *Service) cors(w http.ResponseWriter, r *http.Request, account string) bool {
	origin := r.Header.Get("Origin")
	h := w.Header()
	h.Set("Access-Control-Allow-Origin", origin)
	h.Set("Access-Control-Expose-Headers", "*")
	h.Add("Vary", "Origin")
	if r.Method != http.MethodOptions {
		return false
	}
	h.Set("Access-Control-Allow-Methods", or(r.Header.Get("Access-Control-Request-Method"), "GET,PUT,POST,DELETE,HEAD"))
	if rh := r.Header.Get("Access-Control-Request-Headers"); rh != "" {
		h.Set("Access-Control-Allow-Headers", rh)
	}
	h.Set("Access-Control-Max-Age", "3600")
	w.WriteHeader(http.StatusOK)
	return true
}

// ---- console ----

// Resources lists containers across accounts.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := core.Table{Name: "Containers", Columns: []string{"Name", "Account", "Blobs", "Size", "Public", "Modified"}}
	for _, acct := range core.SortedKeys(s.st.Accounts) {
		a := s.st.Accounts[acct]
		for _, name := range core.SortedKeys(a.Containers) {
			ct := a.Containers[name]
			n, size := 0, 0
			for _, b := range ct.Blobs {
				if !b.Staged {
					n++
					size += len(b.Data)
				}
			}
			t.Rows = append(t.Rows, []string{name, acct, strconv.Itoa(n), strconv.Itoa(size) + " B", ct.Public, ct.Modified.Format(time.DateTime)})
		}
	}
	return []core.Table{t}
}

// Inspect returns a container's properties and blob names.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for acct, a := range s.st.Accounts {
		if ct := a.Containers[id]; ct != nil {
			blobs := append([]string{}, core.SortedKeys(ct.Blobs)...)
			return map[string]any{"account": acct, "name": ct.Name, "created": ct.Created, "metadata": ct.Meta, "public": ct.Public, "blobs": blobs}
		}
	}
	return nil
}

// splitRef turns the console's "account/container" into its parts.
func splitRef(ref string) (string, string) {
	a, c, ok := strings.Cut(ref, "/")
	if !ok {
		return "devstoreaccount1", a
	}
	return a, c
}

// Browse lists one "directory" level of a container for the console.
func (s *Service) Browse(ref, prefix string) ([]string, []core.ObjectInfo, error) {
	account, container := splitRef(ref)
	var dirs []string
	var objs []core.ObjectInfo
	err := s.withContainer(account, container, func(ct *Container) error {
		seen := map[string]bool{}
		for _, k := range core.SortedKeys(ct.Blobs) {
			b := ct.Blobs[k]
			if b.Staged || !strings.HasPrefix(k, prefix) {
				continue
			}
			if i := strings.Index(k[len(prefix):], "/"); i >= 0 {
				if d := k[:len(prefix)+i+1]; !seen[d] {
					seen[d] = true
					dirs = append(dirs, d)
				}
				continue
			}
			objs = append(objs, core.ObjectInfo{Key: k, Size: len(b.Data), Modified: b.Modified, ContentType: b.Props["Content-Type"]})
		}
		return nil
	})
	return dirs, objs, err
}

// Download returns a blob's bytes and content type (console).
func (s *Service) Download(ref, name string) ([]byte, string, error) {
	account, container := splitRef(ref)
	var data []byte
	var ct string
	err := s.withBlob(account, container, name, func(_ *Container, b *Blob) error {
		data, ct = b.Data, b.Props["Content-Type"]
		return nil
	})
	return data, ct, err
}

// Upload stores a block blob (console).
func (s *Service) Upload(ref, name, contentType string, data []byte) error {
	account, container := splitRef(ref)
	return s.withContainer(account, container, func(ct *Container) error {
		now := time.Now().UTC()
		ct.Blobs[name] = &Blob{Name: name, Type: "BlockBlob", Data: data, Props: map[string]string{"Content-Type": contentType, "Content-MD5": md5b64(data)},
			Created: now, Modified: now, ETag: s.etag()}
		return nil
	})
}

// Remove deletes a blob (console).
func (s *Service) Remove(ref, name string) error {
	account, container := splitRef(ref)
	return s.withContainer(account, container, func(ct *Container) error {
		delete(ct.Blobs, name)
		return nil
	})
}

// MakeBucket creates a container in devstoreaccount1 (console).
func (s *Service) MakeBucket(ref string) error {
	account, name := splitRef(ref)
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.account(account)
	if a.Containers[name] != nil {
		return core.Errorf(409, "ContainerAlreadyExists", "The specified container already exists.")
	}
	now := time.Now().UTC()
	a.Containers[name] = &Container{Name: name, Created: now, Modified: now, ETag: s.etag(), Blobs: map[string]*Blob{}}
	return nil
}

// DropBucket deletes a container and its blobs (console).
func (s *Service) DropBucket(ref string) error {
	account, name := splitRef(ref)
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.account(account)
	if a.Containers[name] == nil {
		return errNoContainer()
	}
	delete(a.Containers, name)
	return nil
}
