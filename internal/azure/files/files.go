// Package files emulates Azure Files over REST, at <account>.file.localhost on pogi's main port:
// shares (quota, metadata), directories and files with ranges. Shared Key and SAS are accepted
// without signature checks. SMB, NFS, leases, snapshots and handles are not implemented.
package files

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// Service is the Azure Files emulator.
type Service struct {
	hub     *core.Hub
	mu      sync.Mutex
	st      state
	lastTag int64
}

type state struct {
	Accounts map[string]*Account `json:"accounts"`
}

// Account holds the shares of one storage account.
type Account struct {
	Shares map[string]*Share `json:"shares"`
}

// Share is a file share. Entries are its directories and files by full path ("dir/file.txt").
type Share struct {
	Name     string            `json:"name"`
	Quota    int               `json:"quota"` // GiB
	Tier     string            `json:"tier,omitempty"`
	Meta     map[string]string `json:"meta,omitempty"`
	ACL      string            `json:"acl,omitempty"` // SignedIdentifiers XML
	ETag     string            `json:"etag"`
	Modified time.Time         `json:"modified"`
	Entries  map[string]*Entry `json:"entries"`
}

// Entry is a directory or a file.
type Entry struct {
	Dir      bool              `json:"dir,omitempty"`
	Data     []byte            `json:"data,omitempty"`
	Ranges   [][2]int64        `json:"ranges,omitempty"` // written byte ranges, [start, end], sorted and apart
	Props    map[string]string `json:"props,omitempty"`  // Content-Type, Content-Encoding, ...
	Meta     map[string]string `json:"meta,omitempty"`
	Created  time.Time         `json:"created"`
	ETag     string            `json:"etag"`
	Modified time.Time         `json:"modified"`
}

const defaultQuota = 5120 // GiB, what Azure gives a share made without one

// New creates the Azure Files service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub}
	s.Reset()
	s.registerARM()
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "azfiles", Cloud: core.Azure, Title: "Azure Files", Protocol: "azure-xml", Match: armres.HostMatch("file")}
}

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

// DropAccount forgets a storage account's shares; the blob package calls it when the account is deleted.
func (s *Service) DropAccount(name string) {
	s.mu.Lock()
	delete(s.st.Accounts, name)
	s.mu.Unlock()
}

func (s *Service) etag() string {
	t := time.Now().UnixNano()
	if t <= s.lastTag {
		t = s.lastTag + 1
	}
	s.lastTag = t
	return fmt.Sprintf(`"0x%X"`, t)
}

func (s *Service) account(name string) *Account {
	a := s.st.Accounts[name]
	if a == nil {
		a = &Account{Shares: map[string]*Share{}}
		s.st.Accounts[name] = a
	}
	return a
}

// ---- errors ----

func errShare() error {
	return core.Errorf(404, "ShareNotFound", "The specified share does not exist.")
}
func errNoResource() error {
	return core.Errorf(404, "ResourceNotFound", "The specified resource does not exist.")
}
func errNoParent() error {
	return core.Errorf(404, "ParentNotFound", "The specified parent path does not exist.")
}
func errName() error {
	return core.Errorf(400, "InvalidResourceName", "The specified resource name contains invalid characters.")
}
func errRange() error {
	return core.Errorf(416, "InvalidRange", "The range specified is invalid for the current size of the resource.")
}
func errHeader(h string) error {
	return core.Errorf(400, "InvalidHeaderValue", "The value for one of the HTTP headers (%s) is not in the correct format.", h)
}

// azError writes the Azure Storage XML error with x-ms-error-code.
func azError(w http.ResponseWriter, r *http.Request, c *core.Ctx, err error) {
	e := core.AsError(err)
	c.ErrCode = e.Code
	w.Header().Set("x-ms-error-code", e.Code)
	if r.Method == http.MethodHead {
		w.WriteHeader(e.Status)
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

// ---- routing ----

type handler func() error

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	r = armres.AccountPath(r, "file")
	c := core.C(r)
	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 3)
	account, share, p := parts[0], "", ""
	if len(parts) > 1 {
		share = parts[1]
	}
	if len(parts) > 2 {
		p = strings.Trim(parts[2], "/")
	}
	h := w.Header()
	h.Set("x-ms-request-id", c.RequestID)
	h.Set("x-ms-version", or(r.Header.Get("x-ms-version"), "2025-01-05"))
	h.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	if id := r.Header.Get("x-ms-client-request-id"); id != "" {
		h.Set("x-ms-client-request-id", id)
	}
	op, fn := s.route(w, r, account, share, p)
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

func or(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

func (s *Service) route(w http.ResponseWriter, r *http.Request, account, share, p string) (string, handler) {
	q := r.URL.Query()
	comp, restype, m := q.Get("comp"), q.Get("restype"), r.Method
	get := m == http.MethodGet || m == http.MethodHead
	switch {
	case share == "":
		if get && comp == "list" {
			return "ListShares", func() error { return s.listShares(w, r, account) }
		}
	case restype == "share" && p == "":
		switch {
		case m == http.MethodPut && comp == "":
			return "CreateShare", func() error { return s.createShare(w, r, account, share) }
		case m == http.MethodPut && comp == "properties":
			return "SetShareProperties", func() error {
				return s.withShare(account, share, func(sh *Share) error {
					if err := setQuota(r.Header, &sh.Quota); err != nil {
						return err
					}
					if t := r.Header.Get("x-ms-access-tier"); t != "" {
						sh.Tier = t
					}
					return s.touchShare(w, sh, http.StatusOK, true)
				})
			}
		case m == http.MethodPut && comp == "metadata":
			return "SetShareMetadata", func() error {
				return s.withShare(account, share, func(sh *Share) error {
					sh.Meta = meta(r.Header)
					return s.touchShare(w, sh, http.StatusOK, true)
				})
			}
		case m == http.MethodPut && comp == "acl":
			return "SetShareACL", func() error {
				return s.withShare(account, share, func(sh *Share) error {
					sh.ACL = string(core.Body(r))
					return s.touchShare(w, sh, http.StatusOK, true)
				})
			}
		case get && comp == "acl":
			return "GetShareACL", func() error {
				return s.withShare(account, share, func(sh *Share) error {
					w.Header().Set("ETag", sh.ETag)
					w.Header().Set("Last-Modified", sh.Modified.Format(http.TimeFormat))
					w.Header().Set("Content-Type", "application/xml")
					io.WriteString(w, or(sh.ACL, `<?xml version="1.0" encoding="utf-8"?><SignedIdentifiers />`))
					return nil
				})
			}
		case get && comp == "stats":
			return "GetShareStats", func() error {
				return s.withShare(account, share, func(sh *Share) error {
					var used int64
					for _, e := range sh.Entries {
						used += int64(len(e.Data))
					}
					w.Header().Set("Content-Type", "application/xml")
					fmt.Fprintf(w, `<?xml version="1.0" encoding="utf-8"?><ShareStats><ShareUsageBytes>%d</ShareUsageBytes></ShareStats>`, used)
					return nil
				})
			}
		case get && comp == "":
			return "GetShareProperties", func() error {
				return s.withShare(account, share, func(sh *Share) error {
					h := w.Header()
					h.Set("x-ms-share-quota", strconv.Itoa(sh.Quota))
					h.Set("x-ms-access-tier", or(sh.Tier, "TransactionOptimized"))
					h.Set("x-ms-enabled-protocols", "SMB")
					h.Set("x-ms-lease-state", "available")
					h.Set("x-ms-lease-status", "unlocked")
					for k, v := range sh.Meta {
						h.Set("x-ms-meta-"+k, v)
					}
					return s.touchShare(w, sh, http.StatusOK, false)
				})
			}
		case m == http.MethodDelete:
			return "DeleteShare", func() error { return s.deleteShare(w, account, share) }
		}
	case restype == "directory":
		switch {
		case m == http.MethodPut && comp == "":
			return "CreateDirectory", func() error { return s.create(w, account, share, p, nil, true) }
		case m == http.MethodPut && comp == "metadata":
			return "SetDirectoryMetadata", func() error {
				return s.write(w, r, account, share, p, true, http.StatusOK, func(e *Entry) error { e.Meta = meta(r.Header); return nil })
			}
		case m == http.MethodPut && comp == "properties":
			return "SetDirectoryProperties", func() error {
				return s.write(w, r, account, share, p, true, http.StatusOK, func(*Entry) error { return nil })
			}
		case get && comp == "list":
			return "ListFilesAndDirectories", func() error { return s.listDir(w, r, account, share, p) }
		case get && comp == "":
			return "GetDirectoryProperties", func() error { return s.get(w, r, account, share, p, true) }
		case m == http.MethodDelete && p != "":
			return "DeleteDirectory", func() error { return s.delete(w, account, share, p, true) }
		}
	case p != "":
		switch {
		case m == http.MethodPut && comp == "":
			return "CreateFile", func() error { return s.createFile(w, r, account, share, p) }
		case m == http.MethodPut && comp == "range":
			return "PutRange", func() error { return s.putRange(w, r, account, share, p) }
		case m == http.MethodPut && comp == "metadata":
			return "SetFileMetadata", func() error {
				return s.write(w, r, account, share, p, false, http.StatusOK, func(e *Entry) error { e.Meta = meta(r.Header); return nil })
			}
		case m == http.MethodPut && comp == "properties":
			return "SetFileProperties", func() error { return s.setFileProps(w, r, account, share, p) }
		case get && comp == "rangelist":
			return "ListRanges", func() error { return s.listRanges(w, r, account, share, p) }
		case get && comp == "":
			op := "GetFile"
			if m == http.MethodHead {
				op = "GetFileProperties"
			}
			return op, func() error { return s.get(w, r, account, share, p, false) }
		case m == http.MethodDelete && comp == "":
			return "DeleteFile", func() error { return s.delete(w, account, share, p, false) }
		}
	}
	return "", nil
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

// ---- shares ----

var shareName = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)

func validShare(n string) bool { return len(n) >= 3 && len(n) <= 63 && shareName.MatchString(n) }

// setQuota reads x-ms-share-quota (1 to 102400 GiB) into q when the header is there.
func setQuota(h http.Header, q *int) error {
	v := h.Get("x-ms-share-quota")
	if v == "" {
		return nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 || n > 102400 {
		return errHeader("x-ms-share-quota")
	}
	*q = n
	return nil
}

func (s *Service) withShare(account, name string, f func(*Share) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sh := s.account(account).Shares[name]
	if sh == nil {
		return errShare()
	}
	return f(sh)
}

// touchShare sends the share's ETag with the status, after a change (bump) a new one; lock held.
func (s *Service) touchShare(w http.ResponseWriter, sh *Share, status int, bump bool) error {
	if bump {
		sh.ETag, sh.Modified = s.etag(), time.Now().UTC()
	}
	w.Header().Set("ETag", sh.ETag)
	w.Header().Set("Last-Modified", sh.Modified.Format(http.TimeFormat))
	w.WriteHeader(status)
	return nil
}

func (s *Service) createShare(w http.ResponseWriter, r *http.Request, account, name string) error {
	if !validShare(name) {
		return errName()
	}
	quota := defaultQuota
	if err := setQuota(r.Header, &quota); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.account(account)
	if a.Shares[name] != nil {
		return core.Errorf(409, "ShareAlreadyExists", "The specified share already exists.")
	}
	sh := &Share{Name: name, Quota: quota, Tier: r.Header.Get("x-ms-access-tier"), Meta: meta(r.Header), Entries: map[string]*Entry{}}
	a.Shares[name] = sh
	return s.touchShare(w, sh, http.StatusCreated, true)
}

func (s *Service) deleteShare(w http.ResponseWriter, account, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.account(account)
	if a.Shares[name] == nil {
		return errShare()
	}
	delete(a.Shares, name)
	w.WriteHeader(http.StatusAccepted)
	return nil
}

func (s *Service) listShares(w http.ResponseWriter, r *http.Request, account string) error {
	prefix := r.URL.Query().Get("prefix")
	type item struct {
		Name       string `xml:"Name"`
		Properties struct {
			LastModified string `xml:"Last-Modified"`
			ETag         string `xml:"Etag"`
			Quota        int    `xml:"Quota"`
			AccessTier   string `xml:"AccessTier"`
		} `xml:"Properties"`
	}
	var out struct {
		XMLName         xml.Name `xml:"EnumerationResults"`
		ServiceEndpoint string   `xml:"ServiceEndpoint,attr"`
		Prefix          string   `xml:"Prefix,omitempty"`
		Shares          []item   `xml:"Shares>Share"`
		NextMarker      string   `xml:"NextMarker"`
	}
	out.ServiceEndpoint, out.Prefix = "https://"+r.Host+"/", prefix
	s.mu.Lock()
	a := s.account(account)
	for _, n := range core.SortedKeys(a.Shares) {
		if sh := a.Shares[n]; strings.HasPrefix(n, prefix) {
			it := item{Name: n}
			it.Properties.LastModified, it.Properties.ETag, it.Properties.Quota = sh.Modified.Format(http.TimeFormat), sh.ETag, sh.Quota
			it.Properties.AccessTier = or(sh.Tier, "TransactionOptimized")
			out.Shares = append(out.Shares, it)
		}
	}
	s.mu.Unlock()
	return writeXML(w, out)
}

func writeXML(w http.ResponseWriter, v any) error {
	w.Header().Set("Content-Type", "application/xml")
	io.WriteString(w, xml.Header)
	return xml.NewEncoder(w).Encode(v)
}

// ---- directories and files ----

// entry finds a share and the entry at p ("" is the share's root directory, which always exists). Lock held.
func (s *Service) entry(account, share, p string) (*Share, *Entry, error) {
	sh := s.account(account).Shares[share]
	if sh == nil {
		return nil, nil, errShare()
	}
	if p == "" {
		return sh, &Entry{Dir: true, Created: sh.Modified, ETag: sh.ETag, Modified: sh.Modified}, nil
	}
	return sh, sh.Entries[p], nil
}

// cleanPath refuses a path with an empty, "." or ".." segment.
func cleanPath(p string) error {
	for _, seg := range strings.Split(p, "/") {
		if seg == "" || seg == "." || seg == ".." || len(seg) > 255 {
			return errName()
		}
	}
	return nil
}

// parentOK checks that the directory above p exists. Lock held.
func parentOK(sh *Share, p string) error {
	i := strings.LastIndexByte(p, '/')
	if i < 0 {
		return nil
	}
	if d := sh.Entries[p[:i]]; d == nil || !d.Dir {
		return errNoParent()
	}
	return nil
}

func timeHeader(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.0000000Z") }

// entryHeaders sets what every directory and file answer carries.
func entryHeaders(w http.ResponseWriter, e *Entry) {
	h := w.Header()
	h.Set("ETag", e.ETag)
	h.Set("Last-Modified", e.Modified.Format(http.TimeFormat))
	h.Set("x-ms-file-creation-time", timeHeader(e.Created))
	h.Set("x-ms-file-last-write-time", timeHeader(e.Modified))
	h.Set("x-ms-file-change-time", timeHeader(e.Modified))
	h.Set("x-ms-file-attributes", map[bool]string{true: "Directory", false: "Archive"}[e.Dir])
	h.Set("x-ms-file-id", "11529285414812647424")
	h.Set("x-ms-file-parent-id", "0")
	h.Set("x-ms-request-server-encrypted", "true")
}

func (s *Service) create(w http.ResponseWriter, account, share, p string, e *Entry, dir bool) error {
	if p == "" || cleanPath(p) != nil {
		return errName()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sh, cur, err := s.entry(account, share, p)
	if err != nil {
		return err
	}
	if err := parentOK(sh, p); err != nil {
		return err
	}
	if cur != nil && dir {
		return core.Errorf(409, "ResourceAlreadyExists", "The specified resource already exists.")
	}
	if cur != nil && cur.Dir {
		return core.Errorf(409, "ResourceTypeMismatch", "The specified resource type does not match the type of the existing resource.")
	}
	if e == nil {
		e = &Entry{Dir: true}
	}
	now := time.Now().UTC()
	e.Created, e.Modified, e.ETag = now, now, s.etag()
	if cur != nil {
		e.Created = cur.Created
	}
	sh.Entries[p] = e
	entryHeaders(w, e)
	w.WriteHeader(http.StatusCreated)
	return nil
}

var propHeaders = map[string]string{"x-ms-content-type": "Content-Type", "x-ms-content-encoding": "Content-Encoding",
	"x-ms-content-language": "Content-Language", "x-ms-cache-control": "Cache-Control", "x-ms-content-disposition": "Content-Disposition"}

func fileProps(h http.Header) map[string]string {
	props := map[string]string{}
	for hdr, k := range propHeaders {
		if v := h.Get(hdr); v != "" {
			props[k] = v
		}
	}
	return props
}

func (s *Service) createFile(w http.ResponseWriter, r *http.Request, account, share, p string) error {
	size, err := strconv.ParseInt(r.Header.Get("x-ms-content-length"), 10, 64)
	if err != nil || size < 0 {
		return errHeader("x-ms-content-length")
	}
	if limit := s.hub.MaxUpload(); size > limit { // the file is allocated at its full size
		return core.Errorf(413, "RequestBodyTooLarge", "%s", s.hub.TooBig("files", p, size, limit))
	}
	return s.create(w, account, share, p, &Entry{Data: make([]byte, size), Props: fileProps(r.Header), Meta: meta(r.Header)}, false)
}

func (s *Service) get(w http.ResponseWriter, r *http.Request, account, share, p string, dir bool) error {
	s.mu.Lock()
	_, e, err := s.entry(account, share, p)
	if err == nil && (e == nil || e.Dir != dir) {
		err = errNoResource()
	}
	var cp Entry
	if err == nil {
		cp = *e
	}
	s.mu.Unlock()
	if err != nil {
		return err
	}
	h := w.Header()
	entryHeaders(w, &cp)
	for k, v := range cp.Meta {
		h.Set("x-ms-meta-"+k, v)
	}
	if dir {
		w.WriteHeader(http.StatusOK)
		return nil
	}
	for k, v := range cp.Props {
		h.Set(k, v)
	}
	if h.Get("Content-Type") == "" {
		h.Set("Content-Type", "application/octet-stream")
	}
	h.Set("x-ms-type", "File")
	h.Set("Accept-Ranges", "bytes")
	data, status := cp.Data, http.StatusOK
	if rg := or(r.Header.Get("x-ms-range"), r.Header.Get("Range")); rg != "" {
		start, end, ok := parseRange(rg, int64(len(cp.Data)))
		if !ok {
			h.Set("Content-Range", fmt.Sprintf("bytes */%d", len(cp.Data)))
			return errRange()
		}
		data, status = cp.Data[start:end+1], http.StatusPartialContent
		h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(cp.Data)))
	}
	h.Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		w.Write(data)
	}
	return nil
}

// parseRange reads "bytes=a-b" or "bytes=a-" against a file of size bytes; a start past the end is not ok.
func parseRange(h string, size int64) (int64, int64, bool) {
	spec, ok := strings.CutPrefix(h, "bytes=")
	if !ok {
		return 0, 0, false
	}
	a, b, _ := strings.Cut(spec, "-")
	start, err := strconv.ParseInt(a, 10, 64)
	if err != nil || start < 0 || start >= size {
		return 0, 0, false
	}
	end := size - 1
	if e, err := strconv.ParseInt(b, 10, 64); err == nil && e < end {
		end = e
	}
	return start, end, end >= start
}

// write changes an existing directory or file.
func (s *Service) write(w http.ResponseWriter, r *http.Request, account, share, p string, dir bool, status int, f func(*Entry) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sh, e, err := s.entry(account, share, p)
	if err != nil {
		return err
	}
	if e == nil || e.Dir != dir {
		return errNoResource()
	}
	if p == "" { // the root directory is the share
		e = &Entry{Dir: true}
	}
	if err := f(e); err != nil {
		return err
	}
	e.ETag, e.Modified = s.etag(), time.Now().UTC()
	if p == "" {
		sh.ETag, sh.Modified = e.ETag, e.Modified
	}
	entryHeaders(w, e)
	w.WriteHeader(status)
	return nil
}

func (s *Service) setFileProps(w http.ResponseWriter, r *http.Request, account, share, p string) error {
	return s.write(w, r, account, share, p, false, http.StatusOK, func(e *Entry) error {
		if v := r.Header.Get("x-ms-content-length"); v != "" {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil || n < 0 {
				return errHeader("x-ms-content-length")
			}
			if limit := s.hub.MaxUpload(); n > limit {
				return core.Errorf(413, "RequestBodyTooLarge", "%s", s.hub.TooBig("files", p, n, limit))
			}
			e.Data = resize(e.Data, n)
			e.Ranges = clip(e.Ranges, n)
		}
		e.Props = fileProps(r.Header) // headers left out are cleared, as Azure does
		return nil
	})
}

func resize(b []byte, n int64) []byte {
	if int64(len(b)) >= n {
		return b[:n:n]
	}
	return append(b, make([]byte, n-int64(len(b)))...)
}

// clip cuts the written ranges to a file of n bytes.
func clip(rs [][2]int64, n int64) [][2]int64 {
	var out [][2]int64
	for _, r := range rs {
		if r[0] < n {
			out = append(out, [2]int64{r[0], min(r[1], n-1)})
		}
	}
	return out
}

// mark records [a, b] as written (set) or cleared (not set), keeping the ranges sorted, apart and merged.
func mark(rs [][2]int64, a, b int64, set bool) [][2]int64 {
	var out [][2]int64
	for _, r := range rs { // take [a, b] out of every range
		if r[1] < a || r[0] > b {
			out = append(out, r)
			continue
		}
		if r[0] < a {
			out = append(out, [2]int64{r[0], a - 1})
		}
		if r[1] > b {
			out = append(out, [2]int64{b + 1, r[1]})
		}
	}
	if set {
		out = append(out, [2]int64{a, b})
	}
	slices.SortFunc(out, func(x, y [2]int64) int { return int(x[0] - y[0]) })
	var merged [][2]int64
	for _, r := range out {
		if n := len(merged); n > 0 && merged[n-1][1]+1 >= r[0] {
			merged[n-1][1] = max(merged[n-1][1], r[1])
		} else {
			merged = append(merged, r)
		}
	}
	return merged
}

func (s *Service) putRange(w http.ResponseWriter, r *http.Request, account, share, p string) error {
	rg := or(r.Header.Get("x-ms-range"), r.Header.Get("Range"))
	spec, ok := strings.CutPrefix(rg, "bytes=")
	as, bs, _ := strings.Cut(spec, "-")
	a, errA := strconv.ParseInt(as, 10, 64)
	b, errB := strconv.ParseInt(bs, 10, 64)
	if !ok || errA != nil || errB != nil || a < 0 || b < a {
		return errHeader("x-ms-range")
	}
	write := r.Header.Get("x-ms-write")
	if write != "update" && write != "clear" {
		return errHeader("x-ms-write")
	}
	var body []byte
	if write == "update" {
		body = core.Body(r)
		if int64(len(body)) != b-a+1 {
			return errHeader("Content-Length")
		}
	}
	return s.write(w, r, account, share, p, false, http.StatusCreated, func(e *Entry) error {
		if b >= int64(len(e.Data)) { // a range writes inside the file; resize first to grow it
			return errRange()
		}
		if write == "update" {
			copy(e.Data[a:], body)
		} else {
			clear(e.Data[a : b+1])
		}
		e.Ranges = mark(e.Ranges, a, b, write == "update")
		return nil
	})
}

func (s *Service) listRanges(w http.ResponseWriter, r *http.Request, account, share, p string) error {
	s.mu.Lock()
	_, e, err := s.entry(account, share, p)
	if err == nil && (e == nil || e.Dir) {
		err = errNoResource()
	}
	var cp Entry
	if err == nil {
		cp = *e
	}
	s.mu.Unlock()
	if err != nil {
		return err
	}
	lo, hi := int64(0), int64(len(cp.Data))-1
	if rg := or(r.Header.Get("x-ms-range"), r.Header.Get("Range")); rg != "" {
		as, bs, _ := strings.Cut(strings.TrimPrefix(rg, "bytes="), "-")
		a, errA := strconv.ParseInt(as, 10, 64)
		b, errB := strconv.ParseInt(bs, 10, 64)
		if errA != nil || errB != nil || a < 0 || b < a || b >= int64(len(cp.Data)) {
			return errRange()
		}
		lo, hi = a, b
	}
	type rng struct {
		Start int64
		End   int64
	}
	var out struct {
		XMLName xml.Name `xml:"Ranges"`
		Range   []rng
	}
	for _, x := range cp.Ranges {
		if x[1] >= lo && x[0] <= hi {
			out.Range = append(out.Range, rng{max(x[0], lo), min(x[1], hi)})
		}
	}
	w.Header().Set("ETag", cp.ETag)
	w.Header().Set("Last-Modified", cp.Modified.Format(http.TimeFormat))
	w.Header().Set("x-ms-content-length", strconv.Itoa(len(cp.Data)))
	return writeXML(w, out)
}

func (s *Service) delete(w http.ResponseWriter, account, share, p string, dir bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sh, e, err := s.entry(account, share, p)
	if err != nil {
		return err
	}
	if e == nil || e.Dir != dir {
		return errNoResource()
	}
	if dir {
		for n := range sh.Entries {
			if strings.HasPrefix(n, p+"/") {
				return core.Errorf(409, "DirectoryNotEmpty", "The specified directory is not empty.")
			}
		}
	}
	delete(sh.Entries, p)
	w.WriteHeader(http.StatusAccepted)
	return nil
}

func (s *Service) listDir(w http.ResponseWriter, r *http.Request, account, share, p string) error {
	q := r.URL.Query()
	max := 5000
	if v := q.Get("maxresults"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return core.Errorf(400, "InvalidQueryParameterValue", "Value for the query parameter maxresults is invalid.")
		}
		max = min(n, max)
	}
	type props struct {
		ContentLength int64 `xml:"Content-Length"`
	}
	type item struct {
		Name       string `xml:"Name"`
		FileID     string `xml:"FileId"`
		Properties props  `xml:"Properties"`
	}
	var out struct {
		XMLName         xml.Name `xml:"EnumerationResults"`
		ServiceEndpoint string   `xml:"ServiceEndpoint,attr"`
		ShareName       string   `xml:"ShareName,attr"`
		DirectoryPath   string   `xml:"DirectoryPath,attr"`
		Prefix          string   `xml:"Prefix"`
		Marker          string   `xml:"Marker"`
		Directories     []item   `xml:"Entries>Directory"`
		Files           []item   `xml:"Entries>File"`
		NextMarker      string   `xml:"NextMarker"`
	}
	out.ServiceEndpoint, out.ShareName, out.DirectoryPath = "https://"+r.Host+"/", share, p
	out.Prefix, out.Marker = q.Get("prefix"), q.Get("marker")
	s.mu.Lock()
	sh, e, err := s.entry(account, share, p)
	if err == nil && (e == nil || !e.Dir) {
		err = errNoResource() // Azure says ResourceNotFound for a missing directory
	}
	if err != nil {
		s.mu.Unlock()
		return err
	}
	dirPrefix := ""
	if p != "" {
		dirPrefix = p + "/"
	}
	count := 0
	for _, n := range core.SortedKeys(sh.Entries) { // directories and files come together, in name order
		name, ok := strings.CutPrefix(n, dirPrefix)
		if !ok || strings.Contains(name, "/") || !strings.HasPrefix(name, out.Prefix) || out.Marker != "" && name < out.Marker {
			continue
		}
		if count == max {
			out.NextMarker = name
			break
		}
		count++
		x := sh.Entries[n]
		it := item{Name: name, FileID: "11529285414812647424", Properties: props{int64(len(x.Data))}}
		if x.Dir {
			out.Directories = append(out.Directories, it)
		} else {
			out.Files = append(out.Files, it)
		}
	}
	s.mu.Unlock()
	return writeXML(w, out)
}
