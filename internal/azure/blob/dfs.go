package blob

import (
	"encoding/base64"
	"encoding/json"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// DFS is the Data Lake Storage Gen2 endpoint, <account>.dfs.localhost. It works on Blob's state: a file
// system is a container, and a file or directory is a blob (a directory is an empty blob whose metadata
// says hdi_isfolder), so the Blob API sees everything that is made here. It needs an account with a
// hierarchical namespace (isHnsEnabled), as Azure does.
type DFS struct{ hub *core.Hub }

// NewDFS creates the Data Lake Gen2 service. The Blob service must be running too.
func NewDFS(hub *core.Hub) *DFS { return &DFS{hub} }

func (d *DFS) Info() core.Info {
	return core.Info{Name: "azdatalake", Cloud: core.Azure, Title: "Data Lake Storage Gen2", Protocol: "azure-json", Match: armres.HostMatch("dfs")}
}

// hns tells if a storage account was made with a hierarchical namespace.
func (s *Service) hns(account string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, r := range s.st.ARM {
		if id, ok := arm.ParseID(k); ok && strings.EqualFold(id.Type(), accounts) && strings.EqualFold(id.Name(), account) {
			v, _ := armres.Props(r)["isHnsEnabled"].(bool)
			return v
		}
	}
	return false
}

func dfsError(w http.ResponseWriter, r *http.Request, c *core.Ctx, err error) {
	e := core.AsError(err)
	switch e.Code { // the Blob helpers this code shares speak of blobs and containers
	case "BlobNotFound":
		e = core.Errorf(404, "PathNotFound", "The specified path does not exist.")
	case "ContainerNotFound":
		e = core.Errorf(404, "FilesystemNotFound", "The specified filesystem does not exist.")
	}
	c.ErrCode = e.Code
	w.Header().Set("x-ms-error-code", e.Code)
	if r.Method == http.MethodHead || e.Status == http.StatusNotModified {
		w.WriteHeader(e.Status)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(e.Status)
	json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": e.Code,
		"message": e.Msg + "\nRequestId:" + c.RequestID + "\nTime:" + time.Now().UTC().Format("2006-01-02T15:04:05.0000000Z")}})
}

func (d *DFS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	r = armres.AccountPath(r, "dfs")
	c := core.C(r)
	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 3)
	account, fs, p := parts[0], "", ""
	if len(parts) > 1 {
		fs = parts[1]
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
	s, ok := d.hub.Service("azblob").(*Service)
	if !ok {
		dfsError(w, r, c, core.Errorf(500, "InternalError", "the Blob Storage service is not running"))
		return
	}
	op, fn := s.dfsRoute(w, r, account, fs, p)
	c.Op = op
	if fn == nil {
		dfsError(w, r, c, core.Errorf(400, "UnsupportedHttpVerb", "pogi does not implement %s %s", r.Method, r.URL.RequestURI()))
		return
	}
	if err := c.Intercept(); err != nil {
		dfsError(w, r, c, err)
		return
	}
	if !s.hns(account) {
		dfsError(w, r, c, core.Errorf(400, "HierarchicalNamespaceNotEnabled", "This operation is only supported on a hierarchical namespace account."))
		return
	}
	if err := fn(); err != nil {
		dfsError(w, r, c, err)
	}
}

func (s *Service) dfsRoute(w http.ResponseWriter, r *http.Request, account, fs, p string) (string, handler) {
	q := r.URL.Query()
	resource, action, m := q.Get("resource"), q.Get("action"), r.Method
	switch {
	case fs == "":
		if m == http.MethodGet && resource == "account" {
			return "ListFileSystems", func() error { return s.dfsListFileSystems(w, account) }
		}
	case p == "" && resource == "filesystem":
		switch m {
		case http.MethodPut:
			return "CreateFileSystem", func() error { return s.dfsCreateFS(w, r, account, fs) }
		case http.MethodPatch:
			return "SetFileSystemProperties", func() error {
				return s.withContainer(account, fs, func(ct *Container) error {
					ct.Meta, ct.ETag, ct.Modified = parseProps(r.Header.Get("x-ms-properties")), s.etag(), time.Now().UTC()
					return dfsHeaders(w, ct.ETag, ct.Modified, http.StatusOK)
				})
			}
		case http.MethodHead:
			return "GetFileSystemProperties", func() error {
				return s.withContainer(account, fs, func(ct *Container) error {
					w.Header().Set("x-ms-properties", encodeProps(ct.Meta))
					w.Header().Set("x-ms-namespace-enabled", "true")
					return dfsHeaders(w, ct.ETag, ct.Modified, http.StatusOK)
				})
			}
		case http.MethodDelete:
			return "DeleteFileSystem", func() error { return s.dfsDeleteFS(w, r, account, fs) }
		case http.MethodGet:
			return "ListPaths", func() error { return s.dfsList(w, r, account, fs) }
		}
	case p == "":
	case m == http.MethodPut:
		if r.Header.Get("x-ms-rename-source") != "" {
			return "RenamePath", func() error { return s.dfsRename(w, r, account, fs, p) }
		}
		if resource == "file" || resource == "directory" {
			return "CreatePath", func() error { return s.dfsCreate(w, r, account, fs, p, resource == "directory") }
		}
	case m == http.MethodPatch:
		switch action {
		case "append":
			return "AppendData", func() error { return s.dfsAppend(w, r, account, fs, p) }
		case "flush":
			return "FlushData", func() error { return s.dfsFlush(w, r, account, fs, p) }
		case "setProperties":
			return "SetPathProperties", func() error {
				return s.dfsWrite(w, r, account, fs, p, func(b *Blob) { b.Meta = parseProps(r.Header.Get("x-ms-properties")) })
			}
		case "setAccessControl":
			return "SetAccessControl", func() error {
				return s.dfsWrite(w, r, account, fs, p, func(b *Blob) {
					if b.POSIX == nil {
						b.POSIX = map[string]string{}
					}
					for hdr, k := range map[string]string{"x-ms-owner": "owner", "x-ms-group": "group", "x-ms-permissions": "permissions", "x-ms-acl": "acl"} {
						if v := r.Header.Get(hdr); v != "" {
							b.POSIX[k] = v
						}
					}
				})
			}
		}
	case m == http.MethodGet:
		return "ReadPath", func() error { return s.getBlob(w, r, account, fs, p) }
	case m == http.MethodHead:
		if action == "getStatus" || action == "getAccessControl" {
			return "GetPathStatus", func() error { return s.dfsStatus(w, account, fs, p) }
		}
		return "GetPathProperties", func() error { return s.getBlob(w, r, account, fs, p) }
	case m == http.MethodDelete:
		return "DeletePath", func() error { return s.dfsDelete(w, r, account, fs, p) }
	}
	return "", nil
}

// ---- helpers ----

func dfsHeaders(w http.ResponseWriter, etag string, mod time.Time, status int) error {
	h := w.Header()
	h.Set("ETag", etag)
	h.Set("Last-Modified", mod.Format(http.TimeFormat))
	w.WriteHeader(status)
	return nil
}

func isDir(b *Blob) bool { return b.Meta["hdi_isfolder"] == "true" }

// parseProps reads x-ms-properties: "name=base64value,name2=base64value2".
func parseProps(h string) map[string]string {
	var m map[string]string
	for _, kv := range strings.Split(h, ",") {
		k, v, ok := strings.Cut(kv, "=")
		if !ok || k == "" {
			continue
		}
		if raw, err := base64.StdEncoding.DecodeString(v); err == nil {
			if m == nil {
				m = map[string]string{}
			}
			m[k] = string(raw)
		}
	}
	return m
}

func encodeProps(m map[string]string) string {
	var out []string
	for _, k := range core.SortedKeys(m) {
		if k != "hdi_isfolder" {
			out = append(out, k+"="+base64.StdEncoding.EncodeToString([]byte(m[k])))
		}
	}
	return strings.Join(out, ",")
}

var dfsPropHeaders = map[string]string{"x-ms-content-type": "Content-Type", "x-ms-cache-control": "Cache-Control",
	"x-ms-content-disposition": "Content-Disposition", "x-ms-content-encoding": "Content-Encoding", "x-ms-content-language": "Content-Language"}

func dfsProps(h http.Header, props map[string]string) map[string]string {
	if props == nil {
		props = map[string]string{}
	}
	for hdr, k := range dfsPropHeaders {
		if v := h.Get(hdr); v != "" {
			props[k] = v
		}
	}
	return props
}

func errNoPath() error { return core.Errorf(404, "PathNotFound", "The specified path does not exist.") }

func conflict() error {
	return core.Errorf(409, "PathConflict", "The specified path, or an element of the path, exists and its resource type is invalid for this operation.")
}

// cleanPath refuses a path with an empty, "." or ".." segment.
func cleanPath(p string) error {
	for _, seg := range strings.Split(p, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return core.Errorf(400, "InvalidResourceName", "The specified resource name contains invalid characters.")
		}
	}
	return nil
}

// mkParents makes the directories above p that are not there yet; a file in the way is a conflict. Lock held.
func (s *Service) mkParents(ct *Container, p string) error {
	for i := 0; i < len(p); i++ {
		if p[i] != '/' {
			continue
		}
		dir := p[:i]
		switch b := ct.Blobs[dir]; {
		case b == nil:
			ct.Blobs[dir] = s.newBlob(dir, true)
		case !isDir(b):
			return conflict()
		}
	}
	return nil
}

func (s *Service) newBlob(name string, dir bool) *Blob {
	now := time.Now().UTC()
	b := &Blob{Name: name, Type: "BlockBlob", Data: []byte{}, Props: map[string]string{"Content-Type": "application/octet-stream"},
		Created: now, Modified: now, ETag: s.etag()}
	if dir {
		b.Meta = map[string]string{"hdi_isfolder": "true"}
	}
	return b
}

// dfsFS finds a file system. Lock held.
func (s *Service) dfsFS(account, fs string) (*Container, error) {
	ct := s.account(account).Containers[fs]
	if ct == nil {
		return nil, core.Errorf(404, "FilesystemNotFound", "The specified filesystem does not exist.")
	}
	return ct, nil
}

// ---- file systems ----

func (s *Service) dfsListFileSystems(w http.ResponseWriter, account string) error {
	s.mu.Lock()
	a := s.account(account)
	var out []map[string]string
	for _, n := range core.SortedKeys(a.Containers) {
		ct := a.Containers[n]
		out = append(out, map[string]string{"name": n, "lastModified": ct.Modified.Format(http.TimeFormat), "etag": ct.ETag})
	}
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	return json.NewEncoder(w).Encode(map[string]any{"filesystems": out})
}

func (s *Service) dfsCreateFS(w http.ResponseWriter, r *http.Request, account, fs string) error {
	if len(fs) < 3 || len(fs) > 63 || strings.ToLower(fs) != fs {
		return core.Errorf(400, "InvalidResourceName", "The specified resource name contains invalid characters.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.account(account)
	if a.Containers[fs] != nil {
		return core.Errorf(409, "FilesystemAlreadyExists", "The specified filesystem already exists.")
	}
	now := time.Now().UTC()
	ct := &Container{Name: fs, Created: now, Modified: now, ETag: s.etag(), Blobs: map[string]*Blob{}, Meta: parseProps(r.Header.Get("x-ms-properties"))}
	a.Containers[fs] = ct
	w.Header().Set("x-ms-namespace-enabled", "true")
	return dfsHeaders(w, ct.ETag, ct.Modified, http.StatusCreated)
}

func (s *Service) dfsDeleteFS(w http.ResponseWriter, r *http.Request, account, fs string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ct, err := s.dfsFS(account, fs)
	if err != nil {
		return err
	}
	if ct.Lease.active() && ct.Lease.ID != r.Header.Get("x-ms-lease-id") {
		return core.Errorf(412, "LeaseIdMissing", "There is currently a lease on the filesystem and no lease ID was specified in the request.")
	}
	delete(s.account(account).Containers, fs)
	w.WriteHeader(http.StatusAccepted)
	return nil
}

// ---- paths ----

func (s *Service) dfsCreate(w http.ResponseWriter, r *http.Request, account, fs, p string, dir bool) error {
	if err := cleanPath(p); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ct, err := s.dfsFS(account, fs)
	if err != nil {
		return err
	}
	cur := ct.Blobs[p]
	if cur != nil && r.Header.Get("If-None-Match") == "*" {
		return core.Errorf(409, "PathAlreadyExists", "The specified path already exists.")
	}
	if cur != nil && isDir(cur) != dir {
		return conflict()
	}
	if err := checkWrite(r.Header, cur); err != nil {
		return err
	}
	if err := s.mkParents(ct, p); err != nil {
		return err
	}
	if cur != nil && dir { // an existing directory is left as it is
		return dfsHeaders(w, cur.ETag, cur.Modified, http.StatusCreated)
	}
	b := s.newBlob(p, dir)
	b.Meta = parseProps(r.Header.Get("x-ms-properties"))
	if dir {
		if b.Meta == nil {
			b.Meta = map[string]string{}
		}
		b.Meta["hdi_isfolder"] = "true"
	}
	b.Props = dfsProps(r.Header, b.Props)
	if cur != nil {
		b.Created, b.Lease = cur.Created, cur.Lease
	}
	ct.Blobs[p] = b
	return dfsHeaders(w, b.ETag, b.Modified, http.StatusCreated)
}

// dfsFile finds a file to write to. Lock held.
func (s *Service) dfsFile(account, fs, p string) (*Container, *Blob, error) {
	ct, err := s.dfsFS(account, fs)
	if err != nil {
		return nil, nil, err
	}
	b := ct.Blobs[p]
	switch {
	case b == nil:
		return nil, nil, errNoPath()
	case isDir(b):
		return nil, nil, conflict()
	}
	return ct, b, nil
}

func position(r *http.Request) (int, error) {
	pos, err := strconv.Atoi(r.URL.Query().Get("position"))
	if err != nil || pos < 0 {
		return 0, core.Errorf(400, "InvalidQueryParameterValue", "Value for the query parameter position is missing or invalid.")
	}
	return pos, nil
}

func errFlushPosition() error {
	return core.Errorf(400, "InvalidFlushPosition", "The uploaded data is not contiguous or the position query parameter value is not equal to the length of the file after appending the uploaded data.")
}

// commit makes the appended chunks part of the file, if together with Data they are contiguous and end at
// end (what a flush asks for). Clients append chunks in parallel and in any order. Lock held.
func (s *Service) commit(r *http.Request, b *Blob, end int) error {
	pos := len(b.Data)
	data := b.Data
	for _, at := range slices.Sorted(maps.Keys(b.Pending)) {
		if int(at) != pos {
			return errFlushPosition()
		}
		data = append(data, b.Pending[at]...)
		pos = len(data)
	}
	if pos != end {
		return errFlushPosition()
	}
	b.Data, b.Pending = data, nil
	b.Props = dfsProps(r.Header, b.Props)
	b.ETag, b.Modified = s.etag(), time.Now().UTC()
	return nil
}

func (s *Service) dfsAppend(w http.ResponseWriter, r *http.Request, account, fs, p string) error {
	pos, err := position(r)
	if err != nil {
		return err
	}
	body := core.Body(r)
	s.mu.Lock()
	defer s.mu.Unlock()
	_, b, err := s.dfsFile(account, fs, p)
	if err != nil {
		return err
	}
	if pos < len(b.Data) {
		return errFlushPosition()
	}
	total := int64(len(b.Data))
	for at, c := range b.Pending {
		if int(at) != pos {
			total += int64(len(c))
		}
	}
	if err := s.tooBig(p, max(total+int64(len(body)), int64(pos+len(body)))); err != nil {
		return err
	}
	if b.Pending == nil {
		b.Pending = map[int64][]byte{}
	}
	b.Pending[int64(pos)] = body
	if r.URL.Query().Get("flush") == "true" {
		if err := s.commit(r, b, pos+len(body)); err != nil {
			return err
		}
	}
	w.Header().Set("x-ms-request-server-encrypted", "true")
	w.WriteHeader(http.StatusAccepted)
	return nil
}

func (s *Service) dfsFlush(w http.ResponseWriter, r *http.Request, account, fs, p string) error {
	pos, err := position(r)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, b, err := s.dfsFile(account, fs, p)
	if err != nil {
		return err
	}
	if err := checkWrite(r.Header, b); err != nil {
		return err
	}
	if err := s.commit(r, b, pos); err != nil {
		return err
	}
	return dfsHeaders(w, b.ETag, b.Modified, http.StatusOK)
}

// dfsWrite changes an existing path's attributes.
func (s *Service) dfsWrite(w http.ResponseWriter, r *http.Request, account, fs, p string, f func(*Blob)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ct, err := s.dfsFS(account, fs)
	if err != nil {
		return err
	}
	b := ct.Blobs[p]
	if b == nil {
		return errNoPath()
	}
	if err := checkWrite(r.Header, b); err != nil {
		return err
	}
	keepDir := isDir(b)
	f(b)
	if keepDir {
		if b.Meta == nil {
			b.Meta = map[string]string{}
		}
		b.Meta["hdi_isfolder"] = "true"
	}
	b.ETag, b.Modified = s.etag(), time.Now().UTC()
	return dfsHeaders(w, b.ETag, b.Modified, http.StatusOK)
}

func (s *Service) dfsStatus(w http.ResponseWriter, account, fs, p string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ct, err := s.dfsFS(account, fs)
	if err != nil {
		return err
	}
	b := ct.Blobs[p]
	if b == nil {
		return errNoPath()
	}
	h := w.Header()
	kind, perm := "file", "rw-r-----"
	if isDir(b) {
		kind, perm = "directory", "rwxr-x---"
	}
	h.Set("x-ms-resource-type", kind)
	h.Set("x-ms-properties", encodeProps(b.Meta))
	h.Set("x-ms-owner", or(b.POSIX["owner"], "$superuser"))
	h.Set("x-ms-group", or(b.POSIX["group"], "$superuser"))
	h.Set("x-ms-permissions", or(b.POSIX["permissions"], perm))
	h.Set("x-ms-acl", or(b.POSIX["acl"], "user::"+perm[:3]+",group::"+perm[3:6]+",other::"+perm[6:]))
	h.Set("Content-Length", strconv.Itoa(len(b.Data)))
	return dfsHeaders(w, b.ETag, b.Modified, http.StatusOK)
}

func hasChildren(ct *Container, p string) bool {
	for n := range ct.Blobs {
		if strings.HasPrefix(n, p+"/") {
			return true
		}
	}
	return false
}

func (s *Service) dfsDelete(w http.ResponseWriter, r *http.Request, account, fs, p string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ct, err := s.dfsFS(account, fs)
	if err != nil {
		return err
	}
	b := ct.Blobs[p]
	if b == nil {
		return errNoPath()
	}
	if err := checkWrite(r.Header, b); err != nil {
		return err
	}
	if isDir(b) && r.URL.Query().Get("recursive") != "true" && hasChildren(ct, p) {
		return core.Errorf(409, "DirectoryNotEmpty", "The recursive query parameter value must be true to delete a non-empty directory.")
	}
	for n := range ct.Blobs {
		if n == p || strings.HasPrefix(n, p+"/") {
			delete(ct.Blobs, n)
		}
	}
	w.WriteHeader(http.StatusOK)
	return nil
}

// dfsRename moves a file, or a directory and everything below it. Without If-None-Match: * it replaces
// what is at the destination, as Azure does.
func (s *Service) dfsRename(w http.ResponseWriter, r *http.Request, account, fs, dp string) error {
	if err := cleanPath(dp); err != nil {
		return err
	}
	src := r.Header.Get("x-ms-rename-source")
	if i := strings.IndexByte(src, '?'); i >= 0 { // a SAS token
		src = src[:i]
	}
	src, err := url.PathUnescape(src)
	sfs, sp, ok := strings.Cut(strings.Trim(src, "/"), "/")
	if err != nil || !ok || cleanPath(sp) != nil {
		return core.Errorf(400, "InvalidSourceUri", "The source URI is invalid.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	from, err := s.dfsFS(account, sfs)
	if err != nil {
		return err
	}
	to, err := s.dfsFS(account, fs)
	if err != nil {
		return err
	}
	sb, db := from.Blobs[sp], to.Blobs[dp]
	switch {
	case sb == nil:
		return core.Errorf(404, "SourcePathNotFound", "The source path for a rename operation does not exist.")
	case db != nil && r.Header.Get("If-None-Match") == "*":
		return core.Errorf(409, "PathAlreadyExists", "The specified path already exists.")
	case from == to && (dp == sp || strings.HasPrefix(dp, sp+"/")):
		return conflict() // a directory cannot move into itself
	case db != nil && isDir(db) != isDir(sb):
		return conflict()
	case db != nil && isDir(db) && hasChildren(to, dp):
		return core.Errorf(409, "PathAlreadyExists", "The destination directory is not empty.")
	}
	if err := checkWrite(r.Header, db); err != nil {
		return err
	}
	if err := s.mkParents(to, dp); err != nil {
		return err
	}
	var names []string
	for n := range from.Blobs {
		if n == sp || strings.HasPrefix(n, sp+"/") {
			names = append(names, n)
		}
	}
	slices.Sort(names)
	for _, n := range names {
		b := from.Blobs[n]
		delete(from.Blobs, n)
		b.Name = dp + n[len(sp):]
		to.Blobs[b.Name] = b
	}
	return dfsHeaders(w, sb.ETag, sb.Modified, http.StatusCreated)
}

// dfsList lists the paths of a file system, or of one directory, in name order.
func (s *Service) dfsList(w http.ResponseWriter, r *http.Request, account, fs string) error {
	q := r.URL.Query()
	dir, recursive := strings.Trim(q.Get("directory"), "/"), q.Get("recursive") != "false"
	max := 5000
	if v := q.Get("maxResults"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return core.Errorf(400, "InvalidQueryParameterValue", "Value for the query parameter maxResults is invalid.")
		}
		max = min(n, max)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ct, err := s.dfsFS(account, fs)
	if err != nil {
		return err
	}
	if dir != "" && (ct.Blobs[dir] == nil || !isDir(ct.Blobs[dir])) {
		return errNoPath()
	}
	prefix := ""
	if dir != "" {
		prefix = dir + "/"
	}
	out := []map[string]string{}
	next := ""
	for _, n := range core.SortedKeys(ct.Blobs) {
		rest, ok := strings.CutPrefix(n, prefix)
		if !ok || n <= q.Get("continuation") || !recursive && strings.Contains(rest, "/") {
			continue
		}
		if len(out) == max {
			next = out[len(out)-1]["name"]
			break
		}
		b := ct.Blobs[n]
		item := map[string]string{"name": n, "contentLength": strconv.Itoa(len(b.Data)), "lastModified": b.Modified.Format(http.TimeFormat), "etag": b.ETag,
			"owner": or(b.POSIX["owner"], "$superuser"), "group": or(b.POSIX["group"], "$superuser"), "permissions": or(b.POSIX["permissions"], "rw-r-----")}
		if isDir(b) {
			item["isDirectory"], item["contentLength"], item["permissions"] = "true", "0", or(b.POSIX["permissions"], "rwxr-x---")
		}
		out = append(out, item)
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if next != "" {
		w.Header().Set("x-ms-continuation", next)
	}
	return json.NewEncoder(w).Encode(map[string]any{"paths": out})
}
