package ecr

// The OCI Distribution API that docker push and pull speak, under /v2/ on
// pogi's port and on localhost:<Config.ECRPort>, the host of repositoryUri.

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
)

// isRegistry matches registry traffic, which is never SigV4-signed, unlike
// API Gateway v2's /v2/apis, /v2/domainnames, /v2/vpclinks and /v2/tags.
// It also has to look like the registry's own paths: the Google Cloud Run and Cloud Tasks
// APIs live under /v2/projects/ too.
func isRegistry(r *http.Request) bool {
	p := r.URL.Path
	return strings.HasPrefix(p, "/v2/") && !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-") &&
		(p == "/v2/" || p == "/v2/_catalog" || route.MatchString(p))
}

var (
	// /v2/<name>/ then tags/list, manifests/<reference>, blobs/uploads/<id> or blobs/<digest>
	route = regexp.MustCompile(`^/v2/(.+?)/(tags/list|manifests/([^/]+)|blobs/uploads/([^/]*)|blobs/([^/]+))$`)
	tagRe = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$`)
)

// regOps names registry calls after the ECR actions IAM checks for them.
var regOps = map[string]string{
	"GET ping": "GetAuthorizationToken", "GET catalog": "DescribeRepositories", "GET tags": "ListImages",
	"GET manifests": "BatchGetImage", "HEAD manifests": "BatchGetImage", "PUT manifests": "PutImage", "DELETE manifests": "BatchDeleteImage",
	"GET blobs": "GetDownloadUrlForLayer", "HEAD blobs": "BatchCheckLayerAvailability", "DELETE blobs": "BatchDeleteImage",
	"POST uploads": "InitiateLayerUpload", "PATCH uploads": "UploadLayerPart", "GET uploads": "UploadLayerPart",
	"DELETE uploads": "UploadLayerPart", "PUT uploads": "CompleteLayerUpload",
}

// regErrs are the registry's codes for the API errors of the code both share.
var regErrs = map[string]core.Error{
	"InvalidParameterException":        {Status: 400, Code: "MANIFEST_INVALID"},
	"LayersNotFoundException":          {Status: 400, Code: "MANIFEST_BLOB_UNKNOWN"},
	"ImageTagAlreadyExistsException":   {Status: 400, Code: "TAG_INVALID"},
	"ImageDigestDoesNotMatchException": {Status: 400, Code: "DIGEST_INVALID"},
	"InvalidLayerException":            {Status: 400, Code: "DIGEST_INVALID"},
	"UploadNotFoundException":          {Status: 404, Code: "BLOB_UPLOAD_UNKNOWN"},
	"InvalidLayerPartException":        {Status: 416, Code: "BLOB_UPLOAD_INVALID"},
}

// registry serves the OCI Distribution API. It takes anonymous requests and
// any credentials, such as GetAuthorizationToken's.
func (s *Service) registry(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
	m := route.FindStringSubmatch(r.URL.Path)
	kind := ""
	switch {
	case r.URL.Path == "/v2/":
		kind = "ping"
	case r.URL.Path == "/v2/_catalog":
		kind = "catalog"
	case m == nil:
	case strings.HasPrefix(m[2], "blobs/uploads/"):
		kind = "uploads"
	default:
		kind, _, _ = strings.Cut(m[2], "/")
	}
	call := r.Method + " " + kind
	if c.Op = regOps[call]; c.Op == "" {
		regErr(w, c, core.Errorf(http.StatusNotFound, "UNSUPPORTED", "The operation is unsupported."))
		return
	}
	if err := c.Intercept(); err != nil {
		regErr(w, c, err)
		return
	}
	switch kind {
	case "ping":
		regJSON(w, struct{}{})
		return
	case "catalog":
		s.mu.Lock()
		names := []string{}
		if hp := hostPrefix(r.Host); hp != "" { // the repositories of a Container Registry
			for n := range s.st.Ext {
				if rest, ok := strings.CutPrefix(n, hp); ok {
					names = append(names, rest)
				}
			}
		} else {
			for _, repo := range s.st.Repos {
				names = append(names, repo.RepositoryName)
			}
		}
		s.mu.Unlock()
		slices.Sort(names)
		regJSON(w, map[string][]string{"repositories": page(w, r, slices.Compact(names))})
		return
	}
	name, ref := m[1], m[3]+m[4]+m[5] // the path has one of a reference, an upload id and a digest
	s.mu.Lock()
	var repo *Repo
	if hp := hostPrefix(r.Host); hp != "" {
		repo = s.findExt(hp+name, call == "POST uploads")
	} else {
		repo = s.find(name, call == "POST uploads")
	}
	s.mu.Unlock()
	if repo == nil { // as in ECR, a push does not create the repository; under a claimed prefix it does
		regErr(w, c, core.Errorf(http.StatusNotFound, "NAME_UNKNOWN", "repository name not known to registry"))
		return
	}
	var err error
	switch call {
	case "GET tags":
		s.mu.Lock()
		tags := []string{}
		for _, img := range repo.Images {
			tags = append(tags, img.Tags...)
		}
		s.mu.Unlock()
		slices.Sort(tags)
		regJSON(w, map[string]any{"name": name, "tags": page(w, r, tags)})
	case "GET manifests", "HEAD manifests":
		s.mu.Lock()
		d, img := repo.image(idOf(ref))
		s.mu.Unlock()
		if img == nil {
			err = core.Errorf(http.StatusNotFound, "MANIFEST_UNKNOWN", "manifest unknown")
			break
		}
		w.Header().Set("Content-Type", img.MediaType)
		w.Header().Set("Content-Length", strconv.Itoa(len(img.Manifest)))
		w.Header().Set("Docker-Content-Digest", d)
		w.Write(img.Manifest)
	case "PUT manifests":
		id := idOf(ref)
		if id.ImageTag != "" && !tagRe.MatchString(id.ImageTag) {
			err = core.Errorf(http.StatusBadRequest, "TAG_INVALID", "manifest tag did not match URI")
			break
		}
		var d string
		if d, _, _, err = s.put(repo, core.Body(r), r.Header.Get("Content-Type"), id.ImageTag, id.ImageDigest); err == nil {
			created(w, "/v2/"+name+"/manifests/"+d, d)
		}
	case "DELETE manifests":
		s.mu.Lock()
		_, ok := repo.drop(idOf(ref))
		s.mu.Unlock()
		if !ok {
			err = core.Errorf(http.StatusNotFound, "MANIFEST_UNKNOWN", "manifest unknown")
			break
		}
		w.WriteHeader(http.StatusAccepted)
	case "GET blobs", "HEAD blobs":
		s.mu.Lock()
		_, ok := repo.Blobs[ref] // only verified digests get in: safe in a path
		s.mu.Unlock()
		var f *os.File
		if ok {
			f, err = os.Open(s.blob(ref))
		}
		if !ok || err != nil {
			err = core.Errorf(http.StatusNotFound, "BLOB_UNKNOWN", "blob unknown to registry")
			break
		}
		defer f.Close()
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Docker-Content-Digest", ref)
		http.ServeContent(w, r, "", time.Time{}, f) // HEAD and Range
	case "DELETE blobs":
		s.mu.Lock()
		_, ok := repo.Blobs[ref]
		delete(repo.Blobs, ref) // ponytail: its file stays, other repositories may share it; nothing collects unused blobs
		s.mu.Unlock()
		if !ok {
			err = core.Errorf(http.StatusNotFound, "BLOB_UNKNOWN", "blob unknown to registry")
			break
		}
		w.WriteHeader(http.StatusAccepted)
	case "POST uploads":
		q := r.URL.Query()
		if d := q.Get("mount"); d != "" && s.mount(repo, q.Get("from"), d) {
			created(w, "/v2/"+name+"/blobs/"+d, d)
			break
		}
		if ref, err = s.initiate(repo); err != nil {
			break
		}
		if q.Get("digest") == "" {
			uploading(w, name, ref, 0, http.StatusAccepted)
			break
		}
		fallthrough // a monolithic upload: the body is the blob
	case "PUT uploads":
		d := r.URL.Query().Get("digest")
		if _, err = s.write(repo, ref, -1, body(r)); err == nil {
			err = s.complete(repo, ref, d)
		}
		if err == nil {
			created(w, "/v2/"+name+"/blobs/"+d, d)
		}
	case "PATCH uploads", "GET uploads": // GET: the status, an empty write
		first, status := int64(-1), http.StatusAccepted
		if start, _, ok := strings.Cut(r.Header.Get("Content-Range"), "-"); ok {
			first, _ = strconv.ParseInt(start, 10, 64)
		}
		if r.Method == http.MethodGet {
			status = http.StatusNoContent
		}
		var size int64
		if size, err = s.write(repo, ref, first, body(r)); err == nil {
			uploading(w, name, ref, size, status)
		}
	case "DELETE uploads":
		if !s.claim(repo, ref) {
			err = core.Errorf(http.StatusNotFound, "BLOB_UPLOAD_UNKNOWN", "blob upload unknown to registry")
			break
		}
		os.Remove(s.part(ref))
		w.WriteHeader(http.StatusNoContent)
	}
	if err != nil {
		regErr(w, c, err)
	}
}

// regErr writes an error the registry way.
func regErr(w http.ResponseWriter, c *core.Ctx, err error) {
	e := *core.AsError(err)
	if m, ok := regErrs[e.Code]; ok {
		e.Status, e.Code = m.Status, m.Code
	}
	c.ErrCode = e.Code
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(e.Status)
	json.NewEncoder(w).Encode(map[string][]map[string]string{"errors": {{"code": e.Code, "message": e.Msg}}})
}

func regJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

// uploading answers for an upload in progress: where the rest goes, and how
// much came.
func uploading(w http.ResponseWriter, name, id string, size int64, status int) {
	h := w.Header()
	h.Set("Location", "/v2/"+name+"/blobs/uploads/"+id)
	h.Set("Range", "0-"+strconv.FormatInt(max(size-1, 0), 10))
	h.Set("Docker-Upload-UUID", id)
	w.WriteHeader(status)
}

// created answers a stored blob or manifest.
func created(w http.ResponseWriter, location, digest string) {
	w.Header().Set("Location", location)
	w.Header().Set("Docker-Content-Digest", digest)
	w.WriteHeader(http.StatusCreated)
}

// page applies the n and last parameters of tags/list and _catalog to a
// sorted list, with a Link header to the next page.
func page(w http.ResponseWriter, r *http.Request, all []string) []string {
	q := r.URL.Query()
	i, found := slices.BinarySearch(all, q.Get("last"))
	if found {
		i++
	}
	all = all[i:]
	if n, err := strconv.Atoi(q.Get("n")); err == nil && n > 0 && n < len(all) {
		all = all[:n]
		w.Header().Set("Link", "<"+r.URL.Path+"?n="+strconv.Itoa(n)+"&last="+url.QueryEscape(all[n-1])+`>; rel="next"`)
	}
	return all
}

// body is the request body; the gateway has read a chunked one already.
func body(r *http.Request) io.Reader {
	if b := core.C(r).Payload(); b != nil {
		return bytes.NewReader(b)
	}
	return r.Body
}

// idOf reads a manifest reference: a digest or a tag.
func idOf(ref string) imageID {
	if strings.Contains(ref, ":") {
		return imageID{ImageDigest: ref}
	}
	return imageID{ImageTag: ref}
}

// find resolves a registry name; lock held.
// ponytail: one registry host for all accounts and regions: a name is the
// repository of the default account and region, else the only one so named.
// Give accounts their own hosts (<account>.dkr.ecr.<region>.localhost) when
// names clash.
func (s *Service) find(name string, create bool) *Repo {
	if r := s.findExt(name, create); r != nil {
		return r
	}
	if r := s.st.Repos[core.DefaultAccount+"/"+s.hub.Cfg.Region+"/"+name]; r != nil {
		return r
	}
	var found *Repo
	for _, r := range s.st.Repos {
		if r.RepositoryName == name {
			if found != nil {
				return nil
			}
			found = r
		}
	}
	return found
}

// put stores a manifest in r, tagged when tag is set; want is the digest the
// client names, if any. same reports a push of what was there already.
// ponytail: an index's manifests need not exist, docker pushes the platforms
// it has under a multi-platform index.
func (s *Service) put(r *Repo, body []byte, mediaType, tag, want string) (digest, mt string, same bool, err error) {
	var m struct {
		MediaType string
		Config    struct{ Digest string }
		Layers    []struct {
			Digest string
			Size   int64
		}
		Manifests []struct{ Digest string }
	}
	if json.Unmarshal(body, &m) != nil {
		return "", "", false, invalid("Invalid parameter at 'imageManifest' failed to satisfy constraint: 'must be a valid image manifest'")
	}
	if tag != "" && !tagRe.MatchString(tag) {
		return "", "", false, invalid("Invalid parameter at 'imageTag' failed to satisfy constraint: 'must satisfy regular expression '%s''", tagRe)
	}
	sum := sha256.Sum256(body)
	digest = "sha256:" + hex.EncodeToString(sum[:])
	if want != "" && want != digest {
		return "", "", false, core.Errorf(400, "ImageDigestDoesNotMatchException", "The manifest's digest is %s, not %s", digest, want)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var size int64
	var missing []string
	for _, l := range m.Layers {
		size += l.Size
		if _, ok := r.Blobs[l.Digest]; !ok {
			missing = append(missing, l.Digest)
		}
	}
	if _, ok := r.Blobs[m.Config.Digest]; !ok && m.Config.Digest != "" {
		missing = append(missing, m.Config.Digest)
	}
	if missing != nil {
		return "", "", false, core.Errorf(400, "LayersNotFoundException", "Layers with digests '%s' required for pushing image into repository with name '%s' in the registry with id '%s' do not exist",
			strings.Join(missing, ", "), r.RepositoryName, r.RegistryId)
	}
	for _, sub := range m.Manifests {
		if img := r.Images[sub.Digest]; img != nil {
			size = max(size, img.Size)
		}
	}
	old, _ := r.image(imageID{ImageTag: tag})
	if old != "" && old != digest && r.ImageTagMutability == "IMMUTABLE" {
		return "", "", false, core.Errorf(400, "ImageTagAlreadyExistsException", "The image tag '%s' already exists in the '%s' repository and cannot be overwritten because the repository is immutable.",
			tag, r.RepositoryName)
	}
	img := r.Images[digest]
	same = img != nil && (tag == "" || old == digest)
	if img == nil {
		img = &Image{Manifest: body, MediaType: cmp.Or(m.MediaType, mediaType, "application/vnd.oci.image.manifest.v1+json"), Size: size,
			Pushed: core.Time(time.Now().UTC()), Scanned: r.ImageScanningConfiguration.ScanOnPush}
		if r.Images == nil {
			r.Images = map[string]*Image{}
		}
		r.Images[digest] = img
	}
	if tag != "" && old != digest {
		if prev := r.Images[old]; prev != nil { // it stays, untagged
			prev.Tags = slices.DeleteFunc(prev.Tags, func(t string) bool { return t == tag })
		}
		img.Tags = append(img.Tags, tag)
	}
	return digest, img.MediaType, same, nil
}

// link adds a blob to r; lock held.
func (r *Repo) link(digest string, size int64) {
	if r.Blobs == nil {
		r.Blobs = map[string]int64{}
	}
	r.Blobs[digest] = size
}

// mount links a blob of repository from into r, as a cross-repository mount;
// false sends the client to upload it.
func (s *Service) mount(r *Repo, from, digest string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	src := s.find(from, false)
	if src == nil {
		return false
	}
	size, ok := src.Blobs[digest]
	if ok {
		r.link(digest, size)
	}
	return ok
}

func (s *Service) blob(digest string) string {
	return filepath.Join(s.dir, "blobs", "sha256", strings.TrimPrefix(digest, "sha256:"))
}

func (s *Service) part(id string) string { return filepath.Join(s.dir, "uploads", id) }

// initiate starts a blob upload into r.
// ponytail: uploads in progress live in memory, a restart drops them.
func (s *Service) initiate(r *Repo) (string, error) {
	id := core.UUID()
	if err := os.MkdirAll(filepath.Join(s.dir, "uploads"), 0o755); err != nil {
		return "", err
	}
	if err := os.WriteFile(s.part(id), nil, 0o644); err != nil {
		return "", err
	}
	s.mu.Lock()
	s.uploads[id] = r
	s.mu.Unlock()
	return id, nil
}

func noUpload(id string) error {
	return core.Errorf(400, "UploadNotFoundException", "The upload with id '%s' does not exist", id)
}

// write appends body to an upload of r, at offset first unless that is -1,
// and returns the upload's size.
func (s *Service) write(r *Repo, id string, first int64, body io.Reader) (int64, error) {
	s.mu.Lock()
	ok := s.uploads[id] == r
	s.mu.Unlock()
	if !ok {
		return 0, noUpload(id)
	}
	f, err := os.OpenFile(s.part(id), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	size, err := f.Seek(0, io.SeekEnd)
	if err == nil && first >= 0 && first != size {
		err = core.Errorf(400, "InvalidLayerPartException", "The part starts at byte %d, the upload has %d bytes", first, size)
	}
	if err != nil {
		return size, err
	}
	n, err := io.Copy(f, body)
	return size + n, err
}

// claim ends an upload of r; false when there is none.
func (s *Service) claim(r *Repo, id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	ok := s.uploads[id] == r
	if ok {
		delete(s.uploads, id)
	}
	return ok
}

// complete checks an upload of r against its digest and moves it into the
// blob store.
func (s *Service) complete(r *Repo, id, digest string) error {
	if !s.claim(r, id) {
		return noUpload(id)
	}
	part := s.part(id)
	defer os.Remove(part) // gone once renamed
	f, err := os.Open(part)
	if err != nil {
		return err
	}
	h := sha256.New()
	size, err := io.Copy(h, f)
	f.Close()
	if err != nil {
		return err
	}
	if sum := "sha256:" + hex.EncodeToString(h.Sum(nil)); digest != sum {
		return core.Errorf(400, "InvalidLayerException", "The layer digest %q does not match the uploaded layer, %s", digest, sum)
	}
	if err := os.MkdirAll(filepath.Dir(s.blob(digest)), 0o755); err != nil {
		return err
	}
	if err := os.Rename(part, s.blob(digest)); err != nil {
		return err
	}
	s.mu.Lock()
	r.link(digest, size)
	s.mu.Unlock()
	return nil
}

// expose serves localhost:<ECRPort>, once and in the background so API calls
// never wait for it.
func (s *Service) expose() {
	if s.hub.Cfg.ECRPort <= 0 || !s.exposed.CompareAndSwap(false, true) {
		return
	}
	core.Go("ecr.expose", func() {
		for !s.hub.Ready.Load() { // pogi's own port is known once it serves
			select {
			case <-s.ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
		stop, err := s.open()
		if err != nil {
			s.exposed.Store(false) // the next call tries again
			if s.ctx.Err() == nil {
				s.hub.Log.Warn("ECR registry port unavailable: docker push and pull need it", "port", s.hub.Cfg.ECRPort, "err", err)
			}
			return
		}
		s.mu.Lock()
		s.unexpose = stop
		closed := s.ctx.Err() != nil
		s.mu.Unlock()
		if closed {
			stop()
		}
	})
}

// open serves localhost:<ECRPort> and returns how to stop. The daemons of
// Docker Desktop, OrbStack, Colima and Podman machine pull from their VM's
// localhost, so there a forwarder container publishes the port, which
// reaches this machine's localhost too, and relays to pogi. Elsewhere pogi
// listens on it itself.
func (s *Service) open() (func(), error) {
	port, d := strconv.Itoa(s.hub.Cfg.ECRPort), s.hub.Docker()
	if !d.Available() || d.HostAddr() == "127.0.0.1" {
		ln, err := net.Listen("tcp", net.JoinHostPort(cmp.Or(s.hub.Cfg.Host, "127.0.0.1"), port))
		if err != nil {
			return nil, err
		}
		srv := &http.Server{Handler: s.hub, ReadHeaderTimeout: 30 * time.Second}
		go srv.Serve(ln)
		s.hub.Log.Info("ECR registry", "host", "localhost:"+port)
		return func() {
			srv.Close()
			s.hub.ClosedPort("ecr", "registry", s.hub.Cfg.ECRPort)
		}, nil
	}
	u, err := url.Parse(s.hub.URL)
	if err != nil {
		return nil, err
	}
	name := docker.Name("ecr", port)
	if err := d.RemoveLeftover(s.ctx, name); err != nil { // another pogi's forwarder holds the port
		return nil, err
	}
	id, err := d.Run(s.ctx, &docker.Container{Name: name, Image: "alpine/socat", Service: "ecr", Resource: port, Ports: map[int]int{4510: s.hub.Cfg.ECRPort},
		Cmd: []string{"tcp-listen:4510,fork,reuseaddr", "tcp-connect:" + d.HostAddr() + ":" + u.Port()}})
	if err != nil {
		return nil, err
	}
	s.hub.Log.Info("ECR registry", "host", "localhost:"+port, "container", name)
	return func() { d.Remove(context.Background(), id) }, nil
}

// Close stops serving localhost:<ECRPort> and removes what is temporary:
// uploads in progress, and the blobs when the state lives in memory.
func (s *Service) Close() error {
	s.cancel()
	s.mu.Lock()
	stop := s.unexpose
	s.mu.Unlock()
	if stop != nil {
		stop()
	}
	if s.temp {
		return os.RemoveAll(s.dir)
	}
	return os.RemoveAll(filepath.Join(s.dir, "uploads"))
}
