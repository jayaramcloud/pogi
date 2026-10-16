// Package ecr emulates Amazon ECR: the control plane (JSON 1.1) with
// repositories, images, policies and auth tokens, and the OCI registry that
// docker push and pull use (registry.go). Manifests live in the state, blobs
// in files.
package ecr

import (
	"bytes"
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"maps"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Service is the ECR emulator.
type Service struct {
	hub     *core.Hub
	rpc     *core.RPC
	mu      sync.Mutex
	st      state
	uploads map[string]*Repo // blob uploads in progress: id → repository
	claims  map[string]bool  // name prefixes other clouds' registries serve here, see Claim; kept across Reset

	dir      string      // blob store: blobs/sha256/<hex> and uploads/<id>
	temp     bool        // dir goes on Close: the state lives in memory
	exposed  atomic.Bool // localhost:<ECRPort> is served, or being set up
	unexpose func()      // stops serving it; guarded by mu
	ctx      context.Context
	cancel   context.CancelFunc
}

type state struct {
	Repos map[string]*Repo `json:"repos"`         // account/region/name
	Ext   map[string]*Repo `json:"ext,omitempty"` // repositories under claimed prefixes, by full name
}

// Repo is a repository with its policies, tags and contents.
type Repo struct {
	repository
	Policy    string            `json:"policy,omitempty"`
	Lifecycle string            `json:"lifecycle,omitempty"`
	Tags      map[string]string `json:"tags,omitempty"`
	Images    map[string]*Image `json:"images,omitempty"` // by manifest digest
	Blobs     map[string]int64  `json:"blobs,omitempty"`  // layers and configs pushed here: digest → size
}

// Image is a pushed manifest (or index) and its tags.
type Image struct {
	Manifest  []byte    `json:"manifest"`
	MediaType string    `json:"mediaType"`
	Tags      []string  `json:"tags,omitempty"`
	Size      int64     `json:"size"` // its compressed layers; an index: its largest image
	Pushed    core.Time `json:"pushed"`
	Scanned   bool      `json:"scanned,omitempty"`
}

// repository is the wire shape; repositoryUri is built per request.
type repository struct {
	RepositoryArn              string     `json:"repositoryArn"`
	RegistryId                 string     `json:"registryId"`
	RepositoryName             string     `json:"repositoryName"`
	RepositoryUri              string     `json:"repositoryUri,omitempty"`
	CreatedAt                  core.Time  `json:"createdAt"`
	ImageTagMutability         string     `json:"imageTagMutability"`
	ImageScanningConfiguration scanning   `json:"imageScanningConfiguration"`
	EncryptionConfiguration    encryption `json:"encryptionConfiguration"`
}

type scanning struct {
	ScanOnPush bool `json:"scanOnPush"`
}

type encryption struct {
	EncryptionType string `json:"encryptionType"`
	KmsKey         string `json:"kmsKey,omitempty"`
}

type tag struct{ Key, Value string }

// New creates the ECR service. Blobs go to DataDir/ecr when the state is
// kept on disk, else to a temporary directory.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, claims: map[string]bool{}}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	cfg := hub.Cfg
	s.dir = filepath.Join(cfg.DataDir, "ecr")
	if cmp.Or(cfg.StorageModes["ecr"], cfg.Storage, map[bool]string{true: "hybrid"}[cfg.Persist], "memory") == "memory" { // the hub's storage mode
		if dir, err := os.MkdirTemp("", "pogi-ecr-"); err == nil {
			s.dir, s.temp = dir, true
		}
	}
	s.Reset()
	s.rpc = &core.RPC{JSONVersion: "1.1", Ops: map[string]core.Handler{
		"CreateRepository":              core.H(s.createRepository),
		"DescribeRepositories":          core.H(s.describeRepositories),
		"DeleteRepository":              core.H(s.deleteRepository),
		"ListImages":                    core.H(s.listImages),
		"DescribeImages":                core.H(s.describeImages),
		"BatchGetImage":                 core.H(s.batchGetImage),
		"BatchDeleteImage":              core.H(s.batchDeleteImage),
		"PutImage":                      core.H(s.putImage),
		"GetDownloadUrlForLayer":        core.H(s.getDownloadUrlForLayer),
		"BatchCheckLayerAvailability":   core.H(s.batchCheckLayerAvailability),
		"InitiateLayerUpload":           core.H(s.initiateLayerUpload),
		"UploadLayerPart":               core.H(s.uploadLayerPart),
		"CompleteLayerUpload":           core.H(s.completeLayerUpload),
		"StartImageScan":                core.H(s.startImageScan),
		"DescribeImageScanFindings":     core.H(s.describeImageScanFindings),
		"GetAuthorizationToken":         core.H(s.getAuthorizationToken),
		"PutLifecyclePolicy":            s.doc(true, "put"),
		"GetLifecyclePolicy":            s.doc(true, "get"),
		"DeleteLifecyclePolicy":         s.doc(true, "delete"),
		"SetRepositoryPolicy":           s.doc(false, "put"),
		"GetRepositoryPolicy":           s.doc(false, "get"),
		"DeleteRepositoryPolicy":        s.doc(false, "delete"),
		"PutImageScanningConfiguration": core.H(s.putScanning),
		"PutImageTagMutability":         core.H(s.putTagMutability),
		"TagResource":                   core.H(s.tagResource),
		"UntagResource":                 core.H(s.untagResource),
		"ListTagsForResource":           core.H(s.listTagsForResource),
		"DescribeRegistry":              core.H(s.describeRegistry),
		"GetRegistryPolicy":             core.H(s.getRegistryPolicy),
	}}
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "ecr", Cloud: core.AWS, Title: "ECR", Targets: []string{"AmazonEC2ContainerRegistry_V20150921"}, Signing: []string{"ecr"},
		Match: isRegistry, Protocol: "json"}
}

// BodyLimit lifts the upload limit for image pushes (docker push to /v2/): layers can be large.
func (s *Service) BodyLimit(r *http.Request) int64 {
	if isRegistry(r) {
		return -1
	}
	return 0
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if isRegistry(r) {
		s.registry(w, r)
		return
	}
	s.rpc.ServeHTTP(w, r)
}

func (s *Service) Reset() {
	s.mu.Lock()
	s.st, s.uploads = state{Repos: map[string]*Repo{}, Ext: map[string]*Repo{}}, map[string]*Repo{}
	s.mu.Unlock()
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

// Load restores the state; its repositories get localhost:<ECRPort> served.
func (s *Service) Load(b []byte) error {
	st := state{Repos: map[string]*Repo{}}
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	if st.Ext == nil {
		st.Ext = map[string]*Repo{}
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	if len(st.Repos)+len(st.Ext) > 0 {
		s.expose()
	}
	return nil
}

func invalid(format string, a ...any) error {
	return core.Errorf(400, "InvalidParameterException", format, a...)
}

// host is the registry in repositoryUri and proxyEndpoint: localhost:<ECRPort>
// (ECR's is <account>.dkr.ecr.<region>.amazonaws.com), or without a registry
// port the host the request came to.
func (s *Service) host(c *core.Ctx) string {
	if p := s.hub.Cfg.ECRPort; p > 0 {
		return "localhost:" + strconv.Itoa(p)
	}
	return cmp.Or(c.Host, "localhost:7644")
}

// view is the wire shape with a repositoryUri of "<host>/<name>".
func (r *Repo) view(host string) repository {
	v := r.repository
	v.RepositoryUri = host + "/" + r.RepositoryName
	return v
}

// repo finds a repository of the caller; lock held.
func (s *Service) repo(c *core.Ctx, name string) (*Repo, error) {
	if r := s.st.Repos[c.Account+"/"+c.Region+"/"+name]; r != nil {
		return r, nil
	}
	return nil, core.Errorf(400, "RepositoryNotFoundException", "The repository with name '%s' does not exist in the registry with id '%s'", name, c.Account)
}

// lookup is repo for callers without the lock.
func (s *Service) lookup(c *core.Ctx, name string) (*Repo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.repo(c, name)
}

var nameRe = regexp.MustCompile(`^(?:[a-z0-9]+(?:[._-][a-z0-9]+)*/)*[a-z0-9]+(?:[._-][a-z0-9]+)*$`)

func (s *Service) createRepository(c *core.Ctx, in *struct {
	RepositoryName             string
	Tags                       []tag
	ImageTagMutability         string
	ImageScanningConfiguration scanning
	EncryptionConfiguration    *encryption
}) (any, error) {
	if len(in.RepositoryName) < 2 || len(in.RepositoryName) > 256 || !nameRe.MatchString(in.RepositoryName) {
		return nil, invalid("Invalid parameter at 'repositoryName' failed to satisfy constraint: 'must satisfy regular expression '%s''", nameRe)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.repo(c, in.RepositoryName); err == nil {
		return nil, core.Errorf(400, "RepositoryAlreadyExistsException", "The repository with name '%s' already exists in the registry with id '%s'", in.RepositoryName, c.Account)
	}
	tags := map[string]string{}
	for _, t := range in.Tags {
		tags[t.Key] = t.Value
	}
	r := &Repo{repository: repository{RepositoryArn: c.ARN("ecr", "repository/"+in.RepositoryName), RegistryId: c.Account,
		RepositoryName: in.RepositoryName, CreatedAt: core.Time(time.Now().UTC()), ImageTagMutability: cmp.Or(in.ImageTagMutability, "MUTABLE"),
		ImageScanningConfiguration: in.ImageScanningConfiguration, EncryptionConfiguration: *cmp.Or(in.EncryptionConfiguration, &encryption{EncryptionType: "AES256"})},
		Tags: tags}
	s.st.Repos[c.Account+"/"+c.Region+"/"+in.RepositoryName] = r
	s.expose()
	return map[string]repository{"repository": r.view(s.host(c))}, nil
}

func (s *Service) describeRepositories(c *core.Ctx, in *struct {
	RepositoryNames []string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []repository{}
	for _, n := range in.RepositoryNames {
		r, err := s.repo(c, n)
		if err != nil {
			return nil, err
		}
		out = append(out, r.view(s.host(c)))
	}
	if len(in.RepositoryNames) == 0 {
		for _, k := range core.SortedKeys(s.st.Repos) {
			if strings.HasPrefix(k, c.Account+"/"+c.Region+"/") {
				out = append(out, s.st.Repos[k].view(s.host(c)))
			}
		}
	}
	return map[string][]repository{"repositories": out}, nil
}

func (s *Service) deleteRepository(c *core.Ctx, in *struct {
	RepositoryName string
	Force          bool
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := s.repo(c, in.RepositoryName)
	if err != nil {
		return nil, err
	}
	if len(r.Images) > 0 && !in.Force {
		return nil, core.Errorf(400, "RepositoryNotEmptyException", "The repository with name '%s' in registry with id '%s' cannot be deleted because it still contains images",
			in.RepositoryName, c.Account)
	}
	delete(s.st.Repos, c.Account+"/"+c.Region+"/"+in.RepositoryName)
	return map[string]repository{"repository": r.view(s.host(c))}, nil
}

type imageID struct {
	ImageDigest string `json:"imageDigest,omitempty"`
	ImageTag    string `json:"imageTag,omitempty"`
}

type imagesInput struct {
	RepositoryName string
	ImageIds       []imageID
	Filter         struct{ TagStatus string }
}

// image finds an image by digest and/or tag; lock held.
func (r *Repo) image(id imageID) (string, *Image) {
	if id == (imageID{}) {
		return "", nil
	}
	for d, img := range r.Images {
		if (id.ImageDigest == "" || d == id.ImageDigest) && (id.ImageTag == "" || slices.Contains(img.Tags, id.ImageTag)) {
			return d, img
		}
	}
	return "", nil
}

// drop deletes an image by digest, or one tag and with the last tag the
// image, as BatchDeleteImage does; lock held.
func (r *Repo) drop(id imageID) (imageID, bool) {
	d, img := r.image(id)
	if img == nil {
		return id, false
	}
	img.Tags = slices.DeleteFunc(img.Tags, func(t string) bool { return t == id.ImageTag })
	if id.ImageTag == "" || len(img.Tags) == 0 {
		delete(r.Images, d)
	}
	return imageID{d, id.ImageTag}, true
}

// is applies a tagStatus filter: TAGGED, UNTAGGED or ANY.
func (img *Image) is(status string) bool {
	switch status {
	case "TAGGED":
		return len(img.Tags) > 0
	case "UNTAGGED":
		return len(img.Tags) == 0
	}
	return true
}

var scanned = map[string]string{"status": "COMPLETE", "description": "The scan was completed successfully."}

// detail is an image's imageDetails entry; lock held.
func (r *Repo) detail(d string, img *Image) map[string]any {
	out := map[string]any{"registryId": r.RegistryId, "repositoryName": r.RepositoryName, "imageDigest": d, "imageSizeInBytes": img.Size,
		"imagePushedAt": img.Pushed, "imageManifestMediaType": img.MediaType}
	if len(img.Tags) > 0 {
		out["imageTags"] = slices.Clone(img.Tags)
	}
	if img.Scanned {
		out["imageScanStatus"] = scanned
		out["imageScanFindingsSummary"] = map[string]any{"imageScanCompletedAt": img.Pushed, "vulnerabilitySourceUpdatedAt": img.Pushed,
			"findingSeverityCounts": map[string]int{}}
	}
	return out
}

func notFound(c *core.Ctx, repo string, id imageID) error {
	return core.Errorf(400, "ImageNotFoundException", "The image with imageId {imageDigest:'%s', imageTag:'%s'} does not exist within the repository with name '%s' in the registry with id '%s'",
		cmp.Or(id.ImageDigest, "null"), cmp.Or(id.ImageTag, "null"), repo, c.Account)
}

func failure(id imageID) map[string]any {
	return map[string]any{"imageId": id, "failureCode": "ImageNotFound", "failureReason": "Requested image not found"}
}

func (s *Service) listImages(c *core.Ctx, in *imagesInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := s.repo(c, in.RepositoryName)
	if err != nil {
		return nil, err
	}
	ids := []imageID{}
	for _, d := range core.SortedKeys(r.Images) {
		img := r.Images[d]
		if !img.is(in.Filter.TagStatus) {
			continue
		}
		for _, t := range img.Tags {
			ids = append(ids, imageID{d, t})
		}
		if len(img.Tags) == 0 {
			ids = append(ids, imageID{ImageDigest: d})
		}
	}
	return map[string][]imageID{"imageIds": ids}, nil
}

func (s *Service) describeImages(c *core.Ctx, in *imagesInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := s.repo(c, in.RepositoryName)
	if err != nil {
		return nil, err
	}
	out := []map[string]any{}
	for _, id := range in.ImageIds {
		d, img := r.image(id)
		if img == nil {
			return nil, notFound(c, in.RepositoryName, id)
		}
		out = append(out, r.detail(d, img))
	}
	if len(in.ImageIds) == 0 {
		for _, d := range core.SortedKeys(r.Images) {
			if img := r.Images[d]; img.is(in.Filter.TagStatus) {
				out = append(out, r.detail(d, img))
			}
		}
	}
	return map[string]any{"imageDetails": out}, nil
}

func (s *Service) batchGetImage(c *core.Ctx, in *imagesInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := s.repo(c, in.RepositoryName)
	if err != nil {
		return nil, err
	}
	images, failures := []map[string]any{}, []map[string]any{}
	for _, id := range in.ImageIds {
		d, img := r.image(id)
		if img == nil {
			failures = append(failures, failure(id))
			continue
		}
		images = append(images, map[string]any{"registryId": r.RegistryId, "repositoryName": r.RepositoryName, "imageId": imageID{d, id.ImageTag},
			"imageManifest": string(img.Manifest), "imageManifestMediaType": img.MediaType})
	}
	return map[string]any{"images": images, "failures": failures}, nil
}

func (s *Service) batchDeleteImage(c *core.Ctx, in *imagesInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := s.repo(c, in.RepositoryName)
	if err != nil {
		return nil, err
	}
	ids, failures := []imageID{}, []map[string]any{}
	for _, id := range in.ImageIds {
		if gone, ok := r.drop(id); ok {
			ids = append(ids, gone)
		} else {
			failures = append(failures, failure(id))
		}
	}
	return map[string]any{"imageIds": ids, "failures": failures}, nil
}

func (s *Service) putImage(c *core.Ctx, in *struct {
	RepositoryName, ImageManifest, ImageManifestMediaType, ImageTag, ImageDigest string
}) (any, error) {
	r, err := s.lookup(c, in.RepositoryName)
	if err != nil {
		return nil, err
	}
	d, mt, same, err := s.put(r, []byte(in.ImageManifest), in.ImageManifestMediaType, in.ImageTag, in.ImageDigest)
	if err != nil {
		return nil, err
	}
	if same {
		return nil, core.Errorf(400, "ImageAlreadyExistsException", "Image with digest '%s' and tag '%s' already exists in the repository with name '%s' in registry with id '%s'",
			d, in.ImageTag, r.RepositoryName, r.RegistryId)
	}
	return map[string]any{"image": map[string]any{"registryId": r.RegistryId, "repositoryName": r.RepositoryName, "imageId": imageID{d, in.ImageTag},
		"imageManifest": in.ImageManifest, "imageManifestMediaType": mt}}, nil
}

// getDownloadUrlForLayer points at the registry on the host the caller used.
func (s *Service) getDownloadUrlForLayer(c *core.Ctx, in *struct{ RepositoryName, LayerDigest string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := s.repo(c, in.RepositoryName)
	if err != nil {
		return nil, err
	}
	if _, ok := r.Blobs[in.LayerDigest]; !ok {
		return nil, core.Errorf(400, "LayersNotFoundException", "The image layer '%s' does not exist in the repository with name '%s' in the registry with id '%s'",
			in.LayerDigest, r.RepositoryName, r.RegistryId)
	}
	return map[string]string{"downloadUrl": "http://" + cmp.Or(c.Host, "localhost:7644") + "/v2/" + r.RepositoryName + "/blobs/" + in.LayerDigest,
		"layerDigest": in.LayerDigest}, nil
}

func (s *Service) batchCheckLayerAvailability(c *core.Ctx, in *struct {
	RepositoryName string
	LayerDigests   []string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := s.repo(c, in.RepositoryName)
	if err != nil {
		return nil, err
	}
	layers, failures := []map[string]any{}, []map[string]any{}
	for _, d := range in.LayerDigests {
		if size, ok := r.Blobs[d]; ok {
			layers = append(layers, map[string]any{"layerDigest": d, "layerAvailability": "AVAILABLE", "layerSize": size})
		} else {
			failures = append(failures, map[string]any{"layerDigest": d, "failureCode": "MissingLayerDigest", "failureReason": "Could not find layer"})
		}
	}
	return map[string]any{"layers": layers, "failures": failures}, nil
}

func (s *Service) initiateLayerUpload(c *core.Ctx, in *struct{ RepositoryName string }) (any, error) {
	r, err := s.lookup(c, in.RepositoryName)
	if err != nil {
		return nil, err
	}
	id, err := s.initiate(r)
	if err != nil {
		return nil, err
	}
	return map[string]any{"uploadId": id, "partSize": 10 << 20}, nil
}

func (s *Service) uploadLayerPart(c *core.Ctx, in *struct {
	RepositoryName, UploadId    string
	PartFirstByte, PartLastByte int64
	LayerPartBlob               []byte
}) (any, error) {
	r, err := s.lookup(c, in.RepositoryName)
	if err != nil {
		return nil, err
	}
	size, err := s.write(r, in.UploadId, in.PartFirstByte, bytes.NewReader(in.LayerPartBlob))
	if err != nil {
		return nil, err
	}
	return map[string]any{"registryId": r.RegistryId, "repositoryName": r.RepositoryName, "uploadId": in.UploadId, "lastByteReceived": size - 1}, nil
}

func (s *Service) completeLayerUpload(c *core.Ctx, in *struct {
	RepositoryName, UploadId string
	LayerDigests             []string
}) (any, error) {
	r, err := s.lookup(c, in.RepositoryName)
	if err != nil {
		return nil, err
	}
	d := strings.Join(in.LayerDigests, ",") // ECR takes one: more never match
	if err := s.complete(r, in.UploadId, d); err != nil {
		return nil, err
	}
	return map[string]any{"registryId": r.RegistryId, "repositoryName": r.RepositoryName, "uploadId": in.UploadId, "layerDigest": d}, nil
}

type scanInput struct {
	RepositoryName string
	ImageId        imageID
}

// scan finds the image of a scan call; lock held.
func (s *Service) scan(c *core.Ctx, in *scanInput) (*Repo, string, *Image, error) {
	r, err := s.repo(c, in.RepositoryName)
	if err != nil {
		return nil, "", nil, err
	}
	d, img := r.image(in.ImageId)
	if img == nil {
		return nil, "", nil, notFound(c, in.RepositoryName, in.ImageId)
	}
	return r, d, img, nil
}

// startImageScan completes at once and finds nothing.
func (s *Service) startImageScan(c *core.Ctx, in *scanInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, d, img, err := s.scan(c, in)
	if err != nil {
		return nil, err
	}
	img.Scanned = true
	return map[string]any{"registryId": r.RegistryId, "repositoryName": r.RepositoryName, "imageId": imageID{d, in.ImageId.ImageTag}, "imageScanStatus": scanned}, nil
}

func (s *Service) describeImageScanFindings(c *core.Ctx, in *scanInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, d, img, err := s.scan(c, in)
	if err != nil {
		return nil, err
	}
	if !img.Scanned {
		return nil, core.Errorf(400, "ScanNotFoundException", "Image scan does not exist for the image with '{imageDigest:'%s', imageTag:'%s'}' in the repository with name '%s' in the registry with id '%s'",
			d, cmp.Or(in.ImageId.ImageTag, "null"), r.RepositoryName, r.RegistryId)
	}
	return map[string]any{"registryId": r.RegistryId, "repositoryName": r.RepositoryName, "imageId": imageID{d, in.ImageId.ImageTag}, "imageScanStatus": scanned,
		"imageScanFindings": map[string]any{"imageScanCompletedAt": img.Pushed, "vulnerabilitySourceUpdatedAt": img.Pushed,
			"findingSeverityCounts": map[string]int{}, "findings": []any{}}}, nil
}

// getAuthorizationToken returns a password for docker login; the registry
// takes any.
func (s *Service) getAuthorizationToken(c *core.Ctx, _ *struct{}) (any, error) {
	s.expose()
	return map[string]any{"authorizationData": []map[string]any{{
		"authorizationToken": base64.StdEncoding.EncodeToString([]byte("AWS:password")),
		"expiresAt":          core.Time(time.Now().UTC().Add(12 * time.Hour)),
		"proxyEndpoint":      "http://" + s.host(c),
	}}}, nil
}

// doc serves put/get/delete of a repository's policy or lifecycle policy.
func (s *Service) doc(lifecycle bool, op string) core.Handler {
	return core.H(func(c *core.Ctx, in *struct{ RepositoryName, PolicyText, LifecyclePolicyText string }) (any, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		r, err := s.repo(c, in.RepositoryName)
		if err != nil {
			return nil, err
		}
		field, key, code, what := &r.Policy, "policyText", "RepositoryPolicyNotFoundException", "Repository policy"
		if lifecycle {
			field, key, code, what = &r.Lifecycle, "lifecyclePolicyText", "LifecyclePolicyNotFoundException", "Lifecycle policy"
		}
		switch text := in.PolicyText + in.LifecyclePolicyText; {
		case op == "put" && !json.Valid([]byte(text)):
			return nil, invalid("Invalid parameter at '%s' failed to satisfy constraint: 'must be valid JSON'", key)
		case op == "put":
			*field = text
			if lifecycle {
				r.expire(text)
			}
		case *field == "":
			return nil, core.Errorf(400, code, "%s does not exist for the repository with name '%s' in the registry with id '%s'", what, r.RepositoryName, r.RegistryId)
		}
		out := map[string]string{"registryId": r.RegistryId, "repositoryName": r.RepositoryName, key: *field}
		if op == "delete" {
			*field = ""
		}
		return out, nil
	})
}

// rule is a lifecycle policy rule.
type rule struct {
	RulePriority int
	Selection    struct {
		TagStatus                     string // tagged, untagged or any
		TagPrefixList, TagPatternList []string
		CountType                     string // imageCountMoreThan, or sinceImagePushed in days
		CountNumber                   int
	}
}

// expire applies a lifecycle policy at once, where ECR takes up to a day: by
// priority, each rule expires, of the images no earlier rule selected, those
// past the newest countNumber or pushed more than countNumber days ago; lock held.
func (r *Repo) expire(policy string) {
	var p struct{ Rules []rule }
	json.Unmarshal([]byte(policy), &p)
	slices.SortFunc(p.Rules, func(a, b rule) int { return a.RulePriority - b.RulePriority })
	taken := map[string]bool{}
	for _, ru := range p.Rules {
		sel := ru.Selection
		var ds []string
		for d, img := range r.Images {
			if !taken[d] && img.is(strings.ToUpper(sel.TagStatus)) && (sel.TagStatus != "tagged" || matches(img.Tags, sel.TagPrefixList, sel.TagPatternList)) {
				ds = append(ds, d)
			}
		}
		slices.SortFunc(ds, func(a, b string) int { return time.Time(r.Images[b].Pushed).Compare(time.Time(r.Images[a].Pushed)) }) // newest first
		for i, d := range ds {
			taken[d] = true
			old := time.Since(time.Time(r.Images[d].Pushed)) > time.Duration(sel.CountNumber)*24*time.Hour
			if sel.CountType == "imageCountMoreThan" && i >= sel.CountNumber || sel.CountType == "sinceImagePushed" && old {
				delete(r.Images, d)
			}
		}
	}
}

// matches: each prefix and each pattern (* wildcards) matches one of tags.
func matches(tags, prefixes, patterns []string) bool {
	for _, p := range prefixes {
		if !slices.ContainsFunc(tags, func(t string) bool { return strings.HasPrefix(t, p) }) {
			return false
		}
	}
	for _, p := range patterns {
		if !slices.ContainsFunc(tags, func(t string) bool { ok, _ := path.Match(p, t); return ok }) {
			return false
		}
	}
	return true
}

func (s *Service) putScanning(c *core.Ctx, in *struct {
	RepositoryName             string
	ImageScanningConfiguration scanning
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := s.repo(c, in.RepositoryName)
	if err != nil {
		return nil, err
	}
	r.ImageScanningConfiguration = in.ImageScanningConfiguration
	return map[string]any{"registryId": r.RegistryId, "repositoryName": r.RepositoryName, "imageScanningConfiguration": r.ImageScanningConfiguration}, nil
}

func (s *Service) putTagMutability(c *core.Ctx, in *struct{ RepositoryName, ImageTagMutability string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := s.repo(c, in.RepositoryName)
	if err != nil {
		return nil, err
	}
	r.ImageTagMutability = in.ImageTagMutability
	return map[string]string{"registryId": r.RegistryId, "repositoryName": r.RepositoryName, "imageTagMutability": r.ImageTagMutability}, nil
}

// byARN finds a repository from its ARN; lock held.
func (s *Service) byARN(arn string) (*Repo, error) {
	svc, region, account, res, ok := core.ParseARN(arn)
	if !ok || svc != "ecr" || !strings.HasPrefix(res, "repository/") {
		return nil, invalid("Invalid parameter at 'resourceArn' failed to satisfy constraint: 'Invalid ARN'")
	}
	if r := s.st.Repos[account+"/"+region+"/"+strings.TrimPrefix(res, "repository/")]; r != nil {
		return r, nil
	}
	return nil, core.Errorf(400, "RepositoryNotFoundException", "The repository with name '%s' does not exist in the registry with id '%s'", strings.TrimPrefix(res, "repository/"), account)
}

func (s *Service) tagResource(c *core.Ctx, in *struct {
	ResourceArn string
	Tags        []tag
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := s.byARN(in.ResourceArn)
	if err != nil {
		return nil, err
	}
	if r.Tags == nil {
		r.Tags = map[string]string{}
	}
	for _, t := range in.Tags {
		r.Tags[t.Key] = t.Value
	}
	return nil, nil
}

func (s *Service) untagResource(c *core.Ctx, in *struct {
	ResourceArn string
	TagKeys     []string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := s.byARN(in.ResourceArn)
	if err != nil {
		return nil, err
	}
	for _, k := range in.TagKeys {
		delete(r.Tags, k)
	}
	return nil, nil
}

func (s *Service) listTagsForResource(c *core.Ctx, in *struct{ ResourceArn string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := s.byARN(in.ResourceArn)
	if err != nil {
		return nil, err
	}
	tags := []tag{}
	for _, k := range core.SortedKeys(r.Tags) {
		tags = append(tags, tag{k, r.Tags[k]})
	}
	return map[string][]tag{"tags": tags}, nil
}

func (s *Service) describeRegistry(c *core.Ctx, _ *struct{}) (any, error) {
	return map[string]any{"registryId": c.Account, "replicationConfiguration": map[string][]any{"rules": {}}}, nil
}

func (s *Service) getRegistryPolicy(c *core.Ctx, _ *struct{}) (any, error) {
	return nil, core.Errorf(400, "RegistryPolicyNotFoundException", "Registry policy does not exist in the registry with id '%s'", c.Account)
}

// TaggedResources lists repositories (ARN → tags) for the tagging API and Resource Explorer.
func (s *Service) TaggedResources(account, region string) map[string]map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]map[string]string{}
	for k, r := range s.st.Repos {
		if strings.HasPrefix(k, account+"/"+region+"/") {
			out[r.RepositoryArn] = maps.Clone(r.Tags)
		}
	}
	return out
}

// Resources lists repositories.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := core.Table{Name: "Repositories", Columns: []string{"Repository", "Region", "Images", "Tag mutability", "Created"}}
	for _, k := range core.SortedKeys(s.st.Repos) {
		r := s.st.Repos[k]
		t.Rows = append(t.Rows, []string{r.RepositoryName, strings.Split(k, "/")[1], strconv.Itoa(len(r.Images)), r.ImageTagMutability,
			time.Time(r.CreatedAt).Format(time.RFC3339)})
	}
	return []core.Table{t}
}

// Inspect returns a repository with its policies, tags and images.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range core.SortedKeys(s.st.Repos) {
		if r := s.st.Repos[k]; r.RepositoryName == id {
			images := []map[string]any{}
			for _, d := range core.SortedKeys(r.Images) {
				images = append(images, r.detail(d, r.Images[d]))
			}
			return map[string]any{"repository": r.repository, "policy": r.Policy, "lifecyclePolicy": r.Lifecycle, "tags": maps.Clone(r.Tags), "images": images}
		}
	}
	return nil
}
