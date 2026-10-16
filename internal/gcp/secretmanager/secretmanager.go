// Package secretmanager emulates Google Cloud Secret Manager over gRPC and its
// REST mirror: secrets (replication, labels, annotations, expiry, version
// aliases, etags), versions (CRC32C-checked payloads; enable, disable,
// destroy) and IAM policies. Regional names (projects/p/locations/l/…) are
// aliases of the global ones.
package secretmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/iam/apiv1/iampb"
	smpb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	rpccode "google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Service is the Secret Manager emulator.
type Service struct {
	smpb.UnimplementedSecretManagerServiceServer
	mu      sync.Mutex
	secrets map[string]*secret
}

type secret struct {
	p        *smpb.Secret
	versions []*version // versions[n-1] is version n
	policy   *iampb.Policy
}

type version struct {
	p    *smpb.SecretVersion
	data []byte // nil once destroyed
}

// New creates the Secret Manager service.
func New(*core.Hub) *Service { return &Service{secrets: map[string]*secret{}} }

func (s *Service) Info() core.Info {
	return core.Info{Name: "secretmanager", Cloud: core.GCP, Title: "Secret Manager", Match: match,
		Targets: []string{"google.cloud.secretmanager.v1.SecretManagerService"}}
}

func (s *Service) RegisterGRPC(g *grpc.Server) { smpb.RegisterSecretManagerServiceServer(g, s) }

func (s *Service) Reset() {
	s.mu.Lock()
	s.secrets = map[string]*secret{}
	s.mu.Unlock()
}

// ---- persistence ----

type savedSecret struct {
	Secret   json.RawMessage   `json:"secret"`
	Policy   json.RawMessage   `json:"policy,omitempty"`
	Versions []json.RawMessage `json:"versions,omitempty"`
	Data     [][]byte          `json:"data,omitempty"` // payload of each version
}

func pj(m proto.Message) json.RawMessage {
	b, _ := protojson.Marshal(m)
	return b
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []savedSecret{}
	for _, n := range core.SortedKeys(s.secrets) {
		sc := s.secrets[n]
		ss := savedSecret{Secret: pj(sc.p)}
		if sc.policy != nil {
			ss.Policy = pj(sc.policy)
		}
		for _, v := range sc.versions {
			ss.Versions, ss.Data = append(ss.Versions, pj(v.p)), append(ss.Data, v.data)
		}
		out = append(out, ss)
	}
	return json.Marshal(out)
}

func (s *Service) Load(b []byte) error {
	var in []savedSecret
	if err := json.Unmarshal(b, &in); err != nil {
		return err
	}
	var err error
	dec := func(raw json.RawMessage, m proto.Message) {
		if err == nil {
			err = protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(raw, m)
		}
	}
	secrets := map[string]*secret{}
	for _, ss := range in {
		sc := &secret{p: &smpb.Secret{}}
		dec(ss.Secret, sc.p)
		if len(ss.Policy) > 0 {
			sc.policy = &iampb.Policy{}
			dec(ss.Policy, sc.policy)
		}
		for i, raw := range ss.Versions {
			v := &version{p: &smpb.SecretVersion{}}
			dec(raw, v.p)
			if i < len(ss.Data) {
				v.data = ss.Data[i]
			}
			sc.versions = append(sc.versions, v)
		}
		secrets[sc.p.Name] = sc
	}
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.secrets = secrets
	s.mu.Unlock()
	return nil
}

// ---- names and helpers ----

var (
	secretRE  = regexp.MustCompile(`^projects/[^/]+/secrets/[A-Za-z0-9_-]{1,255}$`)
	projectRE = regexp.MustCompile(`^projects/[^/]+$`)
	locRE     = regexp.MustCompile(`^(projects/[^/]+)/locations/[^/]+`)
	crcTable  = crc32.MakeTable(crc32.Castagnoli)
)

// norm maps regional names (projects/p/locations/l/…) to global ones.
func norm(name string) string { return locRE.ReplaceAllString(name, "$1") }

func invalid(format string, a ...any) error {
	return status.Errorf(codes.InvalidArgument, format, a...)
}

func etag() string { return `"` + core.RandHex(7) + `"` }

func checkEtag(got, want string) error {
	if got != "" && got != want {
		return status.Errorf(codes.Aborted, "The etag %s does not match the current etag %s", got, want)
	}
	return nil
}

func clone[M proto.Message](m M) M { return proto.Clone(m).(M) }

func expired(p *smpb.Secret) bool {
	t := p.GetExpireTime()
	return t != nil && t.AsTime().Before(time.Now())
}

// secret finds a live secret; expired ones are deleted on the way. Lock held.
func (s *Service) secret(name string) (*secret, error) {
	name = norm(name)
	sc := s.secrets[name]
	if sc != nil && expired(sc.p) {
		delete(s.secrets, name)
		sc = nil
	}
	if sc == nil {
		return nil, status.Errorf(codes.NotFound, "Secret [%s] not found.", name)
	}
	return sc, nil
}

// version resolves projects/p/secrets/s/versions/{n|latest|alias}. Lock held.
func (s *Service) version(name string) (*secret, *version, error) {
	parent, id, ok := strings.Cut(norm(name), "/versions/")
	if !ok {
		return nil, nil, invalid("Invalid secret version name: %s", name)
	}
	sc, err := s.secret(parent)
	if err != nil {
		return nil, nil, err
	}
	n, err := strconv.Atoi(id)
	if err != nil {
		n = int(sc.p.VersionAliases[id]) // 0 when unknown
	}
	if id == "latest" {
		n = len(sc.versions)
	}
	if n < 1 || n > len(sc.versions) {
		return nil, nil, status.Errorf(codes.NotFound, "Secret Version [%s] not found.", norm(name))
	}
	return sc, sc.versions[n-1], nil
}

// expiration turns an input-only ttl into expire_time.
func expiration(p *smpb.Secret) {
	if ttl := p.GetTtl(); ttl != nil {
		p.Expiration = &smpb.Secret_ExpireTime{ExpireTime: timestamppb.New(time.Now().Add(ttl.AsDuration()))}
	}
}

func checkAliases(p *smpb.Secret, vs []*version) error {
	for alias, n := range p.VersionAliases {
		if n < 1 || int(n) > len(vs) || vs[n-1].p.State == smpb.SecretVersion_DESTROYED {
			return invalid("Version alias %q points to version %d, which does not exist or is destroyed", alias, n)
		}
	}
	return nil
}

// page cuts one page from a sorted list; the token is the offset of the next page.
func page[T any](items []T, size int32, token string) ([]T, string) {
	start, _ := strconv.Atoi(token)
	start = min(max(start, 0), len(items))
	end := len(items)
	if size > 0 {
		end = min(end, start+int(size))
	}
	if end < len(items) {
		return items[start:end], strconv.Itoa(end)
	}
	return items[start:end], ""
}

// update copies the fields named by an update mask from src to dst.
func update(dst, src proto.Message, paths []string, allowed ...string) error {
	if len(paths) == 0 {
		return invalid("The update_mask in the request must be set")
	}
	src = proto.Clone(src)
	for _, path := range paths {
		parts := strings.Split(path, ".")
		if !slices.Contains(allowed, parts[0]) {
			return invalid("Invalid update_mask provided in the request: %q", path)
		}
		dm, sm := dst.ProtoReflect(), src.ProtoReflect()
		for i, name := range parts {
			fd := dm.Descriptor().Fields().ByName(protoreflect.Name(name))
			last := i == len(parts)-1
			switch {
			case fd == nil || !last && (fd.Message() == nil || fd.IsList() || fd.IsMap()):
				return invalid("Invalid update_mask provided in the request: %q", path)
			case !last:
				dm, sm = dm.Mutable(fd).Message(), sm.Get(fd).Message()
			case sm.Has(fd):
				dm.Set(fd, sm.Get(fd))
			default:
				dm.Clear(fd)
			}
		}
	}
	return nil
}

// filter supports the common list filters, joined by spaces or AND:
// name:SUBSTR, name:PREFIX*, labels.KEY=VALUE, labels.KEY:PREFIX* and labels.KEY:* (has the label).
func filter(f string) (func(*smpb.Secret) bool, error) {
	type term struct{ field, op, val string }
	var terms []term
	for _, t := range strings.Fields(f) {
		i := strings.IndexAny(t, ":=")
		switch {
		case t == "AND":
			continue
		case i <= 0 || t[:i] != "name" && !strings.HasPrefix(t[:i], "labels."):
			return nil, invalid("Unsupported filter %q (pogi supports name and labels terms)", f)
		}
		terms = append(terms, term{t[:i], t[i : i+1], strings.Trim(t[i+1:], `"`)})
	}
	return func(p *smpb.Secret) bool {
		for _, t := range terms {
			got, ok := p.Name[strings.LastIndex(p.Name, "/")+1:], true
			if t.field != "name" {
				got, ok = p.Labels[strings.TrimPrefix(t.field, "labels.")]
			}
			prefix, wild := strings.CutSuffix(t.val, "*")
			if !ok || t.op == "=" && got != t.val || t.op == ":" && (wild && !strings.HasPrefix(got, prefix) || !wild && !strings.Contains(got, t.val)) {
				return false
			}
		}
		return true
	}, nil
}

// ---- secrets ----

func (s *Service) CreateSecret(_ context.Context, req *smpb.CreateSecretRequest) (*smpb.Secret, error) {
	parent := norm(req.Parent)
	name := parent + "/secrets/" + req.SecretId
	if !projectRE.MatchString(parent) || !secretRE.MatchString(name) {
		return nil, invalid("Invalid secret name %q: the ID must be 1-255 letters, digits, - or _", name)
	}
	p := req.GetSecret()
	if p.GetReplication().GetReplication() == nil {
		return nil, invalid("Secret.replication must be set")
	}
	if um := p.Replication.GetUserManaged(); um != nil && len(um.Replicas) == 0 {
		return nil, invalid("Secret.replication.user_managed needs at least one replica")
	}
	p.Name, p.CreateTime, p.Etag = name, timestamppb.Now(), etag()
	expiration(p)
	if err := checkAliases(p, nil); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.secret(name); err == nil {
		return nil, status.Errorf(codes.AlreadyExists, "Secret [%s] already exists.", name)
	}
	s.secrets[name] = &secret{p: clone(p)}
	return p, nil
}

func (s *Service) GetSecret(_ context.Context, req *smpb.GetSecretRequest) (*smpb.Secret, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sc, err := s.secret(req.Name)
	if err != nil {
		return nil, err
	}
	return clone(sc.p), nil
}

func (s *Service) UpdateSecret(_ context.Context, req *smpb.UpdateSecretRequest) (*smpb.Secret, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sc, err := s.secret(req.GetSecret().GetName())
	if err != nil {
		return nil, err
	}
	if err := checkEtag(req.Secret.Etag, sc.p.Etag); err != nil {
		return nil, err
	}
	p := clone(sc.p)
	if err := update(p, req.Secret, req.GetUpdateMask().GetPaths(), "labels", "annotations", "version_aliases",
		"expire_time", "ttl", "topics", "rotation", "version_destroy_ttl", "tags"); err != nil {
		return nil, err
	}
	expiration(p)
	if err := checkAliases(p, sc.versions); err != nil {
		return nil, err
	}
	p.Etag = etag()
	sc.p = p
	return clone(p), nil
}

func (s *Service) ListSecrets(_ context.Context, req *smpb.ListSecretsRequest) (*smpb.ListSecretsResponse, error) {
	parent := norm(req.Parent)
	if !projectRE.MatchString(parent) {
		return nil, invalid("Invalid parent %q", req.Parent)
	}
	keep, err := filter(req.Filter)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var list []*smpb.Secret
	for _, n := range core.SortedKeys(s.secrets) {
		if p := s.secrets[n].p; strings.HasPrefix(n, parent+"/") && !expired(p) && keep(p) {
			list = append(list, p)
		}
	}
	items, next := page(list, req.PageSize, req.PageToken)
	out := &smpb.ListSecretsResponse{NextPageToken: next, TotalSize: int32(len(list))}
	for _, p := range items {
		out.Secrets = append(out.Secrets, clone(p))
	}
	return out, nil
}

func (s *Service) DeleteSecret(_ context.Context, req *smpb.DeleteSecretRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sc, err := s.secret(req.Name)
	if err != nil {
		return nil, err
	}
	if err := checkEtag(req.Etag, sc.p.Etag); err != nil {
		return nil, err
	}
	delete(s.secrets, sc.p.Name)
	return &emptypb.Empty{}, nil
}

// ---- versions ----

func (s *Service) AddSecretVersion(_ context.Context, req *smpb.AddSecretVersionRequest) (*smpb.SecretVersion, error) {
	pl := req.GetPayload()
	switch {
	case pl == nil:
		return nil, invalid("The payload is required")
	case len(pl.Data) > 64<<10:
		return nil, invalid("Secret payload must be at most 65536 bytes")
	case pl.DataCrc32C != nil && *pl.DataCrc32C != int64(crc32.Checksum(pl.Data, crcTable)):
		return nil, invalid("Checksum mismatch: data_crc32c does not match the payload")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sc, err := s.secret(req.Parent)
	if err != nil {
		return nil, err
	}
	v := &smpb.SecretVersion{Name: fmt.Sprintf("%s/versions/%d", sc.p.Name, len(sc.versions)+1), CreateTime: timestamppb.Now(),
		State: smpb.SecretVersion_ENABLED, Etag: etag(), ClientSpecifiedPayloadChecksum: pl.DataCrc32C != nil}
	sc.versions = append(sc.versions, &version{p: v, data: pl.Data})
	return clone(v), nil
}

func (s *Service) GetSecretVersion(_ context.Context, req *smpb.GetSecretVersionRequest) (*smpb.SecretVersion, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, v, err := s.version(req.Name)
	if err != nil {
		return nil, err
	}
	return clone(v.p), nil
}

// AccessSecretVersion returns the payload of an enabled version ("latest" is the newest version).
func (s *Service) AccessSecretVersion(_ context.Context, req *smpb.AccessSecretVersionRequest) (*smpb.AccessSecretVersionResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, v, err := s.version(req.Name)
	if err != nil {
		return nil, err
	}
	if v.p.State != smpb.SecretVersion_ENABLED {
		return nil, status.Errorf(codes.FailedPrecondition, "Secret Version [%s] is in %s state.", v.p.Name, v.p.State)
	}
	crc := int64(crc32.Checksum(v.data, crcTable))
	return &smpb.AccessSecretVersionResponse{Name: v.p.Name, Payload: &smpb.SecretPayload{Data: v.data, DataCrc32C: &crc}}, nil
}

// ListSecretVersions lists versions newest first.
func (s *Service) ListSecretVersions(_ context.Context, req *smpb.ListSecretVersionsRequest) (*smpb.ListSecretVersionsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sc, err := s.secret(req.Parent)
	if err != nil {
		return nil, err
	}
	var list []*smpb.SecretVersion
	for _, v := range slices.Backward(sc.versions) {
		list = append(list, v.p)
	}
	items, next := page(list, req.PageSize, req.PageToken)
	out := &smpb.ListSecretVersionsResponse{NextPageToken: next, TotalSize: int32(len(list))}
	for _, p := range items {
		out.Versions = append(out.Versions, clone(p))
	}
	return out, nil
}

func (s *Service) EnableSecretVersion(_ context.Context, req *smpb.EnableSecretVersionRequest) (*smpb.SecretVersion, error) {
	return s.setState(req.Name, req.Etag, smpb.SecretVersion_ENABLED)
}

func (s *Service) DisableSecretVersion(_ context.Context, req *smpb.DisableSecretVersionRequest) (*smpb.SecretVersion, error) {
	return s.setState(req.Name, req.Etag, smpb.SecretVersion_DISABLED)
}

func (s *Service) DestroySecretVersion(_ context.Context, req *smpb.DestroySecretVersionRequest) (*smpb.SecretVersion, error) {
	return s.setState(req.Name, req.Etag, smpb.SecretVersion_DESTROYED)
}

// setState enables, disables or destroys a version; destroying wipes the payload.
func (s *Service) setState(name, tag string, st smpb.SecretVersion_State) (*smpb.SecretVersion, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, v, err := s.version(name)
	if err != nil {
		return nil, err
	}
	if err := checkEtag(tag, v.p.Etag); err != nil {
		return nil, err
	}
	if v.p.State == smpb.SecretVersion_DESTROYED {
		return nil, status.Errorf(codes.FailedPrecondition, "Secret Version [%s] is in DESTROYED state.", v.p.Name)
	}
	v.p.State, v.p.Etag = st, etag()
	if st == smpb.SecretVersion_DESTROYED {
		v.p.DestroyTime, v.data = timestamppb.Now(), nil
	}
	return clone(v.p), nil
}

// ---- IAM (stored, not enforced) ----

func (s *Service) SetIamPolicy(_ context.Context, req *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	if req.Policy == nil {
		return nil, invalid("The policy is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sc, err := s.secret(req.Resource)
	if err != nil {
		return nil, err
	}
	p := clone(req.Policy)
	p.Etag = []byte(core.RandHex(4))
	if p.Version == 0 {
		p.Version = 1
	}
	sc.policy = p
	return clone(p), nil
}

func (s *Service) GetIamPolicy(_ context.Context, req *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sc, err := s.secret(req.Resource)
	if err != nil {
		return nil, err
	}
	if sc.policy == nil {
		return &iampb.Policy{Version: 1}, nil
	}
	return clone(sc.policy), nil
}

// TestIamPermissions grants every permission asked for.
func (s *Service) TestIamPermissions(_ context.Context, req *iampb.TestIamPermissionsRequest) (*iampb.TestIamPermissionsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.secret(req.Resource); err != nil {
		return nil, err
	}
	return &iampb.TestIamPermissionsResponse{Permissions: req.Permissions}, nil
}

// ---- REST mirror ----

// split cuts /v1/projects/{p}[/locations/{l}]/secrets… into the project and the
// path after it; ok reports whether the path is Secret Manager's.
func split(path string) (project string, parts []string, ok bool) {
	seg := strings.Split(path, "/")
	if len(seg) < 5 || seg[0] != "" || seg[1] != "v1" || seg[2] != "projects" {
		return "", nil, false
	}
	parts = seg[4:]
	if parts[0] == "locations" && len(parts) > 2 {
		parts = parts[2:]
	}
	coll, _, _ := strings.Cut(parts[0], ":")
	return seg[3], parts, coll == "secrets"
}

func match(r *http.Request) bool {
	_, _, ok := split(r.URL.Path)
	return ok
}

// route maps a REST call onto a gRPC method: field takes the resource named
// by the path, body is the request field the JSON body fills ("" = all).
type route struct{ method, field, body string }

var routes = map[string]route{
	"GET secrets":                       {"ListSecrets", "parent", ""},
	"POST secrets":                      {"CreateSecret", "parent", "secret"},
	"GET secrets/*":                     {"GetSecret", "name", ""},
	"PATCH secrets/*":                   {"UpdateSecret", "secret.name", "secret"},
	"DELETE secrets/*":                  {"DeleteSecret", "name", ""},
	"POST secrets/*:addVersion":         {"AddSecretVersion", "parent", ""},
	"GET secrets/*/versions":            {"ListSecretVersions", "parent", ""},
	"GET secrets/*/versions/*":          {"GetSecretVersion", "name", ""},
	"GET secrets/*/versions/*:access":   {"AccessSecretVersion", "name", ""},
	"POST secrets/*/versions/*:enable":  {"EnableSecretVersion", "name", ""},
	"POST secrets/*/versions/*:disable": {"DisableSecretVersion", "name", ""},
	"POST secrets/*/versions/*:destroy": {"DestroySecretVersion", "name", ""},
	"POST secrets/*:setIamPolicy":       {"SetIamPolicy", "resource", ""},
	"GET secrets/*:getIamPolicy":        {"GetIamPolicy", "resource", ""},
	"POST secrets/*:testIamPermissions": {"TestIamPermissions", "resource", ""},
}

// ServeHTTP serves the REST mirror by calling the gRPC handlers.
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	project, parts, ok := split(r.URL.Path)
	verb := ""
	if ok {
		if last, v, cut := strings.Cut(parts[len(parts)-1], ":"); cut {
			parts[len(parts)-1], verb = last, ":"+v
		}
	}
	shape := slices.Clone(parts)
	for i := 1; i < len(shape); i += 2 {
		shape[i] = "*"
	}
	rt, found := routes[r.Method+" "+strings.Join(shape, "/")+verb]
	c.Op = rt.method
	if !ok || !found {
		writeErr(w, c, status.Errorf(codes.Unimplemented, "pogi does not implement %s %s", r.Method, r.URL.Path))
		return
	}
	if err := c.Intercept(); err != nil {
		e := core.AsError(err)
		writeJSONErr(w, c, e.Status, e.Code, e.Msg)
		return
	}
	res := "projects/" + project
	if n := len(parts) &^ 1; n > 0 {
		res += "/" + strings.Join(parts[:n], "/")
	}
	out, err := s.rest(r, rt, res)
	if err != nil {
		writeErr(w, c, err)
		return
	}
	b, _ := protojson.Marshal(out)
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.Write(b)
}

// rest builds the gRPC request from the JSON body, the query parameters and
// the resource name in the path, and calls the handler.
func (s *Service) rest(r *http.Request, rt route, res string) (proto.Message, error) {
	in, err := gapi.ParseBody(core.Body(r), rt.body)
	if err != nil {
		return nil, err
	}
	for k, v := range r.URL.Query() {
		in[k] = v[0]
	}
	setPath(in, rt.field, res)
	for _, m := range smpb.SecretManagerService_ServiceDesc.Methods {
		if m.MethodName != rt.method {
			continue
		}
		out, err := m.Handler(s, r.Context(), func(req any) error {
			return gapi.Decode(req.(proto.Message), in)
		}, nil)
		if err != nil {
			return nil, err
		}
		return out.(proto.Message), nil
	}
	return nil, status.Errorf(codes.Unimplemented, "%s is not implemented", rt.method)
}

// setPath stores v at a dotted path of a JSON object.
func setPath(m map[string]any, path, v string) {
	k, rest, nested := strings.Cut(path, ".")
	if !nested {
		m[k] = v
		return
	}
	child, _ := m[k].(map[string]any)
	if child == nil {
		child = map[string]any{}
		m[k] = child
	}
	setPath(child, rest, v)
}

var httpStatus = map[codes.Code]int{codes.InvalidArgument: 400, codes.FailedPrecondition: 400, codes.OutOfRange: 400,
	codes.Unauthenticated: 401, codes.PermissionDenied: 403, codes.NotFound: 404, codes.AlreadyExists: 409, codes.Aborted: 409,
	codes.ResourceExhausted: 429, codes.Canceled: 499, codes.Unimplemented: 501, codes.Unavailable: 503, codes.DeadlineExceeded: 504}

// writeErr renders a gRPC status as a Google REST error.
func writeErr(w http.ResponseWriter, c *core.Ctx, err error) {
	st := status.Convert(err)
	code, ok := httpStatus[st.Code()]
	if !ok {
		code = http.StatusInternalServerError
	}
	writeJSONErr(w, c, code, rpccode.Code(st.Code()).String(), st.Message())
}

func writeJSONErr(w http.ResponseWriter, c *core.Ctx, code int, name, msg string) {
	c.ErrCode = name
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": code, "message": msg, "status": name}})
}

// ---- console ----

// Resources lists secrets.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := core.Table{Name: "Secrets", Columns: []string{"Name", "Project", "Versions", "Latest state", "Created"}}
	for _, n := range core.SortedKeys(s.secrets) {
		sc := s.secrets[n]
		if expired(sc.p) {
			continue
		}
		latest := "-"
		if len(sc.versions) > 0 {
			latest = sc.versions[len(sc.versions)-1].p.State.String()
		}
		t.Rows = append(t.Rows, []string{n, strings.Split(n, "/")[1], strconv.Itoa(len(sc.versions)), latest,
			sc.p.CreateTime.AsTime().Format(time.DateTime)})
	}
	return []core.Table{t}
}

// Inspect returns a secret's metadata, versions and policy; never payloads.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	sc := s.secrets[id]
	if table != "Secrets" || sc == nil {
		return nil
	}
	versions := []json.RawMessage{}
	for _, v := range sc.versions {
		versions = append(versions, pj(v.p))
	}
	out := map[string]any{"secret": pj(sc.p), "versions": versions}
	if sc.policy != nil {
		out["policy"] = pj(sc.policy)
	}
	return out
}
