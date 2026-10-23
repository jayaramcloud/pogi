// Package spanner runs Google's own Cloud Spanner emulator in Docker behind
// pogi's port. The emulator starts on the first Spanner call. Its gRPC
// services (data, instance admin, database admin) and its REST gateway are
// passed through, so the official clients (SPANNER_EMULATOR_HOST) and Terraform
// work against pogi. What the emulator lacks and Terraform needs (instance
// configs such as regional-us-central1) pogi answers itself. Without Docker
// every call fails and says that Docker is needed.
package spanner

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instancepb "cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	spannerpb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	rpccode "google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Image is Google's emulator; docker.images in pogi.yaml may override it ("spanner.emulator").
const Image = "gcr.io/cloud-spanner-emulator/emulator"

const (
	grpcPort = 9010
	restPort = 9020
)

const noDocker = "Cloud Spanner runs Google's emulator in Docker, and no container engine is running: start Docker (or Podman, Colima, OrbStack) and try again"

// Service passes Spanner calls to the emulator.
type Service struct {
	hub *core.Hub
	mu  sync.Mutex // held while the emulator starts, so that calls made meanwhile wait for it
	box *box       // nil until the first call
}

// box is the running emulator.
type box struct {
	container string
	rest      *httputil.ReverseProxy
	conn      *grpc.ClientConn
}

// New creates the Spanner service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub}
	if hub != nil {
		gapi.Operations(hub).Delegate(func() longrunningpb.OperationsClient {
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.box == nil {
				return nil
			}
			return longrunningpb.NewOperationsClient(s.box.conn)
		})
	}
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "spanner", Cloud: core.GCP, Title: "Cloud Spanner", Match: match,
		Targets: []string{"google.spanner.v1.Spanner", "google.spanner.admin.instance.v1.InstanceAdmin", "google.spanner.admin.database.v1.DatabaseAdmin"}}
}

// restPath is Spanner's REST surface that pogi routes: instances (with their databases, backups and
// operations) and instance configs of project p. Core has turned /spanner/v1/... into /v1/... on spanner.googleapis.com.
var restPath = regexp.MustCompile(`^/v1/projects/[^/]+/(instances|instanceConfigs)(/|:|$)`)

// match claims the Spanner API on host spanner.googleapis.com. On any other host the path is
// ambiguous (Cloud SQL, Memorystore and Filestore have instances too), so the /spanner/ prefix is needed.
func match(r *http.Request) bool {
	host, _, err := net.SplitHostPort(r.Host)
	if err != nil {
		host = r.Host
	}
	return strings.HasPrefix(strings.ToLower(host), "spanner.") && restPath.MatchString(r.URL.Path)
}

// RegisterGRPC passes the three Spanner services to the emulator.
func (s *Service) RegisterGRPC(g *grpc.Server) {
	for _, sd := range []*grpc.ServiceDesc{&spannerpb.Spanner_ServiceDesc, &instancepb.InstanceAdmin_ServiceDesc, &databasepb.DatabaseAdmin_ServiceDesc} {
		g.RegisterService(gapi.ProxyDesc(sd, s.dial, nil), nil)
	}
}

func (s *Service) dial(ctx context.Context) (*grpc.ClientConn, error) {
	b, err := s.emulator(ctx)
	if err != nil {
		return nil, err
	}
	return b.conn, nil
}

// emulator returns the running emulator, starting it first if needed.
func (s *Service) emulator(ctx context.Context) (*box, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.box != nil {
		return s.box, nil
	}
	if s.hub == nil || !s.hub.Docker().Available() {
		return nil, status.Error(codes.FailedPrecondition, noDocker)
	}
	b, err := s.start(context.WithoutCancel(ctx)) // a client that gives up must not leave a half-started emulator
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "the Spanner emulator did not start: %v", err)
	}
	s.box = b
	return b, nil
}

// start runs the container and waits until its REST gateway answers. Lock held.
func (s *Service) start(ctx context.Context) (*box, error) {
	d := s.hub.Docker()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute) // the image may have to be pulled
	defer cancel()
	name := docker.Name("spanner", "emulator-"+core.NewID()[:8])
	id, err := d.Run(ctx, &docker.Container{Name: name, Image: Image, Service: "spanner", Resource: "emulator",
		Ports: map[int]int{grpcPort: 0, restPort: 0}})
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*box, error) {
		d.Remove(context.WithoutCancel(ctx), id)
		return nil, err
	}
	info, err := d.Inspect(ctx, id)
	if err != nil {
		return fail(err)
	}
	g, r := info.HostPort(grpcPort), info.HostPort(restPort)
	if g == 0 || r == 0 {
		return fail(fmt.Errorf("the container published no ports"))
	}
	rest, err := url.Parse("http://" + d.Addr(r))
	if err != nil {
		return fail(err)
	}
	for end := time.Now().Add(time.Minute); ; time.Sleep(200 * time.Millisecond) {
		resp, err := (&http.Client{Timeout: 5 * time.Second}).Get(rest.String() + "/v1/projects/p/instances")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(end) || ctx.Err() != nil {
			return fail(fmt.Errorf("no answer from the container: %w", err))
		}
	}
	conn, err := grpc.NewClient(d.Addr(g), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fail(err)
	}
	p := httputil.NewSingleHostReverseProxy(rest)
	p.ModifyResponse = googleError
	return &box{container: id, rest: p, conn: conn}, nil
}

// googleError rewrites the emulator's REST errors, {"code": 5, "message": …} with a gRPC code, into
// the form of Google's REST APIs, {"error": {"code": 404, "message": …, "status": "NOT_FOUND"}}, which
// the Google API clients parse.
func googleError(resp *http.Response) error {
	if resp.StatusCode < 400 {
		return nil
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	resp.Body.Close()
	var e struct {
		Code    *int
		Message string
		Error   json.RawMessage
	}
	if err == nil && json.Unmarshal(b, &e) == nil && e.Code != nil && e.Error == nil {
		b, _ = json.Marshal(map[string]any{"error": map[string]any{"code": resp.StatusCode, "message": e.Message,
			"status": rpccode.Code(*e.Code).String()}})
		resp.Header.Set("Content-Type", "application/json; charset=UTF-8")
	}
	resp.Body = io.NopCloser(strings.NewReader(string(b)))
	resp.ContentLength = int64(len(b))
	resp.Header.Set("Content-Length", strconv.Itoa(len(b)))
	return nil
}

// configID is an instance config name the emulator does not have but real Spanner does:
// regional-<region> and the multi-region names (nam3, eur5, asia1, ...).
var configID = regexp.MustCompile(`^(regional-[a-z]+[a-z0-9-]*|nam[0-9]+|eur[0-9]+|asia[0-9]+|nam-eur-asia[0-9]+)$`)

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	c.Op = r.Method + " " + strings.Join(strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/projects/"), "/")[1:], "/")
	if err := c.Intercept(); err != nil {
		e := core.AsError(err)
		gapi.JSONErr(w, c, e.Status, e.Code, e.Msg)
		return
	}
	// Terraform and the Spanner clients read the config an instance names; the emulator knows only emulator-config.
	if p := strings.Split(r.URL.Path, "/"); r.Method == http.MethodGet && len(p) == 6 && p[4] == "instanceConfigs" && configID.MatchString(p[5]) {
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		json.NewEncoder(w).Encode(map[string]any{"name": "projects/" + p[3] + "/instanceConfigs/" + p[5], "displayName": p[5],
			"configType": "GOOGLE_MANAGED"})
		return
	}
	b, err := s.emulator(r.Context())
	if err != nil {
		gapi.Err(w, c, err)
		return
	}
	b.rest.ServeHTTP(w, r)
}

// Reset removes the emulator and with it every instance and database.
func (s *Service) Reset() { s.stop() }

// Close removes the emulator.
func (s *Service) Close() error {
	s.stop()
	return nil
}

func (s *Service) stop() {
	s.mu.Lock()
	b := s.box
	s.box = nil
	s.mu.Unlock()
	if b != nil {
		b.conn.Close()
		s.hub.Docker().Remove(context.Background(), b.container)
	}
}

// Save, Load: the emulator keeps its data in memory only, so nothing is saved.
// ponytail: instances and databases are gone after a restart; the emulator has no data directory to keep.
func (s *Service) Save() ([]byte, error) { return []byte("{}"), nil }
func (s *Service) Load([]byte) error     { return nil }
