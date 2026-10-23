// Package bigtable emulates Cloud Bigtable. Data and table admin are the
// official emulator, the bttest package of cloud.google.com/go/bigtable (what
// "gcloud beta emulators bigtable" runs), started in this process on the first
// call: Bigtable needs no Docker. bttest has no instance admin, so pogi serves
// that itself (instances, clusters, app profiles), over gRPC and over REST at
// /bigtableadmin/v2/. Point the clients at pogi with
// BIGTABLE_EMULATOR_HOST=localhost:7644.
package bigtable

import (
	"context"
	"net"
	"net/http"
	"regexp"
	"strings"
	"sync"

	"cloud.google.com/go/bigtable/admin/apiv2/adminpb"
	"cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"cloud.google.com/go/bigtable/bttest"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Service is the Bigtable emulator.
type Service struct {
	adminpb.UnimplementedBigtableInstanceAdminServer
	hub  *core.Hub
	ops  *gapi.Ops
	rest *gapi.REST
	mu   sync.Mutex
	inst map[string]*instance // by name, projects/p/instances/i
	box  *box                 // nil until a table or data call
}

// box is the in-process bttest server and the connection to it.
type box struct {
	srv  *bttest.Server
	conn *grpc.ClientConn
}

// New creates the Bigtable service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, inst: map[string]*instance{}}
	if hub != nil {
		s.ops = gapi.Operations(hub)
	}
	s.rest = &gapi.REST{Versions: []string{"v2"}, Collections: []string{"instances"}, Desc: &adminpb.BigtableInstanceAdmin_ServiceDesc, Impl: s,
		Routes: map[string]gapi.Route{
			"POST instances":                       {Method: "CreateInstance", Field: "parent"},
			"GET instances":                        {Method: "ListInstances", Field: "parent"},
			"GET instances/*":                      {Method: "GetInstance", Field: "name"},
			"PUT instances/*":                      {Method: "UpdateInstance", Field: "name"},
			"PATCH instances/*":                    {Method: "PartialUpdateInstance", Field: "instance.name", Body: "instance"},
			"DELETE instances/*":                   {Method: "DeleteInstance", Field: "name"},
			"POST instances/*/clusters":            {Method: "CreateCluster", Field: "parent", Body: "cluster"},
			"GET instances/*/clusters":             {Method: "ListClusters", Field: "parent"},
			"GET instances/*/clusters/*":           {Method: "GetCluster", Field: "name"},
			"PATCH instances/*/clusters/*":         {Method: "PartialUpdateCluster", Field: "cluster.name", Body: "cluster"},
			"PUT instances/*/clusters/*":           {Method: "UpdateCluster", Field: "name"},
			"DELETE instances/*/clusters/*":        {Method: "DeleteCluster", Field: "name"},
			"POST instances/*/appProfiles":         {Method: "CreateAppProfile", Field: "parent", Body: "appProfile"},
			"GET instances/*/appProfiles":          {Method: "ListAppProfiles", Field: "parent"},
			"GET instances/*/appProfiles/*":        {Method: "GetAppProfile", Field: "name"},
			"PATCH instances/*/appProfiles/*":      {Method: "UpdateAppProfile", Field: "appProfile.name", Body: "appProfile"},
			"DELETE instances/*/appProfiles/*":     {Method: "DeleteAppProfile", Field: "name"},
			"GET instances/*/operations/*":         {Method: "GetOperation", Field: "name"},
			"GET instances/*/operations":           {Method: "ListOperations", Field: "name"},
			"DELETE instances/*/operations/*":      {Method: "DeleteOperation", Field: "name"},
			"POST instances/*/operations/*:cancel": {Method: "CancelOperation", Field: "name"},
		}}
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "bigtable", Cloud: core.GCP, Title: "Cloud Bigtable", Match: s.match,
		Targets: []string{"google.bigtable.admin.v2.BigtableInstanceAdmin", "google.bigtable.admin.v2.BigtableTableAdmin", "google.bigtable.v2.Bigtable"}}
}

// match claims the instance admin API on host bigtableadmin.googleapis.com, which core's
// /bigtableadmin/ prefix gives. On other hosts /v2/projects/p/instances is ambiguous.
func (s *Service) match(r *http.Request) bool {
	host, _, err := net.SplitHostPort(r.Host)
	if err != nil {
		host = r.Host
	}
	return strings.HasPrefix(strings.ToLower(host), "bigtableadmin.") && s.rest.Match(r)
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.rest.ServeHTTP(w, r) }

// RegisterGRPC serves instance admin itself and passes table admin and data to bttest.
func (s *Service) RegisterGRPC(g *grpc.Server) {
	adminpb.RegisterBigtableInstanceAdminServer(g, s)
	g.RegisterService(gapi.ProxyDesc(&adminpb.BigtableTableAdmin_ServiceDesc, s.dial, s.known), nil)
	g.RegisterService(gapi.ProxyDesc(&bigtablepb.Bigtable_ServiceDesc, s.dial, s.known), nil)
}

// bttest returns the connection to bttest, which it starts first if needed.
func (s *Service) engine() (*grpc.ClientConn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.box == nil {
		srv, err := bttest.NewServer("127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		conn, err := grpc.NewClient(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			srv.Close()
			return nil, err
		}
		s.box = &box{srv, conn}
	}
	return s.box.conn, nil
}

func (s *Service) dial(context.Context) (*grpc.ClientConn, error) { return s.engine() }

// instanceFields are the request fields that name a resource inside an instance, in the order they are tried.
var instanceFields = []protoreflect.Name{"parent", "name", "table_name", "authorized_view_name", "materialized_view_name"}

// tableRE is what a table id may be (Bigtable's documented form), up to 50 characters.
var tableRE = regexp.MustCompile(`^[_a-zA-Z0-9][-_.a-zA-Z0-9]*$`)

// known refuses a table or data request that names an instance pogi does not have, and a table with a bad id.
// bttest accepts both.
func (s *Service) known(m proto.Message) error {
	if c, ok := m.(*adminpb.CreateTableRequest); ok && (len(c.TableId) > 50 || !tableRE.MatchString(c.TableId)) {
		return gapi.Invalid("Invalid table id %q: use up to 50 letters, digits, '_', '-' and '.', not starting with '-' or '.'", c.TableId)
	}
	for _, f := range instanceFields {
		fd := m.ProtoReflect().Descriptor().Fields().ByName(f)
		if fd == nil || fd.Kind() != protoreflect.StringKind || fd.IsList() {
			continue
		}
		p := strings.Split(m.ProtoReflect().Get(fd).String(), "/")
		if len(p) < 4 || p[0] != "projects" || p[2] != "instances" {
			continue
		}
		name := strings.Join(p[:4], "/")
		s.mu.Lock()
		_, ok := s.inst[name]
		s.mu.Unlock()
		if !ok {
			return gapi.NotFound("Instance %s not found.", name)
		}
		return nil
	}
	return nil
}

// tables removes every table of an instance that is deleted.
func (s *Service) dropTables(ctx context.Context, name string) {
	s.mu.Lock()
	b := s.box
	s.mu.Unlock()
	if b == nil {
		return
	}
	c := adminpb.NewBigtableTableAdminClient(b.conn)
	l, err := c.ListTables(ctx, &adminpb.ListTablesRequest{Parent: name})
	if err != nil {
		return
	}
	for _, t := range l.Tables {
		c.DeleteTable(ctx, &adminpb.DeleteTableRequest{Name: t.Name})
	}
}

// Reset removes every instance and table.
func (s *Service) Reset() {
	s.mu.Lock()
	b := s.box
	s.box, s.inst = nil, map[string]*instance{}
	s.mu.Unlock()
	if b != nil {
		b.conn.Close()
		b.srv.Close()
	}
}

// Close stops bttest.
func (s *Service) Close() error {
	s.Reset()
	return nil
}

// Save, Load: bttest keeps its data in memory only, so nothing is saved.
// ponytail: instances and tables are gone after a restart; bttest cannot save its tables.
func (s *Service) Save() ([]byte, error) { return []byte("{}"), nil }
func (s *Service) Load([]byte) error     { return nil }

// The operations service answers the REST polls of an instance's operations.
func (s *Service) GetOperation(ctx context.Context, r *longrunningpb.GetOperationRequest) (*longrunningpb.Operation, error) {
	return s.ops.GetOperation(ctx, r)
}
func (s *Service) ListOperations(ctx context.Context, r *longrunningpb.ListOperationsRequest) (*longrunningpb.ListOperationsResponse, error) {
	return s.ops.ListOperations(ctx, r)
}
func (s *Service) DeleteOperation(ctx context.Context, r *longrunningpb.DeleteOperationRequest) (*emptypb.Empty, error) {
	return s.ops.DeleteOperation(ctx, r)
}
func (s *Service) CancelOperation(ctx context.Context, r *longrunningpb.CancelOperationRequest) (*emptypb.Empty, error) {
	return s.ops.CancelOperation(ctx, r)
}
