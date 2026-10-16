// Package serviceusage emulates Service Usage v1: which Google APIs a
// project has enabled. Enable and disable are long-running operations that
// finish at once, on the shared operations service. Enabling is bookkeeping
// only: pogi serves every API whether a project enabled it or not.
package serviceusage

import (
	"context"
	"encoding/json"
	"net/http"
	"regexp"
	"strings"
	"sync"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	pb "cloud.google.com/go/serviceusage/apiv1/serviceusagepb"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	"google.golang.org/grpc"
)

// Service is Service Usage.
type Service struct {
	pb.UnimplementedServiceUsageServer
	hub  *core.Hub
	mu   sync.Mutex
	st   state
	rest *gapi.Mux
}

// state maps a project (as the caller named it: id or number) to its enabled services.
type state struct {
	Enabled map[string]map[string]bool `json:"enabled"`
}

// New creates the Service Usage service.
func New(h *core.Hub) *Service {
	s := &Service{hub: h}
	s.Reset()
	var rules []gapi.Rule
	for _, parent := range []string{"projects", "folders", "organizations"} {
		p := "/v1/{parent=" + parent + "/*}/services"
		n := "/v1/{name=" + parent + "/*/services/*}"
		rules = append(rules,
			gapi.Rule{HTTP: "GET " + p, Method: "ListServices"},
			gapi.Rule{HTTP: "POST " + p + ":batchEnable", Method: "BatchEnableServices"},
			gapi.Rule{HTTP: "GET " + p + ":batchGet", Method: "BatchGetServices"},
			gapi.Rule{HTTP: "GET " + n, Method: "GetService"},
			gapi.Rule{HTTP: "POST " + n + ":enable", Method: "EnableService"},
			gapi.Rule{HTTP: "POST " + n + ":disable", Method: "DisableService"})
	}
	s.rest = gapi.NewMux(s, rules...)
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "serviceusage", Cloud: core.GCP, Title: "Service Usage", Match: s.rest.Match,
		Targets: []string{"google.api.serviceusage.v1.ServiceUsage"}}
}

func (s *Service) RegisterGRPC(g *grpc.Server) { pb.RegisterServiceUsageServer(g, s) }

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.rest.ServeHTTP(w, r) }

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{Enabled: map[string]map[string]bool{}}
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
	if st.Enabled == nil {
		st.Enabled = map[string]map[string]bool{}
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

// ---- services ----

var (
	nameRE    = regexp.MustCompile(`^((?:projects|folders|organizations)/[^/]+)/services/([^/]+)$`)
	parentRE  = regexp.MustCompile(`^(?:projects|folders|organizations)/[^/]+$`)
	serviceRE = regexp.MustCompile(`^[a-z][a-z0-9-]*(\.[a-z0-9-]+)+$`)
)

// titles are the display names of the APIs people enable most; any other
// API is titled by its name.
var titles = map[string]string{
	"compute.googleapis.com":              "Compute Engine API",
	"storage.googleapis.com":              "Cloud Storage API",
	"storage-api.googleapis.com":          "Google Cloud Storage JSON API",
	"iam.googleapis.com":                  "Identity and Access Management (IAM) API",
	"iamcredentials.googleapis.com":       "IAM Service Account Credentials API",
	"cloudresourcemanager.googleapis.com": "Cloud Resource Manager API",
	"serviceusage.googleapis.com":         "Service Usage API",
	"pubsub.googleapis.com":               "Cloud Pub/Sub API",
	"run.googleapis.com":                  "Cloud Run Admin API",
	"cloudfunctions.googleapis.com":       "Cloud Functions API",
	"cloudtasks.googleapis.com":           "Cloud Tasks API",
	"cloudscheduler.googleapis.com":       "Cloud Scheduler API",
	"eventarc.googleapis.com":             "Eventarc API",
	"secretmanager.googleapis.com":        "Secret Manager API",
	"firestore.googleapis.com":            "Cloud Firestore API",
	"bigquery.googleapis.com":             "BigQuery API",
	"sqladmin.googleapis.com":             "Cloud SQL Admin API",
	"cloudkms.googleapis.com":             "Cloud Key Management Service (KMS) API",
	"logging.googleapis.com":              "Cloud Logging API",
	"monitoring.googleapis.com":           "Cloud Monitoring API",
	"managedkafka.googleapis.com":         "Managed Service for Apache Kafka API",
}

// split cuts projects/<p>/services/<s>.
func split(name string) (parent, svc string, err error) {
	m := nameRE.FindStringSubmatch(name)
	if m == nil {
		return "", "", gapi.Invalid("Invalid service name %q: want projects/{project}/services/{service}", name)
	}
	if !serviceRE.MatchString(m[2]) {
		return "", "", gapi.NotFound("Service %q not found or permission denied.", m[2])
	}
	return m[1], m[2], nil
}

func (s *Service) service(parent, svc string) *pb.Service {
	title, ok := titles[svc]
	if !ok {
		title = svc
	}
	st := pb.State_DISABLED
	if s.st.Enabled[parent][svc] {
		st = pb.State_ENABLED
	}
	return &pb.Service{Name: parent + "/services/" + svc, Parent: parent, State: st,
		Config: &pb.ServiceConfig{Name: svc, Title: title}}
}

// set enables or disables a service. Lock held.
func (s *Service) set(parent, svc string, on bool) *pb.Service {
	if on {
		if s.st.Enabled[parent] == nil {
			s.st.Enabled[parent] = map[string]bool{}
		}
		s.st.Enabled[parent][svc] = true
	} else {
		delete(s.st.Enabled[parent], svc)
	}
	return s.service(parent, svc)
}

func (s *Service) EnableService(_ context.Context, req *pb.EnableServiceRequest) (*longrunningpb.Operation, error) {
	parent, svc, err := split(req.Name)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	out := s.set(parent, svc, true)
	s.mu.Unlock()
	return gapi.Operations(s.hub).Done("", &pb.OperationMetadata{ResourceNames: []string{out.Name}}, &pb.EnableServiceResponse{Service: out}), nil
}

func (s *Service) DisableService(_ context.Context, req *pb.DisableServiceRequest) (*longrunningpb.Operation, error) {
	parent, svc, err := split(req.Name)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	out := s.set(parent, svc, false)
	s.mu.Unlock()
	return gapi.Operations(s.hub).Done("", &pb.OperationMetadata{ResourceNames: []string{out.Name}}, &pb.DisableServiceResponse{Service: out}), nil
}

func (s *Service) BatchEnableServices(_ context.Context, req *pb.BatchEnableServicesRequest) (*longrunningpb.Operation, error) {
	if !parentRE.MatchString(req.Parent) {
		return nil, gapi.Invalid("Invalid parent %q: want projects/{project}", req.Parent)
	}
	if len(req.ServiceIds) == 0 || len(req.ServiceIds) > 20 {
		return nil, gapi.Invalid("service_ids must name between 1 and 20 services.")
	}
	for _, id := range req.ServiceIds {
		if !serviceRE.MatchString(id) {
			return nil, gapi.NotFound("Service %q not found or permission denied.", id)
		}
	}
	resp := &pb.BatchEnableServicesResponse{}
	var names []string
	s.mu.Lock()
	for _, id := range req.ServiceIds {
		out := s.set(req.Parent, id, true)
		resp.Services, names = append(resp.Services, out), append(names, out.Name)
	}
	s.mu.Unlock()
	return gapi.Operations(s.hub).Done("", &pb.OperationMetadata{ResourceNames: names}, resp), nil
}

func (s *Service) GetService(_ context.Context, req *pb.GetServiceRequest) (*pb.Service, error) {
	parent, svc, err := split(req.Name)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.service(parent, svc), nil
}

func (s *Service) BatchGetServices(_ context.Context, req *pb.BatchGetServicesRequest) (*pb.BatchGetServicesResponse, error) {
	out := &pb.BatchGetServicesResponse{}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, n := range req.Names {
		parent, svc, err := split(n)
		if err != nil {
			return nil, err
		}
		out.Services = append(out.Services, s.service(parent, svc))
	}
	return out, nil
}

// ListServices lists the services a parent has enabled; filter "state:DISABLED"
// lists the known ones that are not. Without a filter it lists both.
// ponytail: "known" is the titled APIs above; pogi has no catalog of all of Google's.
func (s *Service) ListServices(_ context.Context, req *pb.ListServicesRequest) (*pb.ListServicesResponse, error) {
	if !parentRE.MatchString(req.Parent) {
		return nil, gapi.Invalid("Invalid parent %q: want projects/{project}", req.Parent)
	}
	want := strings.ToUpper(strings.TrimSpace(strings.TrimPrefix(strings.ReplaceAll(req.Filter, " ", ""), "state:")))
	s.mu.Lock()
	defer s.mu.Unlock()
	names := map[string]bool{}
	for svc := range s.st.Enabled[req.Parent] {
		names[svc] = true
	}
	if want != "ENABLED" {
		for svc := range titles {
			names[svc] = true
		}
	}
	var list []*pb.Service
	for _, svc := range core.SortedKeys(names) {
		if p := s.service(req.Parent, svc); want == "" || p.State.String() == want {
			list = append(list, p)
		}
	}
	items, next := gapi.Page(list, req.PageSize, req.PageToken)
	return &pb.ListServicesResponse{Services: items, NextPageToken: next}, nil
}

// ---- console ----

func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := core.Table{Name: "Enabled services", Columns: []string{"Project", "Service"}}
	for _, p := range core.SortedKeys(s.st.Enabled) {
		for _, svc := range core.SortedKeys(s.st.Enabled[p]) {
			t.Rows = append(t.Rows, []string{p, svc})
		}
	}
	return []core.Table{t}
}
