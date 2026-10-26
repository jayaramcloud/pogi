package run

import (
	"encoding/json"
	"net/http"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/run/apiv2/runpb"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// The Knative v1 Service: the API of GA `gcloud run deploy`. It is a view over the v2 state: every write
// goes through the v2 methods. The region is only in the host (us-central1-<host>), not in the body.

var (
	knativeRE = regexp.MustCompile(`^/apis/serving\.knative\.dev/v1/namespaces/([^/]+)/services(?:/([^/]+))?$`)
	regionRE  = regexp.MustCompile(`^([a-z]+-[a-z]+[0-9]+)-`)
	// v1All is the list `gcloud run services list` makes without --region: every region of a project.
	// Only location "-" is claimed, because Dataproc Metastore shares the path projects/p/locations/l/services.
	v1All = regexp.MustCompile(`^/v1/projects/([^/]+)/locations/-/services$`)
)

const locationLabel = "cloud.googleapis.com/location"

type v1Meta struct {
	Name              string            `json:"name,omitempty"`
	Namespace         string            `json:"namespace,omitempty"`
	SelfLink          string            `json:"selfLink,omitempty"`
	UID               string            `json:"uid,omitempty"`
	ResourceVersion   string            `json:"resourceVersion,omitempty"`
	Generation        int64             `json:"generation,omitempty"`
	CreationTimestamp string            `json:"creationTimestamp,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	Annotations       map[string]string `json:"annotations,omitempty"`
}

type v1Pod struct {
	ContainerConcurrency int32             `json:"containerConcurrency,omitempty"`
	TimeoutSeconds       int64             `json:"timeoutSeconds,omitempty"`
	ServiceAccountName   string            `json:"serviceAccountName,omitempty"`
	Containers           []json.RawMessage `json:"containers,omitempty"`
	Volumes              []json.RawMessage `json:"volumes,omitempty"`
}

type v1Template struct {
	Metadata v1Meta `json:"metadata"`
	Spec     v1Pod  `json:"spec"`
}

type v1Traffic struct {
	LatestRevision bool   `json:"latestRevision,omitempty"`
	Percent        int32  `json:"percent"`
	RevisionName   string `json:"revisionName,omitempty"`
	Tag            string `json:"tag,omitempty"`
	URL            string `json:"url,omitempty"`
}

type v1Condition struct {
	Type               string `json:"type"`
	Status             string `json:"status"`
	Message            string `json:"message,omitempty"`
	LastTransitionTime string `json:"lastTransitionTime,omitempty"`
}

type v1Address struct {
	URL string `json:"url,omitempty"`
}

type v1Status struct {
	ObservedGeneration        int64         `json:"observedGeneration,omitempty"`
	Conditions                []v1Condition `json:"conditions,omitempty"`
	LatestReadyRevisionName   string        `json:"latestReadyRevisionName,omitempty"`
	LatestCreatedRevisionName string        `json:"latestCreatedRevisionName,omitempty"`
	Traffic                   []v1Traffic   `json:"traffic,omitempty"`
	URL                       string        `json:"url,omitempty"`
	Address                   *v1Address    `json:"address,omitempty"`
}

type v1Service struct {
	APIVersion string `json:"apiVersion,omitempty"`
	Kind       string `json:"kind,omitempty"`
	Metadata   v1Meta `json:"metadata"`
	Spec       struct {
		Template v1Template  `json:"template"`
		Traffic  []v1Traffic `json:"traffic,omitempty"`
	} `json:"spec"`
	Status *v1Status `json:"status,omitempty"`
}

// fromV1 is the v2 service a Knative body describes (only the fields pogi uses).
func fromV1(in *v1Service) (*runpb.Service, error) {
	out := &runpb.Service{Annotations: in.Metadata.Annotations, Template: &runpb.RevisionTemplate{
		Revision: in.Spec.Template.Metadata.Name, Labels: in.Spec.Template.Metadata.Labels, Annotations: in.Spec.Template.Metadata.Annotations,
		MaxInstanceRequestConcurrency: in.Spec.Template.Spec.ContainerConcurrency, ServiceAccount: in.Spec.Template.Spec.ServiceAccountName}}
	for k, v := range in.Metadata.Labels {
		if k != locationLabel {
			if out.Labels == nil {
				out.Labels = map[string]string{}
			}
			out.Labels[k] = v
		}
	}
	if n := in.Spec.Template.Spec.TimeoutSeconds; n > 0 {
		out.Template.Timeout = durationpb.New(time.Duration(n) * time.Second)
	}
	if len(in.Spec.Template.Spec.Volumes) > 0 {
		return nil, gapi.Invalid("Invalid value for spec.template.spec.volumes: pogi's v1 view does not take volumes; use the v2 API")
	}
	for i, raw := range in.Spec.Template.Spec.Containers {
		c := &runpb.Container{}
		if err := protojson.Unmarshal(raw, c); err != nil {
			return nil, gapi.Invalid("Invalid value for spec.template.spec.containers[%d]: %s", i, gapi.PlainProto(raw, err))
		}
		out.Template.Containers = append(out.Template.Containers, c)
	}
	for _, t := range in.Spec.Traffic {
		tt := &runpb.TrafficTarget{Percent: t.Percent, Revision: t.RevisionName, Tag: t.Tag}
		if t.LatestRevision {
			tt.Type, tt.Revision = latestType, ""
		}
		out.Traffic = append(out.Traffic, tt)
	}
	return out, nil
}

func stamp(t interface{ AsTime() time.Time }) string { return t.AsTime().UTC().Format(time.RFC3339) }

// toV1 is what every reply shows.
func toV1(svc *runpb.Service) *v1Service {
	project, region := splitName(svc.Name)
	out := &v1Service{APIVersion: "serving.knative.dev/v1", Kind: "Service"}
	m := &out.Metadata
	m.Name, m.Namespace, m.UID, m.Generation = last(svc.Name), project, svc.Uid, svc.Generation
	m.SelfLink = "/apis/serving.knative.dev/v1/namespaces/" + project + "/services/" + m.Name
	m.ResourceVersion, m.Annotations = strings.Trim(svc.Etag, `"`), svc.Annotations
	m.CreationTimestamp = stamp(svc.CreateTime)
	m.Labels = map[string]string{locationLabel: region}
	for k, v := range svc.Labels {
		m.Labels[k] = v
	}
	t := svc.Template
	if t == nil {
		t = &runpb.RevisionTemplate{}
	}
	p := &out.Spec.Template
	p.Metadata = v1Meta{Name: t.Revision, Labels: t.Labels, Annotations: t.Annotations}
	p.Spec = v1Pod{ContainerConcurrency: t.MaxInstanceRequestConcurrency, TimeoutSeconds: int64(t.Timeout.AsDuration() / time.Second), ServiceAccountName: t.ServiceAccount}
	for _, c := range t.Containers {
		b, _ := protojson.Marshal(c)
		p.Spec.Containers = append(p.Spec.Containers, b)
	}
	for _, t := range svc.Traffic {
		out.Spec.Traffic = append(out.Spec.Traffic, v1Traffic{LatestRevision: t.Type == latestType, Percent: t.Percent, RevisionName: t.Revision, Tag: t.Tag})
	}
	st := &v1Status{ObservedGeneration: svc.ObservedGeneration, LatestReadyRevisionName: last(svc.LatestReadyRevision),
		LatestCreatedRevisionName: last(svc.LatestCreatedRevision), URL: svc.Uri, Address: &v1Address{URL: svc.Uri}}
	for _, c := range svc.Conditions {
		s := map[runpb.Condition_State]string{runpb.Condition_CONDITION_SUCCEEDED: "True", runpb.Condition_CONDITION_FAILED: "False"}[c.State]
		st.Conditions = append(st.Conditions, v1Condition{Type: c.Type, Status: cmpUnknown(s), Message: c.Message, LastTransitionTime: stamp(c.LastTransitionTime)})
	}
	for _, t := range svc.TrafficStatuses {
		st.Traffic = append(st.Traffic, v1Traffic{LatestRevision: t.Type == latestType, Percent: t.Percent, RevisionName: t.Revision, Tag: t.Tag, URL: t.Uri})
	}
	out.Status = st
	return out
}

func cmpUnknown(s string) string {
	if s == "" {
		return "Unknown"
	}
	return s
}

// regionOf reads the region from a <region>-<host> prefix; "" when there is none.
func regionOf(host string) string {
	host, _, _ = strings.Cut(strings.ToLower(host), ":")
	if m := regionRE.FindStringSubmatch(host); m != nil {
		return m[1]
	}
	return ""
}

func (s *Service) serveKnative(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	m := knativeRE.FindStringSubmatch(r.URL.Path)
	project, name := m[1], m[2]
	c.Op = map[string]string{"GET": "GetService", "POST": "CreateService", "PUT": "ReplaceService", "DELETE": "DeleteService"}[r.Method]
	if name == "" && r.Method == "GET" {
		c.Op = "ListServices"
	}
	fail := func(err error) { gapi.Err(w, c, err) }
	if c.Op == "" || name == "" && (r.Method == "PUT" || r.Method == "DELETE") || name != "" && r.Method == "POST" {
		fail(status.Errorf(codes.Unimplemented, "pogi does not implement %s %s", r.Method, r.URL.Path))
		return
	}
	if err := c.Intercept(); err != nil {
		e := core.AsError(err)
		gapi.JSONErr(w, c, e.Status, e.Code, e.Msg)
		return
	}
	region := regionOf(r.Host)
	if region == "" && c.Op != "ListServices" {
		fail(gapi.Invalid("The Knative v1 API needs a regional host such as us-central1-%s", r.Host))
		return
	}
	dry := r.URL.Query().Get("dryRun") == "all"
	parentName := "projects/" + project + "/locations/" + region
	ctx := r.Context()

	switch c.Op {
	case "ListServices":
		list := struct {
			APIVersion string       `json:"apiVersion"`
			Kind       string       `json:"kind"`
			Metadata   struct{}     `json:"metadata"`
			Items      []*v1Service `json:"items"`
		}{APIVersion: "serving.knative.dev/v1", Kind: "ServiceList", Items: []*v1Service{}}
		for _, svc := range s.listProject(project, region) {
			list.Items = append(list.Items, toV1(svc))
		}
		gapi.JSON(w, list)
	case "GetService":
		svc, err := s.GetService(ctx, &runpb.GetServiceRequest{Name: parentName + "/services/" + name})
		if err != nil {
			fail(err)
			return
		}
		gapi.JSON(w, toV1(svc))
	case "DeleteService":
		op, err := s.DeleteService(ctx, &runpb.DeleteServiceRequest{Name: parentName + "/services/" + name, ValidateOnly: dry})
		if err != nil {
			fail(err)
			return
		}
		svc := &runpb.Service{}
		op.GetResponse().UnmarshalTo(svc)
		gapi.JSON(w, map[string]any{"apiVersion": "v1", "kind": "Status", "metadata": map[string]any{}, "status": "Success",
			"details": map[string]any{"name": name, "group": "serving.knative.dev", "kind": "services", "uid": svc.Uid}})
	default: // CreateService, ReplaceService
		in := &v1Service{}
		body := core.Body(r)
		if err := json.Unmarshal(body, in); err != nil {
			fail(gapi.Invalid("%s", core.PlainJSON(err)))
			return
		}
		if n := in.Metadata.Namespace; n != "" && n != project || name != "" && in.Metadata.Name != "" && in.Metadata.Name != name {
			fail(gapi.Invalid("metadata.namespace and metadata.name must match the request path"))
			return
		}
		if name == "" {
			name = in.Metadata.Name
		}
		svc, err := fromV1(in)
		if err != nil {
			fail(err)
			return
		}
		resp := &runpb.Service{}
		if c.Op == "CreateService" {
			o, err := s.CreateService(ctx, &runpb.CreateServiceRequest{Parent: parentName, ServiceId: name, Service: svc, ValidateOnly: dry})
			if err != nil {
				fail(err)
				return
			}
			o.GetResponse().UnmarshalTo(resp)
		} else {
			svc.Name = parentName + "/services/" + name
			// ponytail: resourceVersion is not checked on replace (last write wins)
			o, err := s.UpdateService(ctx, &runpb.UpdateServiceRequest{Service: svc, ValidateOnly: dry})
			if err != nil {
				fail(err)
				return
			}
			o.GetResponse().UnmarshalTo(resp)
		}
		gapi.JSON(w, toV1(resp))
	}
}

// listProject is the services of a project in one region, or in all regions when region is "".
func (s *Service) listProject(project, region string) []*runpb.Service {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*runpb.Service
	for _, n := range core.SortedKeys(s.svcs) {
		if p, l := splitName(n); p == project && (region == "" || l == region) {
			out = append(out, proto.Clone(s.svcs[n]).(*runpb.Service))
		}
	}
	return out
}
