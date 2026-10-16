// Package appconfig emulates AWS AppConfig and AppConfigData in one service:
// both are REST-JSON signed "appconfig", and AppConfigData owns
// /configurationsessions and /configuration. Applications, environments,
// configuration profiles, hosted configuration versions, deployment
// strategies, deployments that go DEPLOYING → BAKING → COMPLETE and roll back
// when a monitor's alarm fires, extensions as metadata, and configuration
// sessions that serve what is deployed (feature flags in their values form).
package appconfig

import (
	"cmp"
	"encoding/json"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Service is the AppConfig and AppConfigData emulator.
type Service struct {
	hub  *core.Hub
	mux  *http.ServeMux
	stop func()
	mu   sync.Mutex
	st   state
}

type state struct {
	Apps       map[string]*app         `json:"apps"`         // account/region/id
	Strategies map[string]*strategy    `json:"strategies"`   // account/region/id; not the predefined ones
	Extensions map[string]*extension   `json:"extensions"`   // account/region/id
	Assocs     map[string]*association `json:"associations"` // account/region/id
	Sessions   map[string]*session     `json:"sessions"`     // unused poll token → session
}

// The exported types are API shapes; the unexported ones around them add
// what pogi keeps besides.

// Application is an AppConfig application.
type Application struct {
	Id          string
	Name        string
	Description string `json:",omitempty"`
}

type app struct {
	Application
	Tags     map[string]string   `json:"tags,omitempty"`
	Envs     map[string]*env     `json:"envs"`
	Profiles map[string]*profile `json:"profiles"`
}

// Monitor is a CloudWatch alarm that rolls deployments back.
type Monitor struct {
	AlarmArn     string
	AlarmRoleArn string `json:",omitempty"`
}

// Environment is a deployment target of an application.
type Environment struct {
	ApplicationId string
	Id            string
	Name          string
	Description   string `json:",omitempty"`
	State         string
	Monitors      []Monitor
}

type env struct {
	Environment
	Tags        map[string]string `json:"tags,omitempty"`
	Deployments []*deployment     `json:"deployments,omitempty"` // number n at n-1
}

// Validator checks content before it is deployed.
type Validator struct {
	Type    string // JSON_SCHEMA or LAMBDA
	Content string // the schema, or the function ARN
}

// ConfigurationProfile says where a configuration lives and how it is checked.
type ConfigurationProfile struct {
	ApplicationId    string
	Id               string
	Name             string
	Description      string `json:",omitempty"`
	LocationUri      string
	RetrievalRoleArn string      `json:",omitempty"`
	Validators       []Validator `json:",omitempty"`
	Type             string
	KmsKeyArn        string `json:",omitempty"`
	KmsKeyIdentifier string `json:",omitempty"`
}

type profile struct {
	ConfigurationProfile
	Tags     map[string]string `json:"tags,omitempty"`
	Versions []*Hosted         `json:"versions,omitempty"` // oldest first
	Last     int               `json:"last"`               // the last version number given out
}

// Hosted is a hosted configuration version; the API sends its metadata in headers.
type Hosted struct {
	ApplicationId          string
	ConfigurationProfileId string
	VersionNumber          int
	Description            string `json:",omitempty"`
	ContentType            string
	VersionLabel           string `json:",omitempty"`
	KmsKeyArn              string `json:",omitempty"`
	Content                []byte `json:",omitempty"` // left out of list summaries
}

// Strategy is a deployment strategy.
type Strategy struct {
	Id                          string
	Name                        string
	Description                 string `json:",omitempty"`
	DeploymentDurationInMinutes int
	GrowthType                  string
	GrowthFactor                float64
	FinalBakeTimeInMinutes      int
	ReplicateTo                 string
}

type strategy struct {
	Strategy
	Tags map[string]string `json:"tags,omitempty"`
}

// predefined are AWS's own strategies, in every account and region.
var predefined = []Strategy{
	{"AppConfig.AllAtOnce", "AppConfig.AllAtOnce", "Quick", 0, "LINEAR", 100, 10, "NONE"},
	{"AppConfig.Linear50PercentEvery30Seconds", "AppConfig.Linear50PercentEvery30Seconds", "Test/Demo", 1, "LINEAR", 50, 1, "NONE"},
	{"AppConfig.Canary10Percent20Minutes", "AppConfig.Canary10Percent20Minutes", "AWS Recommended", 20, "EXPONENTIAL", 10, 10, "NONE"},
	{"AppConfig.Linear20PercentEvery6Minutes", "AppConfig.Linear20PercentEvery6Minutes", "AWS Recommended", 30, "LINEAR", 20, 30, "NONE"},
}

// Event is one entry of a deployment's event log.
type Event struct {
	EventType   string
	TriggeredBy string
	Description string
	OccurredAt  time.Time
}

// Deployment is a configuration version rolling out to an environment.
type Deployment struct {
	ApplicationId               string
	EnvironmentId               string
	DeploymentStrategyId        string
	ConfigurationProfileId      string
	DeploymentNumber            int
	ConfigurationName           string
	ConfigurationLocationUri    string
	ConfigurationVersion        string
	Description                 string `json:",omitempty"`
	DeploymentDurationInMinutes int
	GrowthType                  string
	GrowthFactor                float64
	FinalBakeTimeInMinutes      int
	State                       string
	EventLog                    []Event `json:",omitempty"` // newest first; left out of list summaries
	PercentageComplete          float64
	StartedAt                   time.Time
	CompletedAt                 time.Time `json:",omitzero"`
	KmsKeyArn                   string    `json:",omitempty"`
	KmsKeyIdentifier            string    `json:",omitempty"`
	VersionLabel                string    `json:",omitempty"`
}

type deployment struct {
	Deployment
	Tags        map[string]string `json:"tags,omitempty"`
	Content     []byte            `json:"content,omitempty"` // what clients get: flag values for feature flags
	ContentType string            `json:"contentType,omitempty"`
}

// Extension is extension metadata; pogi does not run their actions.
type Extension struct {
	Id            string
	Name          string
	VersionNumber int
	Arn           string
	Description   string          `json:",omitempty"`
	Actions       json.RawMessage `json:",omitempty"`
	Parameters    json.RawMessage `json:",omitempty"`
}

type extension struct {
	Extension
	Tags map[string]string `json:"tags,omitempty"`
}

// Association ties an extension to an application, environment or profile.
type Association struct {
	Id                     string
	ExtensionArn           string
	ResourceArn            string
	Arn                    string
	Parameters             map[string]string `json:",omitempty"`
	ExtensionVersionNumber int
}

type association struct {
	Association
	Tags map[string]string `json:"tags,omitempty"`
}

const (
	freeform     = "AWS.Freeform"
	featureFlags = "AWS.AppConfig.FeatureFlags"
	maxContent   = 2 << 20 // the hosted store's default size quota
)

// New creates the AppConfig service and starts its deployment clock.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, mux: http.NewServeMux()}
	s.Reset()
	const (
		app  = "/applications/{ApplicationId}"
		env  = app + "/environments/{EnvironmentId}"
		prof = app + "/configurationprofiles/{ConfigurationProfileId}"
	)
	for _, r := range []struct {
		pattern, op string
		status      int
		h           handler
	}{
		{"POST /applications", "CreateApplication", 201, s.createApplication},
		{"GET /applications", "ListApplications", 200, s.listApplications},
		{"GET " + app, "GetApplication", 200, s.getApplication},
		{"PATCH " + app, "UpdateApplication", 200, s.updateApplication},
		{"DELETE " + app, "DeleteApplication", 204, s.deleteApplication},
		{"POST " + app + "/environments", "CreateEnvironment", 201, s.createEnvironment},
		{"GET " + app + "/environments", "ListEnvironments", 200, s.listEnvironments},
		{"GET " + env, "GetEnvironment", 200, s.getEnvironment},
		{"PATCH " + env, "UpdateEnvironment", 200, s.updateEnvironment},
		{"DELETE " + env, "DeleteEnvironment", 204, s.deleteEnvironment},
		{"POST " + app + "/configurationprofiles", "CreateConfigurationProfile", 201, s.createProfile},
		{"GET " + app + "/configurationprofiles", "ListConfigurationProfiles", 200, s.listProfiles},
		{"GET " + prof, "GetConfigurationProfile", 200, s.getProfile},
		{"PATCH " + prof, "UpdateConfigurationProfile", 200, s.updateProfile},
		{"DELETE " + prof, "DeleteConfigurationProfile", 204, s.deleteProfile},
		{"POST " + prof + "/validators", "ValidateConfiguration", 204, s.validateConfiguration},
		{"POST " + prof + "/hostedconfigurationversions", "CreateHostedConfigurationVersion", 201, s.createHosted},
		{"GET " + prof + "/hostedconfigurationversions", "ListHostedConfigurationVersions", 200, s.listHosted},
		{"GET " + prof + "/hostedconfigurationversions/{VersionNumber}", "GetHostedConfigurationVersion", 200, s.getHosted},
		{"DELETE " + prof + "/hostedconfigurationversions/{VersionNumber}", "DeleteHostedConfigurationVersion", 204, s.deleteHosted},
		{"POST /deploymentstrategies", "CreateDeploymentStrategy", 201, s.createStrategy},
		{"GET /deploymentstrategies", "ListDeploymentStrategies", 200, s.listStrategies},
		{"GET /deploymentstrategies/{DeploymentStrategyId}", "GetDeploymentStrategy", 200, s.getStrategy},
		{"PATCH /deploymentstrategies/{DeploymentStrategyId}", "UpdateDeploymentStrategy", 200, s.updateStrategy},
		{"DELETE /deploymentstrategies/{DeploymentStrategyId}", "DeleteDeploymentStrategy", 204, s.deleteStrategy},
		{"DELETE /deployementstrategies/{DeploymentStrategyId}", "DeleteDeploymentStrategy", 204, s.deleteStrategy}, // sic: the SDKs' path
		{"POST " + env + "/deployments", "StartDeployment", 201, s.startDeployment},
		{"GET " + env + "/deployments", "ListDeployments", 200, s.listDeployments},
		{"GET " + env + "/deployments/{DeploymentNumber}", "GetDeployment", 200, s.getDeployment},
		{"DELETE " + env + "/deployments/{DeploymentNumber}", "StopDeployment", 202, s.stopDeployment},
		{"GET " + env + "/configurations/{Configuration}", "GetConfiguration", 200, s.getConfiguration},
		{"POST /extensions", "CreateExtension", 201, s.createExtension},
		{"GET /extensions", "ListExtensions", 200, s.listExtensions},
		{"GET /extensions/{ExtensionIdentifier}", "GetExtension", 200, s.getExtension},
		{"PATCH /extensions/{ExtensionIdentifier}", "UpdateExtension", 200, s.updateExtension},
		{"DELETE /extensions/{ExtensionIdentifier}", "DeleteExtension", 204, s.deleteExtension},
		{"POST /extensionassociations", "CreateExtensionAssociation", 201, s.createAssociation},
		{"GET /extensionassociations", "ListExtensionAssociations", 200, s.listAssociations},
		{"GET /extensionassociations/{ExtensionAssociationId}", "GetExtensionAssociation", 200, s.getAssociation},
		{"PATCH /extensionassociations/{ExtensionAssociationId}", "UpdateExtensionAssociation", 200, s.updateAssociation},
		{"DELETE /extensionassociations/{ExtensionAssociationId}", "DeleteExtensionAssociation", 204, s.deleteAssociation},
		{"GET /tags/{ResourceArn}", "ListTagsForResource", 200, s.listTags},
		{"POST /tags/{ResourceArn}", "TagResource", 204, s.tagResource},
		{"DELETE /tags/{ResourceArn}", "UntagResource", 204, s.untagResource},
		{"POST /configurationsessions", "StartConfigurationSession", 201, s.startSession},
		{"GET /configuration", "GetLatestConfiguration", 200, s.latest},
		{"/", "Unknown", 0, func(c *core.Ctx, r *http.Request) (any, error) {
			return nil, core.Errorf(501, "NotImplemented", "pogi does not implement AppConfig %s %s", r.Method, r.URL.Path)
		}},
	} {
		s.route(r.pattern, r.op, r.status, r.h)
	}
	s.stop = core.Every(time.Second, s.Tick)
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "appconfig", Cloud: core.AWS, Title: "AppConfig", Signing: []string{"appconfig"}, Protocol: "rest-json"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

// Close stops the deployment clock.
func (s *Service) Close() error { s.stop(); return nil }

func newState() state {
	return state{Apps: map[string]*app{}, Strategies: map[string]*strategy{}, Extensions: map[string]*extension{},
		Assocs: map[string]*association{}, Sessions: map[string]*session{}}
}

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = newState()
	s.mu.Unlock()
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

func (s *Service) Load(b []byte) error {
	st := newState()
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

type handler func(c *core.Ctx, r *http.Request) (any, error)

// raw is a response whose body is configuration content, with its metadata in headers.
type raw struct {
	header http.Header
	body   []byte
}

// route serves one operation; status is its success status.
func (s *Service) route(pattern, op string, status int, h handler) {
	s.mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		c := core.C(r)
		c.Op = op
		out, err := any(nil), c.Intercept()
		if err == nil {
			out, err = h(c, r)
		}
		if err != nil {
			core.RESTJSONError(w, c, err)
			return
		}
		if o, ok := out.(raw); ok {
			maps.Copy(w.Header(), o.header)
			w.Header().Set("x-amzn-RequestId", c.RequestID)
			w.WriteHeader(status)
			w.Write(o.body)
			return
		}
		if status == http.StatusNoContent {
			w.Header().Set("x-amzn-RequestId", c.RequestID)
			w.WriteHeader(status)
			return
		}
		core.RESTJSON(w, c, status, out)
	})
}

func decode(r *http.Request, v any) error {
	if b := core.Body(r); len(b) > 0 && json.Unmarshal(b, v) != nil {
		return badRequest("The request body is not valid JSON.")
	}
	return nil
}

func notFound(format string, a ...any) error {
	return core.Errorf(404, "ResourceNotFoundException", format, a...)
}

func badRequest(format string, a ...any) error {
	return core.Errorf(400, "BadRequestException", format, a...)
}

func conflict(format string, a ...any) error {
	return core.Errorf(409, "ConflictException", format, a...)
}

func scope(c *core.Ctx) string { return c.Account + "/" + c.Region + "/" }

// newID makes an id the way AppConfig does: 7 lower-case letters and digits.
func newID() string { return core.RandString(7, "abcdefghijklmnopqrstuvwxyz0123456789") }

// set copies an optional update field.
func set[T any](dst *T, v *T) {
	if v != nil {
		*dst = *v
	}
}

// find looks an item up by id, then by name (AppConfigData and GetConfiguration take either).
func find[T any](m map[string]*T, prefix, ident string, name func(*T) string) *T {
	if v := m[prefix+ident]; v != nil {
		return v
	}
	for _, k := range core.SortedKeys(m) {
		if strings.HasPrefix(k, prefix) && name(m[k]) == ident {
			return m[k]
		}
	}
	return nil
}

// page cuts a list at next_token (an offset) to max_results (50 at most).
func page[T any](items []T, r *http.Request) map[string]any {
	q := r.URL.Query()
	i, _ := strconv.Atoi(q.Get("next_token"))
	i = min(max(i, 0), len(items))
	n, err := strconv.Atoi(q.Get("max_results"))
	if err != nil || n <= 0 || n > 50 {
		n = 50
	}
	out := map[string]any{"Items": items[i:min(i+n, len(items))]}
	if i+n < len(items) {
		out["NextToken"] = strconv.Itoa(i + n)
	}
	return out
}

// kmsARN turns a key id, alias or ARN into an ARN.
func kmsARN(c *core.Ctx, id string) string {
	switch {
	case id == "" || strings.HasPrefix(id, "arn:"):
		return id
	case strings.HasPrefix(id, "alias/"):
		return c.ARN("kms", id)
	}
	return c.ARN("kms", "key/"+id)
}

// ---- applications, environments ----

// app finds an application by id or name; lock held.
func (s *Service) app(c *core.Ctx, ident string) (*app, error) {
	if a := find(s.st.Apps, scope(c), ident, func(a *app) string { return a.Name }); a != nil {
		return a, nil
	}
	return nil, notFound("Application %s not found.", ident)
}

// env finds an application's environment by id or name; lock held.
func (s *Service) env(c *core.Ctx, appID, envID string) (*app, *env, error) {
	a, err := s.app(c, appID)
	if err != nil {
		return nil, nil, err
	}
	if e := find(a.Envs, "", envID, func(e *env) string { return e.Name }); e != nil {
		return a, e, nil
	}
	return nil, nil, notFound("Environment %s not found in application %s.", envID, a.Id)
}

func (s *Service) envOf(c *core.Ctx, r *http.Request) (*app, *env, error) {
	return s.env(c, r.PathValue("ApplicationId"), r.PathValue("EnvironmentId"))
}

func (s *Service) createApplication(c *core.Ctx, r *http.Request) (any, error) {
	var in struct {
		Name, Description string
		Tags              map[string]string
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	if in.Name == "" {
		return nil, badRequest("Name is required.")
	}
	a := &app{Application: Application{newID(), in.Name, in.Description}, Tags: in.Tags, Envs: map[string]*env{}, Profiles: map[string]*profile{}}
	s.mu.Lock()
	s.st.Apps[scope(c)+a.Id] = a
	s.mu.Unlock()
	return a.Application, nil
}

func (s *Service) listApplications(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []Application{}
	for _, k := range core.SortedKeys(s.st.Apps) {
		if strings.HasPrefix(k, scope(c)) {
			out = append(out, s.st.Apps[k].Application)
		}
	}
	return page(out, r), nil
}

func (s *Service) getApplication(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.app(c, r.PathValue("ApplicationId"))
	if err != nil {
		return nil, err
	}
	return a.Application, nil
}

func (s *Service) updateApplication(c *core.Ctx, r *http.Request) (any, error) {
	var in struct{ Name, Description *string }
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.app(c, r.PathValue("ApplicationId"))
	if err != nil {
		return nil, err
	}
	set(&a.Name, in.Name)
	set(&a.Description, in.Description)
	return a.Application, nil
}

// deleteApplication takes the application's environments, profiles and deployments with it.
func (s *Service) deleteApplication(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.app(c, r.PathValue("ApplicationId"))
	if err != nil {
		return nil, err
	}
	delete(s.st.Apps, scope(c)+a.Id)
	return nil, nil
}

// monitors checks an environment's alarms; it never returns nil.
func monitors(m []Monitor) ([]Monitor, error) {
	if len(m) > 5 {
		return nil, badRequest("An environment takes at most 5 monitors.")
	}
	for _, x := range m {
		if !strings.HasPrefix(x.AlarmArn, "arn:") {
			return nil, badRequest("Monitor AlarmArn %q is not an ARN.", x.AlarmArn)
		}
	}
	return append([]Monitor{}, m...), nil
}

func (s *Service) createEnvironment(c *core.Ctx, r *http.Request) (any, error) {
	var in struct {
		Name, Description string
		Monitors          []Monitor
		Tags              map[string]string
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	m, err := monitors(in.Monitors)
	if err != nil {
		return nil, err
	}
	if in.Name == "" {
		return nil, badRequest("Name is required.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.app(c, r.PathValue("ApplicationId"))
	if err != nil {
		return nil, err
	}
	e := &env{Environment: Environment{a.Id, newID(), in.Name, in.Description, "READY_FOR_DEPLOYMENT", m}, Tags: in.Tags}
	a.Envs[e.Id] = e
	return e.Environment, nil
}

func (s *Service) listEnvironments(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.app(c, r.PathValue("ApplicationId"))
	if err != nil {
		return nil, err
	}
	out := []Environment{}
	for _, k := range core.SortedKeys(a.Envs) {
		out = append(out, a.Envs[k].Environment)
	}
	return page(out, r), nil
}

func (s *Service) getEnvironment(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, e, err := s.envOf(c, r)
	if err != nil {
		return nil, err
	}
	return e.Environment, nil
}

func (s *Service) updateEnvironment(c *core.Ctx, r *http.Request) (any, error) {
	var in struct {
		Name, Description *string
		Monitors          *[]Monitor
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	if in.Monitors != nil {
		m, err := monitors(*in.Monitors)
		if err != nil {
			return nil, err
		}
		in.Monitors = &m
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, e, err := s.envOf(c, r)
	if err != nil {
		return nil, err
	}
	set(&e.Name, in.Name)
	set(&e.Description, in.Description)
	set(&e.Monitors, in.Monitors)
	return e.Environment, nil
}

func (s *Service) deleteEnvironment(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, e, err := s.envOf(c, r)
	if err != nil {
		return nil, err
	}
	delete(a.Envs, e.Id)
	return nil, nil
}

// ---- configuration profiles, hosted versions ----

// profile finds an application's configuration profile by id or name; lock held.
func (s *Service) profile(c *core.Ctx, appID, profileID string) (*app, *profile, error) {
	a, err := s.app(c, appID)
	if err != nil {
		return nil, nil, err
	}
	if p := find(a.Profiles, "", profileID, func(p *profile) string { return p.Name }); p != nil {
		return a, p, nil
	}
	return nil, nil, notFound("Configuration profile %s not found in application %s.", profileID, a.Id)
}

func (s *Service) profileOf(c *core.Ctx, r *http.Request) (*app, *profile, error) {
	return s.profile(c, r.PathValue("ApplicationId"), r.PathValue("ConfigurationProfileId"))
}

func checkValidators(vs []Validator) error {
	if len(vs) > 2 {
		return badRequest("A configuration profile takes at most 2 validators.")
	}
	for _, v := range vs {
		switch {
		case v.Type == "JSON_SCHEMA" && !json.Valid([]byte(v.Content)):
			return badRequest("The JSON_SCHEMA validator's content is not valid JSON.")
		case v.Type == "LAMBDA" && !strings.HasPrefix(v.Content, "arn:"):
			return badRequest("The LAMBDA validator's content must be a function ARN.")
		case v.Type != "JSON_SCHEMA" && v.Type != "LAMBDA":
			return badRequest("Validator type %s is not JSON_SCHEMA or LAMBDA.", v.Type)
		}
	}
	return nil
}

func (s *Service) createProfile(c *core.Ctx, r *http.Request) (any, error) {
	var in struct {
		ConfigurationProfile
		Tags map[string]string
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	p := in.ConfigurationProfile
	p.Type = cmp.Or(p.Type, freeform)
	loc, _, _ := strings.Cut(p.LocationUri, "://")
	switch {
	case p.Name == "":
		return nil, badRequest("Name is required.")
	case p.LocationUri != "hosted" && !slices.Contains([]string{"ssm-parameter", "ssm-document", "secretsmanager", "s3"}, loc):
		return nil, badRequest("LocationUri %q is not hosted, ssm-parameter://, ssm-document://, secretsmanager:// or s3://.", p.LocationUri)
	case p.Type != freeform && p.Type != featureFlags:
		return nil, badRequest("Type %s is not %s or %s.", p.Type, freeform, featureFlags)
	}
	if err := checkValidators(p.Validators); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.app(c, r.PathValue("ApplicationId"))
	if err != nil {
		return nil, err
	}
	p.ApplicationId, p.Id, p.KmsKeyArn = a.Id, newID(), kmsARN(c, p.KmsKeyIdentifier)
	a.Profiles[p.Id] = &profile{ConfigurationProfile: p, Tags: in.Tags}
	return p, nil
}

type profileSummary struct {
	ApplicationId, Id, Name, LocationUri string
	ValidatorTypes                       []string `json:",omitempty"`
	Type                                 string
}

func (s *Service) listProfiles(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.app(c, r.PathValue("ApplicationId"))
	if err != nil {
		return nil, err
	}
	out := []profileSummary{}
	for _, k := range core.SortedKeys(a.Profiles) {
		p := a.Profiles[k]
		if t := r.URL.Query().Get("type"); t != "" && t != p.Type {
			continue
		}
		sum := profileSummary{p.ApplicationId, p.Id, p.Name, p.LocationUri, nil, p.Type}
		for _, v := range p.Validators {
			sum.ValidatorTypes = append(sum.ValidatorTypes, v.Type)
		}
		out = append(out, sum)
	}
	return page(out, r), nil
}

func (s *Service) getProfile(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, p, err := s.profileOf(c, r)
	if err != nil {
		return nil, err
	}
	return p.ConfigurationProfile, nil
}

func (s *Service) updateProfile(c *core.Ctx, r *http.Request) (any, error) {
	var in struct {
		Name, Description, RetrievalRoleArn, KmsKeyIdentifier *string
		Validators                                            *[]Validator
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	if in.Validators != nil {
		if err := checkValidators(*in.Validators); err != nil {
			return nil, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, p, err := s.profileOf(c, r)
	if err != nil {
		return nil, err
	}
	set(&p.Name, in.Name)
	set(&p.Description, in.Description)
	set(&p.RetrievalRoleArn, in.RetrievalRoleArn)
	set(&p.Validators, in.Validators)
	if in.KmsKeyIdentifier != nil {
		p.KmsKeyIdentifier, p.KmsKeyArn = *in.KmsKeyIdentifier, kmsARN(c, *in.KmsKeyIdentifier)
	}
	return p.ConfigurationProfile, nil
}

func (s *Service) deleteProfile(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, p, err := s.profileOf(c, r)
	if err != nil {
		return nil, err
	}
	delete(a.Profiles, p.Id)
	return nil, nil
}

// version finds a hosted version by number or label, the newest first.
func (p *profile) version(ref string) (*Hosted, error) {
	for _, v := range slices.Backward(p.Versions) {
		if strconv.Itoa(v.VersionNumber) == ref || v.VersionLabel == ref {
			return v, nil
		}
	}
	return nil, notFound("Hosted configuration version %s not found in configuration profile %s.", ref, p.Id)
}

func (v *Hosted) raw() raw {
	h := http.Header{}
	h.Set("Application-Id", v.ApplicationId)
	h.Set("Configuration-Profile-Id", v.ConfigurationProfileId)
	h.Set("Version-Number", strconv.Itoa(v.VersionNumber))
	h.Set("Content-Type", v.ContentType)
	for k, val := range map[string]string{"Description": v.Description, "VersionLabel": v.VersionLabel, "KmsKeyArn": v.KmsKeyArn} {
		if val != "" {
			h.Set(k, val)
		}
	}
	return raw{h, v.Content}
}

func (s *Service) createHosted(c *core.Ctx, r *http.Request) (any, error) {
	body, label := core.Body(r), r.Header.Get("VersionLabel")
	switch {
	case len(body) > maxContent:
		return nil, core.Errorf(413, "PayloadTooLargeException", "The configuration is %d bytes, over the limit of %d.", len(body), maxContent)
	case r.Header.Get("Content-Type") == "":
		return nil, badRequest("ContentType is required.")
	case label != "" && strings.Trim(label, "0123456789") == "":
		return nil, badRequest("VersionLabel %s must contain a non-numeric character.", label)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, p, err := s.profileOf(c, r)
	if err != nil {
		return nil, err
	}
	if p.LocationUri != "hosted" {
		return nil, badRequest("Configuration profile %s is not stored in the hosted configuration store.", p.Id)
	}
	if p.Type == featureFlags {
		if _, err := flagValues(body); err != nil {
			return nil, badRequest("The feature flags are not valid: %v", err)
		}
	}
	latest := 0
	if n := len(p.Versions); n > 0 {
		latest = p.Versions[n-1].VersionNumber
	}
	if lv := r.Header.Get("Latest-Version-Number"); lv != "" && lv != strconv.Itoa(latest) {
		return nil, conflict("LatestVersionNumber %s is not the latest version, %d.", lv, latest)
	}
	p.Last++
	v := &Hosted{p.ApplicationId, p.Id, p.Last, r.Header.Get("Description"), r.Header.Get("Content-Type"), label, p.KmsKeyArn, body}
	p.Versions = append(p.Versions, v)
	return v.raw(), nil
}

func (s *Service) listHosted(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, p, err := s.profileOf(c, r)
	if err != nil {
		return nil, err
	}
	label := r.URL.Query().Get("version_label")
	prefix, wild := strings.CutSuffix(label, "*")
	out := []Hosted{}
	for _, v := range slices.Backward(p.Versions) {
		if label == "" || v.VersionLabel == label || wild && strings.HasPrefix(v.VersionLabel, prefix) {
			sum := *v
			sum.Content = nil
			out = append(out, sum)
		}
	}
	return page(out, r), nil
}

func (s *Service) getHosted(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, p, err := s.profileOf(c, r)
	if err != nil {
		return nil, err
	}
	n := r.PathValue("VersionNumber")
	for _, v := range p.Versions {
		if strconv.Itoa(v.VersionNumber) == n {
			return v.raw(), nil
		}
	}
	return nil, notFound("Hosted configuration version %s not found in configuration profile %s.", n, p.Id)
}

func (s *Service) deleteHosted(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, p, err := s.profileOf(c, r)
	if err != nil {
		return nil, err
	}
	n := r.PathValue("VersionNumber")
	i := slices.IndexFunc(p.Versions, func(v *Hosted) bool { return strconv.Itoa(v.VersionNumber) == n })
	if i < 0 {
		return nil, notFound("Hosted configuration version %s not found in configuration profile %s.", n, p.Id)
	}
	p.Versions = slices.Delete(p.Versions, i, i+1)
	return nil, nil
}

// ---- deployment strategies ----

func (st Strategy) check() error {
	switch {
	case st.Name == "":
		return badRequest("Name is required.")
	case st.DeploymentDurationInMinutes < 0 || st.DeploymentDurationInMinutes > 1440 || st.FinalBakeTimeInMinutes < 0 || st.FinalBakeTimeInMinutes > 1440:
		return badRequest("Durations are between 0 and 1440 minutes.")
	case st.GrowthFactor < 1 || st.GrowthFactor > 100:
		return badRequest("GrowthFactor %v is not between 1 and 100.", st.GrowthFactor)
	case st.GrowthType != "LINEAR" && st.GrowthType != "EXPONENTIAL":
		return badRequest("GrowthType %s is not LINEAR or EXPONENTIAL.", st.GrowthType)
	case st.ReplicateTo != "NONE" && st.ReplicateTo != "SSM_DOCUMENT":
		return badRequest("ReplicateTo %s is not NONE or SSM_DOCUMENT.", st.ReplicateTo)
	}
	return nil
}

// strategy finds a predefined or custom strategy by id or name; lock held.
func (s *Service) strategy(c *core.Ctx, ident string) (Strategy, *strategy, error) {
	if i := slices.IndexFunc(predefined, func(st Strategy) bool { return st.Id == ident }); i >= 0 {
		return predefined[i], nil, nil
	}
	if st := find(s.st.Strategies, scope(c), ident, func(st *strategy) string { return st.Name }); st != nil {
		return st.Strategy, st, nil
	}
	return Strategy{}, nil, notFound("Deployment strategy %s not found.", ident)
}

// custom finds a strategy pogi may change; lock held.
func (s *Service) custom(c *core.Ctx, ident string) (*strategy, error) {
	_, st, err := s.strategy(c, ident)
	if err == nil && st == nil {
		err = badRequest("Predefined deployment strategy %s cannot be changed or deleted.", ident)
	}
	return st, err
}

func (s *Service) createStrategy(c *core.Ctx, r *http.Request) (any, error) {
	var in struct {
		Strategy
		Tags map[string]string
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	st := in.Strategy
	st.Id, st.GrowthType, st.ReplicateTo = newID(), cmp.Or(st.GrowthType, "LINEAR"), cmp.Or(st.ReplicateTo, "NONE")
	if err := st.check(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.st.Strategies[scope(c)+st.Id] = &strategy{st, in.Tags}
	s.mu.Unlock()
	return st, nil
}

func (s *Service) listStrategies(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := slices.Clone(predefined)
	for _, k := range core.SortedKeys(s.st.Strategies) {
		if strings.HasPrefix(k, scope(c)) {
			out = append(out, s.st.Strategies[k].Strategy)
		}
	}
	return page(out, r), nil
}

func (s *Service) getStrategy(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, _, err := s.strategy(c, r.PathValue("DeploymentStrategyId"))
	if err != nil {
		return nil, err
	}
	return st, nil
}

func (s *Service) updateStrategy(c *core.Ctx, r *http.Request) (any, error) {
	var in struct {
		Description                                         *string
		DeploymentDurationInMinutes, FinalBakeTimeInMinutes *int
		GrowthFactor                                        *float64
		GrowthType                                          *string
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.custom(c, r.PathValue("DeploymentStrategyId"))
	if err != nil {
		return nil, err
	}
	next := st.Strategy
	set(&next.Description, in.Description)
	set(&next.DeploymentDurationInMinutes, in.DeploymentDurationInMinutes)
	set(&next.FinalBakeTimeInMinutes, in.FinalBakeTimeInMinutes)
	set(&next.GrowthFactor, in.GrowthFactor)
	set(&next.GrowthType, in.GrowthType)
	if err := next.check(); err != nil {
		return nil, err
	}
	st.Strategy = next
	return next, nil
}

func (s *Service) deleteStrategy(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.custom(c, r.PathValue("DeploymentStrategyId"))
	if err != nil {
		return nil, err
	}
	delete(s.st.Strategies, scope(c)+st.Id)
	return nil, nil
}

// ---- deployments ----

// current is the environment's deployment in progress, or nil.
func (e *env) current() *deployment {
	if n := len(e.Deployments); n > 0 && (e.Deployments[n-1].State == "DEPLOYING" || e.Deployments[n-1].State == "BAKING") {
		return e.Deployments[n-1]
	}
	return nil
}

// live is what clients of a profile get: its newest deployment that is
// complete or rolling out.
func (e *env) live(profileID string) *deployment {
	for _, d := range slices.Backward(e.Deployments) {
		if d.ConfigurationProfileId == profileID && (d.State == "COMPLETE" || d.State == "DEPLOYING" || d.State == "BAKING") {
			return d
		}
	}
	return nil
}

func (e *env) deployment(n string) (*deployment, error) {
	i, err := strconv.Atoi(n)
	if err != nil || i < 1 || i > len(e.Deployments) {
		return nil, notFound("Deployment %s not found in environment %s.", n, e.Id)
	}
	return e.Deployments[i-1], nil
}

// busy refuses a new deployment while one is in progress, or when the caller
// expected another latest deployment.
func (e *env) busy(latest *int) error {
	if d := e.current(); d != nil {
		return conflict("Deployment %d to environment %s is in progress.", d.DeploymentNumber, e.Id)
	}
	if latest != nil && *latest != len(e.Deployments) {
		return conflict("LatestDeploymentNumber %d is not the latest deployment, %d.", *latest, len(e.Deployments))
	}
	return nil
}

// event logs an event, newest first, in a new slice: copies handed out keep theirs.
func (d *deployment) event(kind, by, desc string, at time.Time) {
	d.EventLog = append([]Event{{kind, by, desc, at}}, d.EventLog...)
}

// phase is how long a deployment phase (deploying, baking) lasts: a second
// when the strategy gives it any time.
// ponytail: growth isn't simulated: every client gets the new content at once
// and phases take seconds whatever the minutes; Tick(now) fast-forwards.
func phase(minutes int) time.Duration {
	if minutes > 0 {
		return time.Second
	}
	return 0
}

// advance moves a deployment through its phases up to now.
func (e *env) advance(d *deployment, now time.Time) {
	deployed := d.StartedAt.Add(phase(d.DeploymentDurationInMinutes))
	if d.State == "DEPLOYING" && !now.Before(deployed) {
		d.State, d.PercentageComplete = "BAKING", 100
		d.event("PERCENTAGE_UPDATED", "APPCONFIG", "Configuration available to 100.00% of clients", deployed)
		d.event("BAKE_TIME_STARTED", "APPCONFIG", "Deployment bake time started", deployed)
	}
	if baked := deployed.Add(phase(d.FinalBakeTimeInMinutes)); d.State == "BAKING" && !now.Before(baked) {
		d.State, d.CompletedAt, e.State = "COMPLETE", baked, "READY_FOR_DEPLOYMENT"
		d.event("DEPLOYMENT_COMPLETED", "APPCONFIG", "Deployment completed", baked)
	}
}

func (e *env) rollBack(d *deployment, by, why string, now time.Time) {
	d.State, d.CompletedAt, e.State = "ROLLED_BACK", now, "ROLLED_BACK"
	d.event("ROLLBACK_STARTED", by, why, now)
	d.event("ROLLBACK_COMPLETED", "APPCONFIG", "Rollback completed", now)
}

// Tick advances deployments, rolls back those whose monitor alarms are in
// ALARM and forgets expired poll tokens. Tests call it to fast-forward.
func (s *Service) Tick(now time.Time) {
	type watch struct {
		d        *deployment
		monitors []Monitor
	}
	var watched []watch
	s.mu.Lock()
	for _, a := range s.st.Apps {
		for _, e := range a.Envs {
			if d := e.current(); d != nil && len(e.Monitors) > 0 {
				watched = append(watched, watch{d, slices.Clone(e.Monitors)})
			}
		}
	}
	s.mu.Unlock()
	fired := map[*deployment]string{}
	for _, w := range watched { // CloudWatch is asked outside the lock
		if arn := s.alarming(w.monitors); arn != "" {
			fired[w.d] = arn
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, a := range s.st.Apps {
		for _, e := range a.Envs {
			switch d := e.current(); {
			case d == nil:
			case fired[d] != "":
				e.rollBack(d, "CLOUDWATCH_ALARM", "Alarm "+fired[d]+" is in ALARM", now)
			default:
				e.advance(d, now)
			}
		}
	}
	for tok, ss := range s.st.Sessions {
		if now.Sub(ss.Issued) > tokenTTL {
			delete(s.st.Sessions, tok)
		}
	}
}

// alarming returns the first monitor whose CloudWatch alarm is in ALARM.
func (s *Service) alarming(ms []Monitor) string {
	for _, m := range ms {
		_, region, account, res, _ := core.ParseARN(m.AlarmArn)
		name, ok := strings.CutPrefix(res, "alarm:")
		var out struct{ MetricAlarms, CompositeAlarms []struct{ StateValue string } }
		if !ok || s.hub.Call(account, region, "GraniteServiceVersion20100801", "DescribeAlarms",
			map[string]any{"AlarmNames": []string{name}, "AlarmTypes": []string{"MetricAlarm", "CompositeAlarm"}}, &out) != nil {
			continue
		}
		for _, a := range slices.Concat(out.MetricAlarms, out.CompositeAlarms) {
			if a.StateValue == "ALARM" {
				return m.AlarmArn
			}
		}
	}
	return ""
}

func (s *Service) startDeployment(c *core.Ctx, r *http.Request) (any, error) {
	var in struct {
		DeploymentStrategyId, ConfigurationProfileId, ConfigurationVersion, Description, KmsKeyIdentifier string
		LatestDeploymentNumber                                                                            *int
		Tags                                                                                              map[string]string
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	s.mu.Lock()
	_, e, err := s.envOf(c, r)
	var p *profile
	if err == nil {
		_, p, err = s.profile(c, r.PathValue("ApplicationId"), in.ConfigurationProfileId)
	}
	var st Strategy
	if err == nil {
		st, _, err = s.strategy(c, in.DeploymentStrategyId)
	}
	if err == nil {
		err = e.busy(in.LatestDeploymentNumber)
	}
	var prof ConfigurationProfile
	if err == nil {
		prof = p.ConfigurationProfile
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	// content and validators may call other services: outside the lock
	got, err := s.fetch(c, prof, in.ConfigurationVersion)
	if err != nil {
		return nil, err
	}
	if err := s.check(c, prof, in.ConfigurationVersion, got.content); err != nil {
		return nil, err
	}
	if prof.Type == featureFlags {
		if got.content, err = flagValues(got.content); err != nil {
			return nil, badRequest("The feature flags are not valid: %v", err)
		}
		got.contentType = "application/json"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, e, err = s.envOf(c, r); err == nil { // it may have gone meanwhile
		err = e.busy(in.LatestDeploymentNumber)
	}
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	d := &deployment{Deployment: Deployment{
		ApplicationId: e.ApplicationId, EnvironmentId: e.Id, DeploymentStrategyId: st.Id, ConfigurationProfileId: prof.Id,
		DeploymentNumber: len(e.Deployments) + 1, ConfigurationName: prof.Name, ConfigurationLocationUri: prof.LocationUri,
		ConfigurationVersion: in.ConfigurationVersion, Description: in.Description, DeploymentDurationInMinutes: st.DeploymentDurationInMinutes,
		GrowthType: st.GrowthType, GrowthFactor: st.GrowthFactor, FinalBakeTimeInMinutes: st.FinalBakeTimeInMinutes, State: "DEPLOYING",
		StartedAt: now, KmsKeyIdentifier: in.KmsKeyIdentifier, KmsKeyArn: cmp.Or(kmsARN(c, in.KmsKeyIdentifier), prof.KmsKeyArn),
		VersionLabel: got.label,
	}, Tags: in.Tags, Content: got.content, ContentType: got.contentType}
	d.event("DEPLOYMENT_STARTED", "USER", "Deployment started", now)
	e.Deployments = append(e.Deployments, d)
	e.State = "DEPLOYING"
	return d.Deployment, nil
}

func (s *Service) listDeployments(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, e, err := s.envOf(c, r)
	if err != nil {
		return nil, err
	}
	out := []Deployment{}
	for _, d := range slices.Backward(e.Deployments) {
		sum := d.Deployment
		sum.EventLog = nil
		out = append(out, sum)
	}
	return page(out, r), nil
}

func (s *Service) getDeployment(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, e, err := s.envOf(c, r)
	if err != nil {
		return nil, err
	}
	d, err := e.deployment(r.PathValue("DeploymentNumber"))
	if err != nil {
		return nil, err
	}
	return d.Deployment, nil
}

// stopDeployment rolls back a deployment in progress or, with Allow-Revert,
// reverts a complete one: clients go back to the previous content.
func (s *Service) stopDeployment(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, e, err := s.envOf(c, r)
	if err != nil {
		return nil, err
	}
	d, err := e.deployment(r.PathValue("DeploymentNumber"))
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	switch {
	case d == e.current():
		e.rollBack(d, "USER", "Deployment stopped", now)
	case d.State == "COMPLETE" && r.Header.Get("Allow-Revert") == "true":
		d.State, e.State = "REVERTED", "REVERTED"
		d.event("REVERT_COMPLETED", "USER", "Deployment reverted", now)
	default:
		return nil, badRequest("Deployment %d is %s: only one in progress can be stopped, or a complete one reverted with AllowRevert.", d.DeploymentNumber, d.State)
	}
	return d.Deployment, nil
}

// ---- extensions (metadata) ----

// extension finds an extension by id, name or ARN; lock held.
func (s *Service) extension(c *core.Ctx, ident string) (*extension, error) {
	if _, _, _, res, ok := core.ParseARN(ident); ok { // extension/{id}/{version}
		if p := strings.Split(res, "/"); len(p) > 1 {
			ident = p[1]
		}
	}
	if x := find(s.st.Extensions, scope(c), ident, func(x *extension) string { return x.Name }); x != nil {
		return x, nil
	}
	return nil, notFound("Extension %s not found.", ident)
}

// createExtension makes an extension, or a new version of the one with that name.
// ponytail: only the latest version of an extension is kept.
func (s *Service) createExtension(c *core.Ctx, r *http.Request) (any, error) {
	var in struct {
		Name, Description   string
		Actions, Parameters json.RawMessage
		Tags                map[string]string
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	if in.Name == "" {
		return nil, badRequest("Name is required.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	x, _ := s.extension(c, in.Name)
	switch lv := r.Header.Get("Latest-Version-Number"); {
	case x == nil:
		x = &extension{Extension: Extension{Id: newID(), Name: in.Name}, Tags: in.Tags}
		s.st.Extensions[scope(c)+x.Id] = x
	case lv != "" && lv != strconv.Itoa(x.VersionNumber):
		return nil, conflict("LatestVersionNumber %s is not the latest version of %s, %d.", lv, x.Name, x.VersionNumber)
	}
	x.VersionNumber++
	x.Description, x.Actions, x.Parameters = in.Description, in.Actions, in.Parameters
	x.Arn = c.ARN("appconfig", "extension/"+x.Id+"/"+strconv.Itoa(x.VersionNumber))
	return x.Extension, nil
}

func (s *Service) listExtensions(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []Extension{}
	for _, k := range core.SortedKeys(s.st.Extensions) {
		x := s.st.Extensions[k]
		if strings.HasPrefix(k, scope(c)) && cmp.Or(r.URL.Query().Get("name"), x.Name) == x.Name {
			sum := x.Extension
			sum.Actions, sum.Parameters = nil, nil
			out = append(out, sum)
		}
	}
	return page(out, r), nil
}

func (s *Service) getExtension(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	x, err := s.extension(c, r.PathValue("ExtensionIdentifier"))
	if err != nil {
		return nil, err
	}
	return x.Extension, nil
}

func (s *Service) updateExtension(c *core.Ctx, r *http.Request) (any, error) {
	var in struct {
		Description         *string
		Actions, Parameters json.RawMessage
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	x, err := s.extension(c, r.PathValue("ExtensionIdentifier"))
	if err != nil {
		return nil, err
	}
	set(&x.Description, in.Description)
	if in.Actions != nil {
		x.Actions = in.Actions
	}
	if in.Parameters != nil {
		x.Parameters = in.Parameters
	}
	return x.Extension, nil
}

func (s *Service) deleteExtension(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	x, err := s.extension(c, r.PathValue("ExtensionIdentifier"))
	if err != nil {
		return nil, err
	}
	delete(s.st.Extensions, scope(c)+x.Id)
	return nil, nil
}

func (s *Service) association(c *core.Ctx, id string) (*association, error) {
	if as := s.st.Assocs[scope(c)+id]; as != nil {
		return as, nil
	}
	return nil, notFound("Extension association %s not found.", id)
}

func (s *Service) createAssociation(c *core.Ctx, r *http.Request) (any, error) {
	var in struct {
		ExtensionIdentifier, ResourceIdentifier string
		ExtensionVersionNumber                  int
		Parameters, Tags                        map[string]string
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	x, err := s.extension(c, in.ExtensionIdentifier)
	if err != nil {
		return nil, err
	}
	if _, err := s.tagsOf(c, in.ResourceIdentifier); err != nil { // the resource must exist
		return nil, err
	}
	id := newID()
	as := &association{Association{id, x.Arn, in.ResourceIdentifier, c.ARN("appconfig", "extensionassociation/"+id), in.Parameters,
		cmp.Or(in.ExtensionVersionNumber, x.VersionNumber)}, in.Tags}
	s.st.Assocs[scope(c)+id] = as
	return as.Association, nil
}

func (s *Service) listAssociations(c *core.Ctx, r *http.Request) (any, error) {
	q := r.URL.Query()
	s.mu.Lock()
	defer s.mu.Unlock()
	ext := ""
	if id := q.Get("extension_identifier"); id != "" {
		x, err := s.extension(c, id)
		if err != nil {
			return nil, err
		}
		ext = x.Arn
	}
	out := []Association{}
	for _, k := range core.SortedKeys(s.st.Assocs) {
		as := s.st.Assocs[k].Association
		version := strconv.Itoa(as.ExtensionVersionNumber)
		if !strings.HasPrefix(k, scope(c)) || cmp.Or(q.Get("resource_identifier"), as.ResourceArn) != as.ResourceArn ||
			ext != "" && ext != as.ExtensionArn || cmp.Or(q.Get("extension_version_number"), version) != version {
			continue
		}
		as.Parameters = nil
		out = append(out, as)
	}
	return page(out, r), nil
}

func (s *Service) getAssociation(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	as, err := s.association(c, r.PathValue("ExtensionAssociationId"))
	if err != nil {
		return nil, err
	}
	return as.Association, nil
}

func (s *Service) updateAssociation(c *core.Ctx, r *http.Request) (any, error) {
	var in struct{ Parameters map[string]string }
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	as, err := s.association(c, r.PathValue("ExtensionAssociationId"))
	if err != nil {
		return nil, err
	}
	if in.Parameters != nil {
		as.Parameters = in.Parameters
	}
	return as.Association, nil
}

func (s *Service) deleteAssociation(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	as, err := s.association(c, r.PathValue("ExtensionAssociationId"))
	if err != nil {
		return nil, err
	}
	delete(s.st.Assocs, scope(c)+as.Id)
	return nil, nil
}

// ---- tags ----

// tagsOf finds the tags of the resource an ARN names; lock held. An ARN
// without an account means the caller's: Terraform builds them so when it
// does not look the account id up.
func (s *Service) tagsOf(c *core.Ctx, arn string) (*map[string]string, error) {
	_, region, account, res, ok := core.ParseARN(arn)
	if !ok {
		return nil, badRequest("%q is not an ARN.", arn)
	}
	k := cmp.Or(account, c.Account) + "/" + cmp.Or(region, c.Region) + "/"
	p := strings.Split(res, "/")
	var tags *map[string]string
	switch {
	case p[0] == "application" && len(p)%2 == 0:
		a := s.st.Apps[k+p[1]]
		switch {
		case a == nil:
		case len(p) == 2:
			tags = &a.Tags
		case p[2] == "configurationprofile" && len(p) == 4 && a.Profiles[p[3]] != nil:
			tags = &a.Profiles[p[3]].Tags
		case p[2] == "environment" && a.Envs[p[3]] != nil:
			e := a.Envs[p[3]]
			if len(p) == 4 {
				tags = &e.Tags
			} else if d, err := e.deployment(p[5]); err == nil && p[4] == "deployment" && len(p) == 6 {
				tags = &d.Tags
			}
		}
	case p[0] == "deploymentstrategy" && len(p) == 2 && s.st.Strategies[k+p[1]] != nil:
		tags = &s.st.Strategies[k+p[1]].Tags
	case p[0] == "extension" && len(p) > 1 && s.st.Extensions[k+p[1]] != nil:
		tags = &s.st.Extensions[k+p[1]].Tags
	case p[0] == "extensionassociation" && len(p) == 2 && s.st.Assocs[k+p[1]] != nil:
		tags = &s.st.Assocs[k+p[1]].Tags
	}
	if tags == nil {
		return nil, notFound("Resource %s not found.", arn)
	}
	return tags, nil
}

func (s *Service) listTags(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tags, err := s.tagsOf(c, r.PathValue("ResourceArn"))
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	maps.Copy(out, *tags)
	return map[string]map[string]string{"Tags": out}, nil
}

func (s *Service) tagResource(c *core.Ctx, r *http.Request) (any, error) {
	var in struct{ Tags map[string]string }
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tags, err := s.tagsOf(c, r.PathValue("ResourceArn"))
	if err != nil {
		return nil, err
	}
	if *tags == nil {
		*tags = map[string]string{}
	}
	maps.Copy(*tags, in.Tags)
	return nil, nil
}

func (s *Service) untagResource(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tags, err := s.tagsOf(c, r.PathValue("ResourceArn"))
	if err != nil {
		return nil, err
	}
	for _, k := range r.URL.Query()["tagKeys"] {
		delete(*tags, k)
	}
	return nil, nil
}

// ---- console ----

// Resources lists applications, environments, profiles and deployments.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	apps := core.Table{Name: "Applications", Columns: []string{"Id", "Name", "Region", "Environments", "Profiles"}}
	envs := core.Table{Name: "Environments", Columns: []string{"Id", "Application", "Name", "State", "Monitors"}}
	profs := core.Table{Name: "Configuration profiles", Columns: []string{"Id", "Application", "Name", "Type", "Location", "Versions"}}
	deps := core.Table{Name: "Deployments", Columns: []string{"Deployment", "Profile", "Version", "Strategy", "State", "Started"}}
	for _, k := range core.SortedKeys(s.st.Apps) {
		a := s.st.Apps[k]
		apps.Rows = append(apps.Rows, []string{a.Id, a.Name, strings.Split(k, "/")[1], strconv.Itoa(len(a.Envs)), strconv.Itoa(len(a.Profiles))})
		for _, id := range core.SortedKeys(a.Envs) {
			e := a.Envs[id]
			envs.Rows = append(envs.Rows, []string{e.Id, a.Name, e.Name, e.State, strconv.Itoa(len(e.Monitors))})
			for _, d := range slices.Backward(e.Deployments) {
				deps.Rows = append(deps.Rows, []string{a.Id + "/" + e.Id + "/" + strconv.Itoa(d.DeploymentNumber), d.ConfigurationName,
					d.ConfigurationVersion, d.DeploymentStrategyId, d.State, d.StartedAt.Format(time.DateTime)})
			}
		}
		for _, id := range core.SortedKeys(a.Profiles) {
			p := a.Profiles[id]
			profs.Rows = append(profs.Rows, []string{p.Id, a.Name, p.Name, p.Type, p.LocationUri, strconv.Itoa(len(p.Versions))})
		}
	}
	return []core.Table{apps, envs, profs, deps}
}

// Inspect returns an application, environment, profile or deployment ("app/env/number").
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := strings.Split(id, "/")
	for _, a := range s.st.Apps {
		switch {
		case table == "Applications" && a.Id == id:
			return a.Application
		case table == "Environments" && a.Envs[id] != nil:
			return a.Envs[id].Environment
		case table == "Configuration profiles" && a.Profiles[id] != nil:
			return a.Profiles[id].ConfigurationProfile
		case table == "Deployments" && len(p) == 3 && a.Id == p[0] && a.Envs[p[1]] != nil:
			if d, err := a.Envs[p[1]].deployment(p[2]); err == nil {
				return map[string]any{"deployment": d.Deployment, "contentType": d.ContentType, "content": string(d.Content)}
			}
		}
	}
	return nil
}
