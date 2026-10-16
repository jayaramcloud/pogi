// Package codedeploy emulates AWS CodeDeploy (JSON 1.1): applications,
// deployment groups, deployment configs and deployments. Lambda deployments
// run their BeforeAllowTraffic and AfterAllowTraffic hooks and shift an
// alias' traffic in canary or linear steps, rolling back to the current
// version on a failed hook, an alarm in ALARM or StopDeployment; Server and
// ECS deployments are metadata.
package codedeploy

import (
	"archive/zip"
	"bytes"
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
	"go.yaml.in/yaml/v3"
)

// Service is the CodeDeploy emulator.
type Service struct {
	hub  *core.Hub
	rpc  *core.RPC
	stop func()
	mu   sync.Mutex
	st   state
}

type state struct {
	Apps        map[string]*App           `json:"applications"`      // account/region/name
	Groups      map[string]map[string]any `json:"deploymentGroups"`  // account/region/app/name, in the API's shape
	Configs     map[string]*Config        `json:"deploymentConfigs"` // account/region/name; the predefined ones are not stored
	Deployments map[string]*Deployment    `json:"deployments"`       // account/region/id
	Tags        map[string][]Tag          `json:"tags,omitempty"`    // ARN → tags
}

func newState() state {
	return state{Apps: map[string]*App{}, Groups: map[string]map[string]any{}, Configs: map[string]*Config{},
		Deployments: map[string]*Deployment{}, Tags: map[string][]Tag{}}
}

// App is an application.
type App struct {
	ApplicationID   string    `json:"applicationId"`
	ApplicationName string    `json:"applicationName"`
	CreateTime      core.Time `json:"createTime"`
	LinkedToGitHub  bool      `json:"linkedToGitHub"`
	ComputePlatform string    `json:"computePlatform"`
}

// Config is a deployment config.
type Config struct {
	DeploymentConfigID   string          `json:"deploymentConfigId"`
	DeploymentConfigName string          `json:"deploymentConfigName"`
	MinimumHealthyHosts  *Hosts          `json:"minimumHealthyHosts,omitempty"`
	CreateTime           core.Time       `json:"createTime"`
	ComputePlatform      string          `json:"computePlatform"`
	TrafficRoutingConfig *Routing        `json:"trafficRoutingConfig,omitempty"`
	ZonalConfig          json.RawMessage `json:"zonalConfig,omitempty"`
}

// Hosts is how many hosts must stay healthy (Server deployments).
type Hosts struct {
	Type  string `json:"type"`
	Value int    `json:"value"`
}

// Routing is how a Lambda or ECS deployment shifts traffic.
type Routing struct {
	Type            string `json:"type"` // AllAtOnce | TimeBasedCanary | TimeBasedLinear
	TimeBasedCanary *struct {
		CanaryPercentage int `json:"canaryPercentage"`
		CanaryInterval   int `json:"canaryInterval"`
	} `json:"timeBasedCanary,omitempty"`
	TimeBasedLinear *struct {
		LinearPercentage int `json:"linearPercentage"`
		LinearInterval   int `json:"linearInterval"`
	} `json:"timeBasedLinear,omitempty"`
}

// step is a routing's percentage per step and minutes between steps.
func (r *Routing) step() (pct, minutes int) {
	switch {
	case r != nil && r.TimeBasedCanary != nil && r.Type == "TimeBasedCanary":
		return r.TimeBasedCanary.CanaryPercentage, r.TimeBasedCanary.CanaryInterval
	case r != nil && r.TimeBasedLinear != nil && r.Type == "TimeBasedLinear":
		return r.TimeBasedLinear.LinearPercentage, r.TimeBasedLinear.LinearInterval
	}
	return 100, 0
}

// next is the target version's weight after weight w, and how long w stays:
// linear steps add their percentage, a canary goes to 100 after its first.
func (r *Routing) next(w int) (int, time.Duration) {
	pct, minutes := r.step()
	if w > 0 && r != nil && r.Type == "TimeBasedCanary" {
		pct = 100
	}
	return min(100, w+pct), time.Duration(minutes) * time.Minute
}

// predefined are the CodeDeployDefault.* configs.
var predefined = func() map[string]*Config {
	out := map[string]*Config{}
	add := func(name, platform string, hosts *Hosts, routing string) {
		sum := sha256.Sum256([]byte(name))
		c := &Config{DeploymentConfigID: hex.EncodeToString(sum[:16]), DeploymentConfigName: "CodeDeployDefault." + name,
			ComputePlatform: platform, MinimumHealthyHosts: hosts}
		if routing != "" {
			json.Unmarshal([]byte(routing), &c.TrafficRoutingConfig)
		}
		out[c.DeploymentConfigName] = c
	}
	add("AllAtOnce", "Server", &Hosts{"FLEET_PERCENT", 0}, "")
	add("HalfAtATime", "Server", &Hosts{"FLEET_PERCENT", 50}, "")
	add("OneAtATime", "Server", &Hosts{"HOST_COUNT", 1}, "")
	for _, p := range []string{"Lambda", "ECS"} {
		add(p+"AllAtOnce", p, nil, `{"type":"AllAtOnce"}`)
	}
	canary := `{"type":"TimeBasedCanary","timeBasedCanary":{"canaryPercentage":10,"canaryInterval":%d}}`
	linear := `{"type":"TimeBasedLinear","timeBasedLinear":{"linearPercentage":10,"linearInterval":%d}}`
	for _, m := range []int{5, 10, 15, 30} {
		add(fmt.Sprintf("LambdaCanary10Percent%dMinutes", m), "Lambda", nil, fmt.Sprintf(canary, m))
	}
	for name, m := range map[string]int{"1Minute": 1, "2Minutes": 2, "3Minutes": 3, "10Minutes": 10} {
		add("LambdaLinear10PercentEvery"+name, "Lambda", nil, fmt.Sprintf(linear, m))
	}
	for _, m := range []int{5, 15} {
		add(fmt.Sprintf("ECSCanary10Percent%dMinutes", m), "ECS", nil, fmt.Sprintf(canary, m))
	}
	for _, m := range []int{1, 3} {
		add(fmt.Sprintf("ECSLinear10PercentEvery%dMinutes", m), "ECS", nil, fmt.Sprintf(linear, m))
	}
	return out
}()

// Deployment is a deployment in the API's shape, plus the progress of a
// Lambda deployment.
type Deployment struct {
	ApplicationName          string          `json:"applicationName"`
	DeploymentGroupName      string          `json:"deploymentGroupName"`
	DeploymentConfigName     string          `json:"deploymentConfigName"`
	DeploymentID             string          `json:"deploymentId"`
	Revision                 json.RawMessage `json:"revision,omitempty"`
	Status                   string          `json:"status"`
	ErrorInformation         *ErrorInfo      `json:"errorInformation,omitempty"`
	CreateTime               core.Time       `json:"createTime"`
	StartTime                *core.Time      `json:"startTime,omitempty"`
	CompleteTime             *core.Time      `json:"completeTime,omitempty"`
	DeploymentOverview       map[string]int  `json:"deploymentOverview"`
	Description              string          `json:"description,omitempty"`
	Creator                  string          `json:"creator"`
	RollbackInfo             *RollbackInfo   `json:"rollbackInfo,omitempty"`
	DeploymentStyle          any             `json:"deploymentStyle,omitempty"`
	ComputePlatform          string          `json:"computePlatform"`
	DeploymentStatusMessages []string        `json:"deploymentStatusMessages"`
	Lambda                   *Shift          `json:"lambda,omitempty"`
}

// ErrorInfo says why a deployment failed.
type ErrorInfo struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// RollbackInfo says how a deployment was rolled back.
type RollbackInfo struct {
	RollbackMessage string `json:"rollbackMessage"`
}

// Shift is a Lambda deployment's progress: its AppSpec, lifecycle events and
// how much traffic the target version has.
type Shift struct {
	Function, Alias, Current, Target string
	Hooks                            map[string]string // lifecycle event → hook function
	Routing                          *Routing
	Alarms                           []string
	Events                           []*Event
	Phase                            int // index of the event in progress
	Weight                           int // percent of traffic on the target version
	Shifted                          time.Time
	Hook                             *Hook // the hook execution in progress
	Updated                          time.Time
}

// Event is a lifecycle event of a deployment target.
type Event struct {
	LifecycleEventName string       `json:"lifecycleEventName"`
	Status             string       `json:"status"`
	StartTime          *core.Time   `json:"startTime,omitempty"`
	EndTime            *core.Time   `json:"endTime,omitempty"`
	Diagnostics        *Diagnostics `json:"diagnostics,omitempty"`
}

// Diagnostics says why a lifecycle event failed.
type Diagnostics struct {
	ErrorCode  string `json:"errorCode"`
	ScriptName string `json:"scriptName,omitempty"`
	Message    string `json:"message"`
	LogTail    string `json:"logTail"`
}

// Hook is an execution of a lifecycle hook function.
type Hook struct {
	ID      string
	Status  string // Pending | Succeeded | Failed
	Message string
	Started time.Time
}

// Tag is a resource tag.
type Tag struct {
	Key   string
	Value string
}

// New creates the CodeDeploy service and starts its clock.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, st: newState()}
	s.rpc = &core.RPC{JSONVersion: "1.1", Ops: map[string]core.Handler{
		"CreateApplication":                    core.H(s.createApp),
		"GetApplication":                       core.H(s.getApp),
		"BatchGetApplications":                 core.H(s.batchGetApps),
		"ListApplications":                     core.H(s.listApps),
		"UpdateApplication":                    core.H(s.updateApp),
		"DeleteApplication":                    core.H(s.deleteApp),
		"CreateDeploymentGroup":                core.H(s.createGroup),
		"GetDeploymentGroup":                   core.H(s.getGroup),
		"BatchGetDeploymentGroups":             core.H(s.batchGetGroups),
		"ListDeploymentGroups":                 core.H(s.listGroups),
		"UpdateDeploymentGroup":                core.H(s.updateGroup),
		"DeleteDeploymentGroup":                core.H(s.deleteGroup),
		"CreateDeploymentConfig":               core.H(s.createConfig),
		"GetDeploymentConfig":                  core.H(s.getConfig),
		"ListDeploymentConfigs":                core.H(s.listConfigs),
		"DeleteDeploymentConfig":               core.H(s.deleteConfig),
		"CreateDeployment":                     core.H(s.createDeployment),
		"GetDeployment":                        core.H(s.getDeployment),
		"BatchGetDeployments":                  core.H(s.batchGetDeployments),
		"ListDeployments":                      core.H(s.listDeployments),
		"StopDeployment":                       core.H(s.stopDeployment),
		"ContinueDeployment":                   core.H(s.continueDeployment),
		"ListDeploymentTargets":                core.H(s.listTargets),
		"BatchGetDeploymentTargets":            core.H(s.batchGetTargets),
		"GetDeploymentTarget":                  core.H(s.getTarget),
		"PutLifecycleEventHookExecutionStatus": core.H(s.putHookStatus),
		"TagResource":                          core.H(s.tagResource),
		"UntagResource":                        core.H(s.untagResource),
		"ListTagsForResource":                  core.H(s.listTags),
	}}
	s.stop = core.Every(time.Second, s.Tick)
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "codedeploy", Cloud: core.AWS, Title: "CodeDeploy", Targets: []string{"CodeDeploy_20141006"},
		Signing: []string{"codedeploy"}, Protocol: "json"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.rpc.ServeHTTP(w, r) }

// Close stops the clock.
func (s *Service) Close() error { s.stop(); return nil }

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

func invalid(code, format string, a ...any) error { return core.Errorf(400, code, format, a...) }

func key(c *core.Ctx, parts ...string) string {
	return c.Account + "/" + c.Region + "/" + strings.Join(parts, "/")
}

func ptr[T any](v T) *T { return &v }

// snap renders stored state under the lock, so the clock cannot change it
// while the response is written.
func snap(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

// ---- applications ----

func (s *Service) app(c *core.Ctx, name string) (*App, error) {
	if name == "" {
		return nil, invalid("ApplicationNameRequiredException", "The minimum number of required application names was not specified.")
	}
	if a := s.st.Apps[key(c, name)]; a != nil {
		return a, nil
	}
	return nil, invalid("ApplicationDoesNotExistException", "No application found for name: %s", name)
}

func (s *Service) createApp(c *core.Ctx, in *struct {
	ApplicationName, ComputePlatform string
	Tags                             []Tag
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if in.ApplicationName == "" {
		return nil, invalid("ApplicationNameRequiredException", "The minimum number of required application names was not specified.")
	}
	if s.st.Apps[key(c, in.ApplicationName)] != nil {
		return nil, invalid("ApplicationAlreadyExistsException", "Application %s already exists for this account.", in.ApplicationName)
	}
	a := &App{ApplicationID: core.UUID(), ApplicationName: in.ApplicationName, CreateTime: core.Time(time.Now()), ComputePlatform: cmp.Or(in.ComputePlatform, "Server")}
	s.st.Apps[key(c, a.ApplicationName)] = a
	if len(in.Tags) > 0 {
		s.st.Tags[c.ARN("codedeploy", "application:"+a.ApplicationName)] = in.Tags
	}
	return map[string]string{"applicationId": a.ApplicationID}, nil
}

func (s *Service) getApp(c *core.Ctx, in *struct{ ApplicationName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.app(c, in.ApplicationName)
	if err != nil {
		return nil, err
	}
	return map[string]any{"application": snap(a)}, nil
}

func (s *Service) batchGetApps(c *core.Ctx, in *struct{ ApplicationNames []string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := []*App{}
	for _, n := range in.ApplicationNames {
		a, err := s.app(c, n)
		if err != nil {
			return nil, err
		}
		list = append(list, a)
	}
	return map[string]any{"applicationsInfo": snap(list)}, nil
}

func (s *Service) listApps(c *core.Ctx, _ *struct{ NextToken string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := []string{}
	for _, k := range core.SortedKeys(s.st.Apps) {
		if strings.HasPrefix(k, key(c)) {
			names = append(names, s.st.Apps[k].ApplicationName)
		}
	}
	return map[string]any{"applications": names}, nil
}

func (s *Service) updateApp(c *core.Ctx, in *struct{ ApplicationName, NewApplicationName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.app(c, in.ApplicationName)
	if err != nil || in.NewApplicationName == "" || in.NewApplicationName == a.ApplicationName {
		return nil, err
	}
	if s.st.Apps[key(c, in.NewApplicationName)] != nil {
		return nil, invalid("ApplicationAlreadyExistsException", "Application %s already exists for this account.", in.NewApplicationName)
	}
	delete(s.st.Apps, key(c, a.ApplicationName))
	for k, g := range s.st.Groups {
		if strings.HasPrefix(k, key(c, a.ApplicationName, "")) {
			delete(s.st.Groups, k)
			g["applicationName"] = in.NewApplicationName
			s.st.Groups[key(c, in.NewApplicationName, g["deploymentGroupName"].(string))] = g
		}
	}
	a.ApplicationName = in.NewApplicationName
	s.st.Apps[key(c, a.ApplicationName)] = a
	return nil, nil
}

func (s *Service) deleteApp(c *core.Ctx, in *struct{ ApplicationName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if in.ApplicationName == "" {
		return nil, invalid("ApplicationNameRequiredException", "The minimum number of required application names was not specified.")
	}
	delete(s.st.Apps, key(c, in.ApplicationName))
	delete(s.st.Tags, c.ARN("codedeploy", "application:"+in.ApplicationName))
	for k := range s.st.Groups {
		if strings.HasPrefix(k, key(c, in.ApplicationName, "")) {
			delete(s.st.Groups, k)
		}
	}
	return nil, nil
}

// ---- deployment groups ----

// group finds a deployment group; lock held.
func (s *Service) group(c *core.Ctx, app, name string) (map[string]any, *App, error) {
	a, err := s.app(c, app)
	if err != nil {
		return nil, nil, err
	}
	if name == "" {
		return nil, nil, invalid("DeploymentGroupNameRequiredException", "The deployment group name was not specified.")
	}
	if g := s.st.Groups[key(c, app, name)]; g != nil {
		return g, a, nil
	}
	return nil, nil, invalid("DeploymentGroupDoesNotExistException", "No Deployment Group found for name: %s", name)
}

// config finds a deployment config, predefined or the account's; lock held.
func (s *Service) config(c *core.Ctx, name string) (*Config, error) {
	if cfg := cmp.Or(predefined[name], s.st.Configs[key(c, name)]); cfg != nil {
		return cfg, nil
	}
	return nil, invalid("DeploymentConfigDoesNotExistException", "No deployment configuration found for name: %s", name)
}

// defaults fills what CodeDeploy adds to a deployment group for its platform.
func defaults(g map[string]any, platform string) {
	g["computePlatform"] = platform
	style := map[string]any{"deploymentType": "BLUE_GREEN", "deploymentOption": "WITH_TRAFFIC_CONTROL"}
	config := "CodeDeployDefault." + platform + "AllAtOnce"
	if platform == "Server" {
		style = map[string]any{"deploymentType": "IN_PLACE", "deploymentOption": "WITHOUT_TRAFFIC_CONTROL"}
		config = "CodeDeployDefault.OneAtATime"
	}
	for k, v := range map[string]any{"deploymentStyle": style, "deploymentConfigName": config, "outdatedInstancesStrategy": "UPDATE"} {
		if g[k] == nil || g[k] == "" {
			g[k] = v
		}
	}
}

func str(g map[string]any, k string) string {
	v, _ := g[k].(string)
	return v
}

func (s *Service) createGroup(c *core.Ctx, in *map[string]any) (any, error) {
	g := *in
	app, name := str(g, "applicationName"), str(g, "deploymentGroupName")
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.app(c, app)
	if err != nil {
		return nil, err
	}
	switch {
	case name == "":
		return nil, invalid("DeploymentGroupNameRequiredException", "The deployment group name was not specified.")
	case str(g, "serviceRoleArn") == "":
		return nil, invalid("RoleRequiredException", "The service role ARN was not specified.")
	case s.st.Groups[key(c, app, name)] != nil:
		return nil, invalid("DeploymentGroupAlreadyExistsException", "A deployment group with the name %s already exists for application %s.", name, app)
	}
	if cfg := str(g, "deploymentConfigName"); cfg != "" {
		if _, err := s.config(c, cfg); err != nil {
			return nil, err
		}
	}
	if tags, ok := g["tags"].([]any); ok && len(tags) > 0 {
		var list []Tag
		b, _ := json.Marshal(tags)
		json.Unmarshal(b, &list)
		s.st.Tags[c.ARN("codedeploy", "deploymentgroup:"+app+"/"+name)] = list
	}
	delete(g, "tags")
	g["deploymentGroupId"] = core.UUID()
	defaults(g, a.ComputePlatform)
	s.st.Groups[key(c, app, name)] = g
	return map[string]any{"deploymentGroupId": g["deploymentGroupId"]}, nil
}

func (s *Service) getGroup(c *core.Ctx, in *struct{ ApplicationName, DeploymentGroupName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, _, err := s.group(c, in.ApplicationName, in.DeploymentGroupName)
	if err != nil {
		return nil, err
	}
	return map[string]any{"deploymentGroupInfo": snap(g)}, nil
}

func (s *Service) batchGetGroups(c *core.Ctx, in *struct {
	ApplicationName      string
	DeploymentGroupNames []string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	list, missing := []map[string]any{}, []string{}
	for _, n := range in.DeploymentGroupNames {
		if g, _, err := s.group(c, in.ApplicationName, n); err == nil {
			list = append(list, g)
		} else {
			missing = append(missing, n)
		}
	}
	out := map[string]any{"deploymentGroupsInfo": snap(list)}
	if len(missing) > 0 {
		out["errorMessage"] = "Deployment groups not found: " + strings.Join(missing, ", ")
	}
	return out, nil
}

func (s *Service) listGroups(c *core.Ctx, in *struct{ ApplicationName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.app(c, in.ApplicationName); err != nil {
		return nil, err
	}
	names := []string{}
	for _, k := range core.SortedKeys(s.st.Groups) {
		if strings.HasPrefix(k, key(c, in.ApplicationName, "")) {
			names = append(names, str(s.st.Groups[k], "deploymentGroupName"))
		}
	}
	return map[string]any{"applicationName": in.ApplicationName, "deploymentGroups": names}, nil
}

func (s *Service) updateGroup(c *core.Ctx, in *map[string]any) (any, error) {
	u := *in
	app, current, next := str(u, "applicationName"), str(u, "currentDeploymentGroupName"), str(u, "newDeploymentGroupName")
	s.mu.Lock()
	defer s.mu.Unlock()
	g, a, err := s.group(c, app, current)
	if err != nil {
		return nil, err
	}
	if cfg := str(u, "deploymentConfigName"); cfg != "" {
		if _, err := s.config(c, cfg); err != nil {
			return nil, err
		}
	}
	if next != "" && next != current && s.st.Groups[key(c, app, next)] != nil {
		return nil, invalid("DeploymentGroupAlreadyExistsException", "A deployment group with the name %s already exists for application %s.", next, app)
	}
	for k, v := range u {
		if k != "currentDeploymentGroupName" && k != "newDeploymentGroupName" {
			g[k] = v
		}
	}
	if next != "" && next != current {
		delete(s.st.Groups, key(c, app, current))
		g["deploymentGroupName"] = next
		s.st.Groups[key(c, app, next)] = g
		if tags, ok := s.st.Tags[c.ARN("codedeploy", "deploymentgroup:"+app+"/"+current)]; ok {
			delete(s.st.Tags, c.ARN("codedeploy", "deploymentgroup:"+app+"/"+current))
			s.st.Tags[c.ARN("codedeploy", "deploymentgroup:"+app+"/"+next)] = tags
		}
	}
	defaults(g, a.ComputePlatform)
	return map[string]any{"hooksNotCleanedUp": []any{}}, nil
}

func (s *Service) deleteGroup(c *core.Ctx, in *struct{ ApplicationName, DeploymentGroupName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, _, err := s.group(c, in.ApplicationName, in.DeploymentGroupName); err != nil {
		return nil, err
	}
	delete(s.st.Groups, key(c, in.ApplicationName, in.DeploymentGroupName))
	delete(s.st.Tags, c.ARN("codedeploy", "deploymentgroup:"+in.ApplicationName+"/"+in.DeploymentGroupName))
	return map[string]any{"hooksNotCleanedUp": []any{}}, nil
}

// ---- deployment configs ----

func (s *Service) createConfig(c *core.Ctx, in *Config) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case in.DeploymentConfigName == "":
		return nil, invalid("DeploymentConfigNameRequiredException", "Deployment config name was not specified.")
	case strings.HasPrefix(in.DeploymentConfigName, "CodeDeployDefault."):
		return nil, invalid("InvalidDeploymentConfigNameException", "The prefix CodeDeployDefault. is reserved.")
	case s.st.Configs[key(c, in.DeploymentConfigName)] != nil:
		return nil, invalid("DeploymentConfigAlreadyExistsException", "Deployment config %s already exists.", in.DeploymentConfigName)
	}
	in.ComputePlatform = cmp.Or(in.ComputePlatform, "Server")
	if in.ComputePlatform == "Server" && in.MinimumHealthyHosts == nil {
		return nil, invalid("InvalidMinimumHealthyHostValueException", "minimumHealthyHosts is required for the Server compute platform.")
	}
	if pct, minutes := in.TrafficRoutingConfig.step(); in.ComputePlatform != "Server" && (pct <= 0 || pct > 100 || minutes < 0) {
		return nil, invalid("InvalidTrafficRoutingConfigurationException", "The traffic routing configuration is not valid.")
	}
	in.DeploymentConfigID, in.CreateTime = core.UUID(), core.Time(time.Now())
	s.st.Configs[key(c, in.DeploymentConfigName)] = in
	return map[string]string{"deploymentConfigId": in.DeploymentConfigID}, nil
}

func (s *Service) getConfig(c *core.Ctx, in *struct{ DeploymentConfigName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cfg, err := s.config(c, in.DeploymentConfigName)
	if err != nil {
		return nil, err
	}
	return map[string]any{"deploymentConfigInfo": cfg}, nil
}

func (s *Service) listConfigs(c *core.Ctx, _ *struct{ NextToken string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := core.SortedKeys(predefined)
	for _, k := range core.SortedKeys(s.st.Configs) {
		if strings.HasPrefix(k, key(c)) {
			names = append(names, s.st.Configs[k].DeploymentConfigName)
		}
	}
	return map[string]any{"deploymentConfigsList": names}, nil
}

func (s *Service) deleteConfig(c *core.Ctx, in *struct{ DeploymentConfigName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if predefined[in.DeploymentConfigName] != nil {
		return nil, invalid("InvalidOperationException", "The predefined deployment configuration %s cannot be deleted.", in.DeploymentConfigName)
	}
	for k, g := range s.st.Groups {
		if strings.HasPrefix(k, key(c)) && str(g, "deploymentConfigName") == in.DeploymentConfigName {
			return nil, invalid("DeploymentConfigInUseException", "The deployment configuration is in use by deployment group %s.", str(g, "deploymentGroupName"))
		}
	}
	delete(s.st.Configs, key(c, in.DeploymentConfigName))
	return nil, nil
}

// ---- deployments ----

type createDeploymentInput struct {
	ApplicationName, DeploymentGroupName, DeploymentConfigName, Description string
	Revision                                                                json.RawMessage
	OverrideAlarmConfiguration                                              *alarmConfig
}

type alarmConfig struct {
	Enabled bool
	Alarms  []struct{ Name string }
}

// appSpec is the part of a Lambda AppSpec file CodeDeploy acts on.
type appSpec struct {
	Resources []map[string]struct {
		Type       string `yaml:"Type"`
		Properties struct {
			Name           string `yaml:"Name"`
			Alias          string `yaml:"Alias"`
			CurrentVersion string `yaml:"CurrentVersion"`
			TargetVersion  string `yaml:"TargetVersion"`
		} `yaml:"Properties"`
	} `yaml:"Resources"`
	Hooks []map[string]string `yaml:"Hooks"`
}

// appSpecOf reads a revision's AppSpec: inline (AppSpecContent, String) or
// an S3 object, a zip holding appspec.yaml or the YAML or JSON file itself.
func (s *Service) appSpecOf(c *core.Ctx, raw json.RawMessage) (*appSpec, error) {
	var rev struct {
		RevisionType   string
		AppSpecContent struct{ Content string }
		String         struct{ Content string }
		S3Location     struct{ Bucket, Key, BundleType string }
	}
	json.Unmarshal(raw, &rev)
	text := cmp.Or(rev.AppSpecContent.Content, rev.String.Content)
	if loc := rev.S3Location; rev.RevisionType == "S3" {
		status, _, body := s.hub.Do(c.Account, c.Region, "s3", http.MethodGet, (&url.URL{Path: "/" + loc.Bucket + "/" + loc.Key}).EscapedPath(), nil, nil)
		if status != http.StatusOK {
			return nil, invalid("RevisionDoesNotExistException", "No revision found at s3://%s/%s", loc.Bucket, loc.Key)
		}
		text = string(body)
		if z, err := zip.NewReader(bytes.NewReader(body), int64(len(body))); err == nil {
			for _, f := range z.File {
				if n := strings.ToLower(f.Name); n == "appspec.yaml" || n == "appspec.yml" || n == "appspec.json" {
					rc, _ := f.Open()
					b, _ := io.ReadAll(rc)
					rc.Close()
					text = string(b)
				}
			}
		}
	}
	var spec appSpec
	if err := yaml.Unmarshal([]byte(text), &spec); err != nil || len(spec.Resources) != 1 {
		return nil, invalid("InvalidRevisionException", "The AppSpec must name one AWS::Lambda::Function resource (%v)", err)
	}
	return &spec, nil
}

func (s *Service) createDeployment(c *core.Ctx, in *createDeploymentInput) (any, error) {
	s.mu.Lock()
	g, a, err := s.group(c, in.ApplicationName, in.DeploymentGroupName)
	var cfg *Config
	if err == nil {
		cfg, err = s.config(c, cmp.Or(in.DeploymentConfigName, str(g, "deploymentConfigName")))
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if len(in.Revision) == 0 && a.ComputePlatform != "Server" {
		return nil, invalid("RevisionRequiredException", "The revision ID was not specified.")
	}
	d := &Deployment{ApplicationName: a.ApplicationName, DeploymentGroupName: in.DeploymentGroupName, DeploymentConfigName: cfg.DeploymentConfigName,
		DeploymentID: "d-" + core.RandString(9, core.UpperNum), Revision: in.Revision, Status: "Created", CreateTime: core.Time(time.Now()),
		Description: in.Description, Creator: "user", DeploymentStyle: g["deploymentStyle"], ComputePlatform: a.ComputePlatform,
		DeploymentStatusMessages: []string{}}
	if a.ComputePlatform == "Lambda" {
		spec, err := s.appSpecOf(c, in.Revision)
		if err != nil {
			return nil, err
		}
		for _, r := range spec.Resources {
			for _, res := range r {
				p := res.Properties
				d.Lambda = &Shift{Function: p.Name, Alias: p.Alias, Current: p.CurrentVersion, Target: p.TargetVersion, Routing: cfg.TrafficRoutingConfig,
					Hooks: map[string]string{}}
			}
		}
		for _, h := range spec.Hooks {
			for event, fn := range h {
				d.Lambda.Hooks[event] = fn
			}
		}
		for _, name := range []string{"BeforeAllowTraffic", "AllowTraffic", "AfterAllowTraffic"} {
			d.Lambda.Events = append(d.Lambda.Events, &Event{LifecycleEventName: name, Status: "Pending"})
		}
		var alarms alarmConfig
		b, _ := json.Marshal(g["alarmConfiguration"])
		json.Unmarshal(b, &alarms)
		if in.OverrideAlarmConfiguration != nil {
			alarms = *in.OverrideAlarmConfiguration
		}
		for _, al := range alarms.Alarms {
			if alarms.Enabled {
				d.Lambda.Alarms = append(d.Lambda.Alarms, al.Name)
			}
		}
	}
	s.mu.Lock()
	s.st.Deployments[key(c, d.DeploymentID)] = d
	s.mu.Unlock()
	return map[string]string{"deploymentId": d.DeploymentID}, nil
}

// deployment finds a deployment; lock held.
func (s *Service) deployment(c *core.Ctx, id string) (*Deployment, error) {
	if id == "" {
		return nil, invalid("DeploymentIdRequiredException", "At least one deployment ID must be specified.")
	}
	if d := s.st.Deployments[key(c, id)]; d != nil {
		return d, nil
	}
	return nil, invalid("DeploymentDoesNotExistException", "The deployment %s could not be found.", id)
}

// view is a deployment as the API shows it, with its overview.
func (d *Deployment) view() *Deployment {
	v := *d
	v.DeploymentOverview = map[string]int{"Pending": 0, "InProgress": 0, "Succeeded": 0, "Failed": 0, "Skipped": 0, "Ready": 0}
	if d.Lambda != nil {
		v.DeploymentOverview[d.targetStatus()]++
	}
	return &v
}

func (s *Service) getDeployment(c *core.Ctx, in *struct{ DeploymentId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.deployment(c, in.DeploymentId)
	if err != nil {
		return nil, err
	}
	return map[string]any{"deploymentInfo": snap(d.view())}, nil
}

func (s *Service) batchGetDeployments(c *core.Ctx, in *struct{ DeploymentIds []string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := []*Deployment{}
	for _, id := range in.DeploymentIds {
		if d, err := s.deployment(c, id); err == nil {
			list = append(list, d.view())
		}
	}
	return map[string]any{"deploymentsInfo": snap(list)}, nil
}

func (s *Service) listDeployments(c *core.Ctx, in *struct {
	ApplicationName, DeploymentGroupName string
	IncludeOnlyStatuses                  []string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var list []*Deployment
	for k, d := range s.st.Deployments {
		if strings.HasPrefix(k, key(c)) && (in.ApplicationName == "" || d.ApplicationName == in.ApplicationName) &&
			(in.DeploymentGroupName == "" || d.DeploymentGroupName == in.DeploymentGroupName) &&
			(len(in.IncludeOnlyStatuses) == 0 || slices.Contains(in.IncludeOnlyStatuses, d.Status)) {
			list = append(list, d)
		}
	}
	slices.SortFunc(list, func(a, b *Deployment) int { return time.Time(b.CreateTime).Compare(time.Time(a.CreateTime)) })
	ids := []string{}
	for _, d := range list {
		ids = append(ids, d.DeploymentID)
	}
	return map[string]any{"deployments": ids}, nil
}

func (s *Service) stopDeployment(c *core.Ctx, in *struct {
	DeploymentId        string
	AutoRollbackEnabled *bool
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.deployment(c, in.DeploymentId)
	if err != nil {
		return nil, err
	}
	if d.done() {
		return nil, invalid("DeploymentAlreadyCompletedException", "The deployment %s is already completed.", d.DeploymentID)
	}
	rollback := in.AutoRollbackEnabled != nil && *in.AutoRollbackEnabled
	s.fail(c.Account, c.Region, d, time.Now(), "Stopped", "MANUAL_STOP", "The deployment was stopped by the user.", rollback)
	return map[string]string{"status": "Succeeded", "statusMessage": "The deployment was stopped."}, nil
}

// continueDeployment has nothing to wait for: pogi does not hold
// deployments for a blue/green switch. ponytail.
func (s *Service) continueDeployment(c *core.Ctx, in *struct{ DeploymentId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.deployment(c, in.DeploymentId)
	return nil, err
}

// ---- deployment targets ----

func (d *Deployment) targetID() string { return d.Lambda.Function + ":" + d.Lambda.Alias }

func (d *Deployment) targetStatus() string {
	switch d.Status {
	case "Created":
		return "Pending"
	case "Succeeded":
		return "Succeeded"
	case "Failed", "Stopped":
		return "Failed"
	}
	return "InProgress"
}

// target is a Lambda deployment's one target.
func (d *Deployment) target(c *core.Ctx) map[string]any {
	l := d.Lambda
	arn := l.Function
	if !strings.HasPrefix(arn, "arn:") {
		arn = c.ARN("lambda", "function:"+l.Function)
	}
	return map[string]any{"deploymentId": d.DeploymentID, "targetId": d.targetID(), "targetArn": arn + ":" + l.Alias, "status": d.targetStatus(),
		"lastUpdatedAt": core.Time(cmp.Or(l.Updated, time.Time(d.CreateTime))), "lifecycleEvents": l.Events,
		"lambdaFunctionInfo": map[string]any{"functionName": l.Function, "functionAlias": l.Alias, "currentVersion": l.Current,
			"targetVersion": l.Target, "targetVersionWeight": float64(l.Weight) / 100}}
}

func (s *Service) listTargets(c *core.Ctx, in *struct{ DeploymentId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.deployment(c, in.DeploymentId)
	if err != nil {
		return nil, err
	}
	ids := []string{}
	if d.Lambda != nil {
		ids = append(ids, d.targetID())
	}
	return map[string]any{"targetIds": ids}, nil
}

// targets renders targets of a deployment by ID.
func (s *Service) targets(c *core.Ctx, id string, ids []string) ([]map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.deployment(c, id)
	if err != nil {
		return nil, err
	}
	list := []map[string]any{}
	for _, t := range ids {
		if d.Lambda == nil || t != d.targetID() {
			return nil, invalid("DeploymentTargetDoesNotExistException", "The target %s does not exist in deployment %s.", t, d.DeploymentID)
		}
		list = append(list, map[string]any{"deploymentTargetType": "LambdaTarget", "lambdaTarget": snap(d.target(c))})
	}
	return list, nil
}

func (s *Service) batchGetTargets(c *core.Ctx, in *struct {
	DeploymentId string
	TargetIds    []string
}) (any, error) {
	list, err := s.targets(c, in.DeploymentId, in.TargetIds)
	if err != nil {
		return nil, err
	}
	return map[string]any{"deploymentTargets": list}, nil
}

func (s *Service) getTarget(c *core.Ctx, in *struct{ DeploymentId, TargetId string }) (any, error) {
	list, err := s.targets(c, in.DeploymentId, []string{in.TargetId})
	if err != nil {
		return nil, err
	}
	return map[string]any{"deploymentTarget": list[0]}, nil
}

func (s *Service) putHookStatus(c *core.Ctx, in *struct{ DeploymentId, LifecycleEventHookExecutionId, Status string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.deployment(c, in.DeploymentId)
	if err != nil {
		return nil, err
	}
	if in.Status != "Succeeded" && in.Status != "Failed" {
		return nil, invalid("InvalidLifecycleEventHookExecutionStatusException", "The status %s is not valid: use Succeeded or Failed.", in.Status)
	}
	h := (*Hook)(nil)
	if d.Lambda != nil {
		h = d.Lambda.Hook
	}
	if h == nil || h.ID != in.LifecycleEventHookExecutionId {
		return nil, invalid("InvalidLifecycleEventHookExecutionIdException", "The lifecycle event hook execution %s is not in progress.", in.LifecycleEventHookExecutionId)
	}
	if h.Status != "Pending" {
		return nil, invalid("LifecycleEventAlreadyCompletedException", "The lifecycle event hook execution %s already completed.", h.ID)
	}
	h.Status = in.Status
	return map[string]string{"lifecycleEventHookExecutionId": h.ID}, nil
}

// ---- tags ----

// tagged checks that an application or deployment group has this ARN and
// returns it whole: Terraform leaves the account out. Lock held.
func (s *Service) tagged(c *core.Ctx, arn string) (string, error) {
	_, region, account, res, ok := core.ParseARN(arn)
	c = &core.Ctx{Account: cmp.Or(account, c.Account), Region: cmp.Or(region, c.Region)}
	if kind, name, _ := strings.Cut(res, ":"); ok && kind == "application" && s.st.Apps[key(c, name)] != nil ||
		ok && kind == "deploymentgroup" && s.st.Groups[key(c, name)] != nil {
		return c.ARN("codedeploy", res), nil
	}
	return "", invalid("InvalidArnException", "The ARN %s does not name an application or deployment group.", arn)
}

func (s *Service) tagResource(c *core.Ctx, in *struct {
	ResourceArn string
	Tags        []Tag
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	arn, err := s.tagged(c, in.ResourceArn)
	if err != nil {
		return nil, err
	}
	tags := s.st.Tags[arn]
	for _, t := range in.Tags {
		tags = append(slices.DeleteFunc(tags, func(x Tag) bool { return x.Key == t.Key }), t)
	}
	s.st.Tags[arn] = tags
	return nil, nil
}

func (s *Service) untagResource(c *core.Ctx, in *struct {
	ResourceArn string
	TagKeys     []string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	arn, err := s.tagged(c, in.ResourceArn)
	if err != nil {
		return nil, err
	}
	s.st.Tags[arn] = slices.DeleteFunc(s.st.Tags[arn], func(x Tag) bool { return slices.Contains(in.TagKeys, x.Key) })
	return nil, nil
}

func (s *Service) listTags(c *core.Ctx, in *struct{ ResourceArn string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	arn, err := s.tagged(c, in.ResourceArn)
	if err != nil {
		return nil, err
	}
	return map[string]any{"Tags": append([]Tag{}, s.st.Tags[arn]...)}, nil
}

// ---- running deployments ----

func (d *Deployment) done() bool {
	return d.Status == "Succeeded" || d.Status == "Failed" || d.Status == "Stopped"
}

// Tick moves deployments along: Server and ECS ones succeed, Lambda ones
// run their hooks and shift traffic one step per interval.
func (s *Service) Tick(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range core.SortedKeys(s.st.Deployments) {
		d := s.st.Deployments[k]
		if d.done() {
			continue
		}
		account, rest, _ := strings.Cut(k, "/")
		region, _, _ := strings.Cut(rest, "/")
		if d.Status == "Created" {
			d.Status, d.StartTime = "InProgress", ptr(core.Time(now))
		}
		if d.Lambda == nil { // ponytail: Server and ECS deployments deploy nothing
			d.Status, d.CompleteTime = "Succeeded", ptr(core.Time(now))
			continue
		}
		s.step(account, region, d, now)
	}
}

// step advances a Lambda deployment until it waits for a hook or an
// interval; lock held. Lambda never calls back into CodeDeploy while its
// alias changes, and hooks run on their own goroutines.
func (s *Service) step(account, region string, d *Deployment, now time.Time) {
	l := d.Lambda
	if alarm := s.alarm(account, region, l.Alarms); alarm != "" {
		s.fail(account, region, d, now, "Stopped", "ALARM_ACTIVE", "The alarm "+alarm+" is in the ALARM state.", true)
		return
	}
	for ; l.Phase < len(l.Events); l.Phase++ {
		ev := l.Events[l.Phase]
		fn := l.Hooks[ev.LifecycleEventName]
		if fn == "" && ev.LifecycleEventName != "AllowTraffic" {
			ev.Status = "Skipped"
			continue
		}
		if ev.StartTime == nil {
			ev.Status, ev.StartTime, l.Updated = "InProgress", ptr(core.Time(now)), now
		}
		if ev.LifecycleEventName == "AllowTraffic" {
			w, every := l.Routing.next(l.Weight)
			if l.Weight > 0 && now.Sub(l.Shifted) < every {
				return
			}
			if err := s.route(account, region, l, w); err != nil {
				s.fail(account, region, d, now, "Failed", "INVALID_LAMBDA_CONFIGURATION", err.Error(), true)
				return
			}
			if l.Weight, l.Shifted, l.Updated = w, now, now; w < 100 {
				return
			}
		} else {
			switch h := l.Hook; {
			case h == nil:
				l.Hook = &Hook{ID: core.RandString(64, core.AlphaNum), Status: "Pending", Started: now}
				payload, _ := json.Marshal(map[string]string{"DeploymentId": d.DeploymentID, "LifecycleEventHookExecutionId": l.Hook.ID})
				core.Go("codedeploy.invoke", func() { s.invoke(account, region, d.DeploymentID, l.Hook.ID, fn, payload) })
				return
			case h.Status == "Failed" || h.Status == "Pending" && now.Sub(h.Started) > time.Hour:
				ev.Diagnostics = &Diagnostics{ErrorCode: "ScriptFailed", ScriptName: fn, Message: cmp.Or(h.Message, "The hook "+fn+" reported Failed.")}
				s.fail(account, region, d, now, "Failed", "HOOK_EXECUTION_FAILURE", "The "+ev.LifecycleEventName+" hook "+fn+" failed.", true)
				return
			case h.Status == "Pending":
				return
			}
			l.Hook = nil
		}
		ev.Status, ev.EndTime, l.Updated = "Succeeded", ptr(core.Time(now)), now
	}
	d.Status, d.CompleteTime = "Succeeded", ptr(core.Time(now))
}

// fail ends a deployment; with rollback the alias goes back to the current
// version. Lock held. ponytail: it rolls back in place, where CodeDeploy runs
// a separate rollback deployment.
func (s *Service) fail(account, region string, d *Deployment, now time.Time, status, code, msg string, rollback bool) {
	d.Status, d.CompleteTime, d.ErrorInformation = status, ptr(core.Time(now)), &ErrorInfo{code, msg}
	l := d.Lambda
	if l == nil {
		return
	}
	for i, ev := range l.Events {
		switch {
		case i == l.Phase && ev.Status == "InProgress":
			ev.Status, ev.EndTime = "Failed", ptr(core.Time(now))
		case i >= l.Phase:
			ev.Status = "Skipped"
		}
	}
	l.Hook, l.Updated = nil, now
	if rollback {
		l.Weight = 0
		info := &RollbackInfo{RollbackMessage: "The alias " + l.Alias + " was rolled back to version " + l.Current + "."}
		if err := s.route(account, region, l, 0); err != nil {
			info.RollbackMessage = "The rollback failed: " + err.Error()
		}
		d.RollbackInfo = info
	}
}

// route points an alias at the current version with weight percent of
// traffic on the target version, or at the target version at 100.
func (s *Service) route(account, region string, l *Shift, weight int) error {
	body := map[string]any{"FunctionVersion": l.Current, "RoutingConfig": map[string]any{"AdditionalVersionWeights": map[string]float64{}}}
	switch {
	case weight >= 100:
		body["FunctionVersion"] = l.Target
	case weight > 0:
		body["RoutingConfig"] = map[string]any{"AdditionalVersionWeights": map[string]float64{l.Target: float64(weight) / 100}}
	}
	b, _ := json.Marshal(body)
	p := "/2015-03-31/functions/" + url.PathEscape(l.Function) + "/aliases/" + url.PathEscape(l.Alias)
	status, _, out := s.hub.Do(account, region, "lambda", http.MethodPut, p, http.Header{"Content-Type": {"application/json"}}, b)
	if status != http.StatusOK {
		return fmt.Errorf("updating alias %s of %s: %s", l.Alias, l.Function, out)
	}
	return nil
}

// invoke runs a lifecycle hook function; the function reports back with
// PutLifecycleEventHookExecutionStatus, and an error fails the hook.
func (s *Service) invoke(account, region, id, hook, fn string, payload []byte) {
	arn := fn
	if !strings.HasPrefix(fn, "arn:") {
		arn = core.ARN("lambda", region, account, "function:"+fn)
	}
	if _, err := s.hub.Invoke(arn, payload); err != nil {
		s.mu.Lock()
		defer s.mu.Unlock()
		if d := s.st.Deployments[account+"/"+region+"/"+id]; d != nil && d.Lambda != nil && d.Lambda.Hook != nil && d.Lambda.Hook.ID == hook && d.Lambda.Hook.Status == "Pending" {
			d.Lambda.Hook.Status, d.Lambda.Hook.Message = "Failed", err.Error()
		}
	}
}

// alarm returns the name of one of the alarms in ALARM, if any.
func (s *Service) alarm(account, region string, names []string) string {
	if len(names) == 0 {
		return ""
	}
	var out struct{ MetricAlarms, CompositeAlarms []struct{ AlarmName string } }
	if err := s.hub.Call(account, region, "GraniteServiceVersion20100801", "DescribeAlarms",
		map[string]any{"AlarmNames": names, "StateValue": "ALARM", "AlarmTypes": []string{"MetricAlarm", "CompositeAlarm"}}, &out); err != nil {
		return ""
	}
	if all := append(out.MetricAlarms, out.CompositeAlarms...); len(all) > 0 {
		return all[0].AlarmName
	}
	return ""
}

// ---- console ----

func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	apps := core.Table{Name: "Applications", Columns: []string{"Name", "Platform", "Groups"}}
	for _, k := range core.SortedKeys(s.st.Apps) {
		a := s.st.Apps[k]
		n := 0
		for gk := range s.st.Groups {
			if strings.HasPrefix(gk, k+"/") {
				n++
			}
		}
		apps.Rows = append(apps.Rows, []string{a.ApplicationName, a.ComputePlatform, fmt.Sprint(n)})
	}
	deps := core.Table{Name: "Deployments", Columns: []string{"ID", "Application", "Group", "Config", "Status"}}
	for _, k := range core.SortedKeys(s.st.Deployments) {
		d := s.st.Deployments[k]
		deps.Rows = append(deps.Rows, []string{d.DeploymentID, d.ApplicationName, d.DeploymentGroupName, d.DeploymentConfigName, d.Status})
	}
	return []core.Table{apps, deps}
}

func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, a := range s.st.Apps {
		if table == "Applications" && a.ApplicationName == id {
			groups := []map[string]any{}
			for gk, g := range s.st.Groups {
				if strings.HasPrefix(gk, k+"/") {
					groups = append(groups, g)
				}
			}
			return snap(map[string]any{"application": a, "deploymentGroups": groups})
		}
	}
	for _, d := range s.st.Deployments {
		if d.DeploymentID == id {
			return snap(d.view())
		}
	}
	return nil
}
