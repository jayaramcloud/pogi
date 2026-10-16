// Package codebuild emulates AWS CodeBuild over JSON 1.1: projects (with
// report groups and webhooks as metadata) and builds that run real
// buildspecs in Docker, with the source from S3, git or none, logs in
// CloudWatch Logs and artifacts in S3.
package codebuild

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"cmp"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
	"go.yaml.in/yaml/v3"
)

// NoDocker is the phase context of builds that cannot run.
const NoDocker = "pogi: Docker is not available, so builds cannot run"

// src is where a build's source is, as on CodeBuild ($CODEBUILD_SRC_DIR).
const src = "/codebuild/output/src"

var (
	errStopped  = errors.New("STOPPED")
	errTimedOut = errors.New("TIMED_OUT")
)

// Service is the CodeBuild emulator.
type Service struct {
	hub *core.Hub
	rpc *core.RPC
	wg  sync.WaitGroup // running builds

	mu   sync.Mutex
	st   state
	runs map[string]context.CancelCauseFunc // build key → its run
}

type state struct {
	Projects map[string]Project        `json:"projects"`     // account/region/name
	Builds   map[string]*Build         `json:"builds"`       // account/region/id
	Starts   map[string]*startInput    `json:"starts"`       // build key → its StartBuild, for RetryBuild
	Numbers  map[string]int64          `json:"numbers"`      // project key → its last build number
	Reports  map[string]map[string]any `json:"reportGroups"` // account/region/name
}

func newState() state {
	return state{Projects: map[string]Project{}, Builds: map[string]*Build{}, Starts: map[string]*startInput{},
		Numbers: map[string]int64{}, Reports: map[string]map[string]any{}}
}

// Project is a build project as the API shows it: as given, plus what
// CodeBuild fills in.
type Project map[string]any

// Source is where a build's source comes from.
type Source struct {
	Type          string `json:"type"`
	Location      string `json:"location,omitempty"`
	Buildspec     string `json:"buildspec,omitempty"`
	GitCloneDepth int    `json:"gitCloneDepth,omitempty"`
	InsecureSsl   bool   `json:"insecureSsl"`
}

// Environment is a build's container.
type Environment struct {
	Type                     string   `json:"type"`
	Image                    string   `json:"image"`
	ComputeType              string   `json:"computeType"`
	EnvironmentVariables     []EnvVar `json:"environmentVariables,omitempty"`
	PrivilegedMode           bool     `json:"privilegedMode"`
	ImagePullCredentialsType string   `json:"imagePullCredentialsType,omitempty"`
}

// EnvVar is an environment variable: PLAINTEXT, PARAMETER_STORE or SECRETS_MANAGER.
type EnvVar struct {
	Name  string `json:"name"`
	Value string `json:"value"`
	Type  string `json:"type,omitempty"`
}

// Artifacts is where a project's output goes.
type Artifacts struct {
	Type          string `json:"type"`
	Location      string `json:"location,omitempty"`
	Path          string `json:"path,omitempty"`
	Name          string `json:"name,omitempty"`
	NamespaceType string `json:"namespaceType,omitempty"`
	Packaging     string `json:"packaging,omitempty"`
}

// Build is one run of a project, in the API's shape.
type Build struct {
	ID                     string      `json:"id"`
	Arn                    string      `json:"arn"`
	BuildNumber            int64       `json:"buildNumber"`
	StartTime              core.Time   `json:"startTime"`
	EndTime                *core.Time  `json:"endTime,omitempty"`
	CurrentPhase           string      `json:"currentPhase"`
	BuildStatus            string      `json:"buildStatus"`
	BuildComplete          bool        `json:"buildComplete"`
	ProjectName            string      `json:"projectName"`
	SourceVersion          string      `json:"sourceVersion,omitempty"`
	ResolvedSourceVersion  string      `json:"resolvedSourceVersion,omitempty"`
	Phases                 []*Phase    `json:"phases"`
	Source                 Source      `json:"source"`
	Environment            Environment `json:"environment"`
	Artifacts              *Output     `json:"artifacts,omitempty"`
	Logs                   *Logs       `json:"logs,omitempty"`
	ServiceRole            string      `json:"serviceRole,omitempty"`
	TimeoutInMinutes       int         `json:"timeoutInMinutes"`
	QueuedTimeoutInMinutes int         `json:"queuedTimeoutInMinutes"`
	Initiator              string      `json:"initiator,omitempty"`
	EncryptionKey          string      `json:"encryptionKey,omitempty"`
}

// Phase is one step of a build.
type Phase struct {
	Type     string     `json:"phaseType"`
	Status   string     `json:"phaseStatus,omitempty"`
	Start    core.Time  `json:"startTime"`
	End      *core.Time `json:"endTime,omitempty"`
	Duration *int64     `json:"durationInSeconds,omitempty"`
	Contexts []Context  `json:"contexts,omitempty"`
}

// Context says why a phase ended as it did.
type Context struct {
	StatusCode string `json:"statusCode"`
	Message    string `json:"message"`
}

// Output is where a build's artifacts went.
type Output struct {
	Location  string `json:"location"`
	Sha256sum string `json:"sha256sum,omitempty"`
	Md5sum    string `json:"md5sum,omitempty"`
}

// Logs is where a build logs to.
type Logs struct {
	GroupName  string `json:"groupName"`
	StreamName string `json:"streamName"`
	DeepLink   string `json:"deepLink"`
	Arn        string `json:"cloudWatchLogsArn"`
}

// New creates the CodeBuild service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, st: newState(), runs: map[string]context.CancelCauseFunc{}}
	s.rpc = &core.RPC{JSONVersion: "1.1", Ops: map[string]core.Handler{
		"CreateProject":           core.H(s.createProject),
		"UpdateProject":           core.H(s.updateProject),
		"BatchGetProjects":        core.H(s.batchGetProjects),
		"ListProjects":            core.H(s.listProjects),
		"DeleteProject":           core.H(s.deleteProject),
		"UpdateProjectVisibility": core.H(s.updateVisibility),
		"StartBuild":              core.H(s.startBuild),
		"RetryBuild":              core.H(s.retryBuild),
		"StopBuild":               core.H(s.stopBuild),
		"BatchGetBuilds":          core.H(s.batchGetBuilds),
		"ListBuilds":              core.H(s.listBuilds),
		"ListBuildsForProject":    core.H(s.listBuildsForProject),
		"BatchDeleteBuilds":       core.H(s.batchDeleteBuilds),
		"CreateWebhook":           core.H(s.createWebhook),
		"UpdateWebhook":           core.H(s.updateWebhook),
		"DeleteWebhook":           core.H(s.deleteWebhook),
		"CreateReportGroup":       core.H(s.createReportGroup),
		"BatchGetReportGroups":    core.H(s.batchGetReportGroups),
		"UpdateReportGroup":       core.H(s.updateReportGroup),
		"DeleteReportGroup":       core.H(s.deleteReportGroup),
		"ListReportGroups":        core.H(s.listReportGroups),
	}}
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "codebuild", Cloud: core.AWS, Title: "CodeBuild", Targets: []string{"CodeBuild_20161006"}, Signing: []string{"codebuild"}, Protocol: "json"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.rpc.ServeHTTP(w, r) }

func invalid(format string, a ...any) error {
	return core.Errorf(http.StatusBadRequest, "InvalidInputException", format, a...)
}

func notFound(format string, a ...any) error {
	return core.Errorf(http.StatusBadRequest, "ResourceNotFoundException", format, a...)
}

func exists(format string, a ...any) error {
	return core.Errorf(http.StatusBadRequest, "ResourceAlreadyExistsException", format, a...)
}

// key is the state key of a resource given by name or ARN; kind is its ARN
// resource type (project, build, report-group).
func key(c *core.Ctx, kind, ref string) string {
	if _, region, account, res, ok := core.ParseARN(ref); ok {
		return account + "/" + region + "/" + strings.TrimPrefix(res, kind+"/")
	}
	return c.Account + "/" + c.Region + "/" + ref
}

// snap encodes state for a response while the lock is held.
func snap(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

// ---- projects ----

var projectName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{1,254}$`)

func (s *Service) createProject(c *core.Ctx, in *Project) (any, error) {
	p := *in
	name, _ := p["name"].(string)
	if !projectName.MatchString(name) {
		return nil, invalid("Invalid project name: %q", name)
	}
	for _, f := range []string{"source", "artifacts", "environment", "serviceRole"} {
		if p[f] == nil {
			return nil, invalid("%s is required", f)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k, arn := key(c, "project", name), c.ARN("codebuild", "project/"+name)
	if s.st.Projects[k] != nil {
		return nil, exists("Project already exists: %s", arn)
	}
	now := core.Time(time.Now())
	p["arn"], p["created"], p["lastModified"] = arn, now, now
	defaults(c, p)
	s.st.Projects[k] = p
	return map[string]any{"project": snap(p)}, nil
}

// defaults fills in what CodeBuild adds to every project.
func defaults(c *core.Ctx, p Project) {
	set := func(m map[string]any, k string, v any) {
		if _, ok := m[k]; !ok {
			m[k] = v
		}
	}
	if b, ok := p["badgeEnabled"]; ok { // input
		p["badge"] = map[string]any{"badgeEnabled": b == true}
		delete(p, "badgeEnabled")
	}
	set(p, "badge", map[string]any{"badgeEnabled": false})
	set(p, "timeoutInMinutes", 60)
	set(p, "queuedTimeoutInMinutes", 480)
	set(p, "encryptionKey", c.ARN("kms", "alias/aws/s3"))
	set(p, "projectVisibility", "PRIVATE")
	set(p, "cache", map[string]any{"type": "NO_CACHE"})
	set(p, "logsConfig", map[string]any{"cloudWatchLogs": map[string]any{"status": "ENABLED"},
		"s3Logs": map[string]any{"status": "DISABLED", "encryptionDisabled": false}})
	if src, ok := p["source"].(map[string]any); ok {
		set(src, "insecureSsl", false)
	}
	if env, ok := p["environment"].(map[string]any); ok {
		set(env, "privilegedMode", false)
		set(env, "imagePullCredentialsType", "CODEBUILD")
		vars, _ := env["environmentVariables"].([]any)
		for _, v := range vars {
			if m, ok := v.(map[string]any); ok {
				set(m, "type", "PLAINTEXT")
			}
		}
	}
}

func (s *Service) updateProject(c *core.Ctx, in *Project) (any, error) {
	u := *in
	name, _ := u["name"].(string)
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.st.Projects[key(c, "project", name)]
	if p == nil {
		return nil, notFound("Project cannot be found: %s", name)
	}
	delete(u, "name")
	maps.Copy(p, u)
	p["lastModified"] = core.Time(time.Now())
	defaults(c, p)
	return map[string]any{"project": snap(p)}, nil
}

func (s *Service) batchGetProjects(c *core.Ctx, in *struct{ Names []string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	found, missing := []Project{}, []string{}
	for _, n := range in.Names {
		if p := s.st.Projects[key(c, "project", n)]; p != nil {
			found = append(found, p)
		} else {
			missing = append(missing, n)
		}
	}
	return map[string]any{"projects": snap(found), "projectsNotFound": missing}, nil
}

// ponytail: one page, by name; CodeBuild pages by 100 and sorts by time too.
func (s *Service) listProjects(c *core.Ctx, in *struct{ SortOrder string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := []string{}
	for _, k := range core.SortedKeys(s.st.Projects) {
		if strings.HasPrefix(k, c.Account+"/"+c.Region+"/") {
			names = append(names, s.st.Projects[k]["name"].(string))
		}
	}
	if in.SortOrder == "DESCENDING" {
		slices.Reverse(names)
	}
	return map[string]any{"projects": names}, nil
}

func (s *Service) deleteProject(c *core.Ctx, in *struct{ Name string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.st.Projects, key(c, "project", in.Name))
	return nil, nil
}

func (s *Service) updateVisibility(c *core.Ctx, in *struct{ ProjectArn, ProjectVisibility, ResourceAccessRole string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.st.Projects[key(c, "project", in.ProjectArn)]
	if p == nil {
		return nil, notFound("Project cannot be found: %s", in.ProjectArn)
	}
	p["projectVisibility"] = in.ProjectVisibility
	if in.ResourceAccessRole != "" {
		p["resourceAccessRole"] = in.ResourceAccessRole
	}
	return map[string]any{"projectArn": p["arn"], "projectVisibility": in.ProjectVisibility}, nil
}

// project finds a project by name or ARN. Lock held.
func (s *Service) project(c *core.Ctx, ref string) (Project, error) {
	if p := s.st.Projects[key(c, "project", ref)]; p != nil {
		return p, nil
	}
	return nil, notFound("Project cannot be found: %s", ref)
}

// ---- webhooks and report groups (metadata) ----

func (s *Service) createWebhook(c *core.Ctx, in *map[string]any) (any, error) {
	w := *in
	name, _ := w["projectName"].(string)
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.project(c, name)
	if err != nil {
		return nil, err
	}
	if p["webhook"] != nil {
		return nil, exists("Webhook already exists for project %s", name)
	}
	delete(w, "projectName")
	w["url"] = "https://codebuild." + c.Region + "." + core.URLSuffix(c.Region) + "/webhooks?t=" + core.RandHex(16) + "&v=1"
	w["payloadUrl"], w["secret"], w["lastModifiedSecret"] = w["url"], core.RandHex(20), core.Time(time.Now())
	p["webhook"] = w
	return map[string]any{"webhook": snap(w)}, nil
}

func (s *Service) updateWebhook(c *core.Ctx, in *map[string]any) (any, error) {
	u := *in
	name, _ := u["projectName"].(string)
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.project(c, name)
	if err != nil {
		return nil, err
	}
	w, _ := p["webhook"].(map[string]any)
	if w == nil {
		return nil, notFound("Webhook for project %s does not exist", name)
	}
	if u["rotateSecret"] == true {
		w["secret"], w["lastModifiedSecret"] = core.RandHex(20), core.Time(time.Now())
	}
	delete(u, "projectName")
	delete(u, "rotateSecret")
	maps.Copy(w, u)
	return map[string]any{"webhook": snap(w)}, nil
}

func (s *Service) deleteWebhook(c *core.Ctx, in *struct{ ProjectName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.project(c, in.ProjectName)
	if err != nil {
		return nil, err
	}
	delete(p, "webhook")
	return nil, nil
}

func (s *Service) createReportGroup(c *core.Ctx, in *map[string]any) (any, error) {
	g := *in
	name, _ := g["name"].(string)
	if !projectName.MatchString(name) {
		return nil, invalid("Invalid report group name: %q", name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k, arn := key(c, "report-group", name), c.ARN("codebuild", "report-group/"+name)
	if s.st.Reports[k] != nil {
		return nil, exists("Report group already exists: %s", arn)
	}
	now := core.Time(time.Now())
	g["arn"], g["created"], g["lastModified"], g["status"] = arn, now, now, "ACTIVE"
	s.st.Reports[k] = g
	return map[string]any{"reportGroup": snap(g)}, nil
}

func (s *Service) batchGetReportGroups(c *core.Ctx, in *struct{ ReportGroupArns []string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	found, missing := []map[string]any{}, []string{}
	for _, arn := range in.ReportGroupArns {
		if g := s.st.Reports[key(c, "report-group", arn)]; g != nil {
			found = append(found, g)
		} else {
			missing = append(missing, arn)
		}
	}
	return map[string]any{"reportGroups": snap(found), "reportGroupsNotFound": missing}, nil
}

func (s *Service) updateReportGroup(c *core.Ctx, in *map[string]any) (any, error) {
	u := *in
	arn, _ := u["arn"].(string)
	s.mu.Lock()
	defer s.mu.Unlock()
	g := s.st.Reports[key(c, "report-group", arn)]
	if g == nil {
		return nil, notFound("Report group %s does not exist", arn)
	}
	maps.Copy(g, u)
	g["lastModified"] = core.Time(time.Now())
	return map[string]any{"reportGroup": snap(g)}, nil
}

func (s *Service) deleteReportGroup(c *core.Ctx, in *struct{ Arn string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.st.Reports, key(c, "report-group", in.Arn))
	return nil, nil
}

func (s *Service) listReportGroups(c *core.Ctx, _ *struct{}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	arns := []any{}
	for _, k := range core.SortedKeys(s.st.Reports) {
		if strings.HasPrefix(k, c.Account+"/"+c.Region+"/") {
			arns = append(arns, s.st.Reports[k]["arn"])
		}
	}
	return map[string]any{"reportGroups": arns}, nil
}

// ---- builds ----

// startInput is StartBuild's request (the overrides pogi applies), kept for RetryBuild.
type startInput struct {
	ProjectName                  string
	SourceVersion                string
	SourceTypeOverride           string
	SourceLocationOverride       string
	BuildspecOverride            string
	GitCloneDepthOverride        *int
	ImageOverride                string
	ComputeTypeOverride          string
	EnvironmentTypeOverride      string
	PrivilegedModeOverride       *bool
	EnvironmentVariablesOverride []EnvVar
	ArtifactsOverride            *Artifacts
	ServiceRoleOverride          string
	TimeoutInMinutesOverride     int
}

func (s *Service) startBuild(c *core.Ctx, in *startInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.start(c, in)
	if err != nil {
		return nil, err
	}
	return map[string]any{"build": snap(b)}, nil
}

func (s *Service) retryBuild(c *core.Ctx, in *struct{ Id string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	start := s.st.Starts[key(c, "build", in.Id)]
	if start == nil {
		return nil, notFound("Build %s does not exist", in.Id)
	}
	b, err := s.start(c, start)
	if err != nil {
		return nil, err
	}
	return map[string]any{"build": snap(b)}, nil
}

// job is what a build's run needs, copied out of the state.
type job struct {
	key, account, region, project, uuid, logs string
	b                                         Build     // as it started
	art                                       Artifacts // where its artifacts go
}

// start creates a build of a project with the request's overrides and runs
// it in the background. Lock held.
func (s *Service) start(c *core.Ctx, in *startInput) (*Build, error) {
	p, err := s.project(c, in.ProjectName)
	if err != nil {
		return nil, err
	}
	var v struct {
		Name                   string
		Source                 Source
		Environment            Environment
		Artifacts              Artifacts
		ServiceRole            string
		SourceVersion          string
		TimeoutInMinutes       int
		QueuedTimeoutInMinutes int
		EncryptionKey          string
		LogsConfig             struct {
			CloudWatchLogs struct{ Status, GroupName, StreamName string }
		}
	}
	json.Unmarshal(snap(p), &v)
	v.Source.Type, v.Source.Location = cmp.Or(in.SourceTypeOverride, v.Source.Type), cmp.Or(in.SourceLocationOverride, v.Source.Location)
	v.Source.Buildspec = cmp.Or(in.BuildspecOverride, v.Source.Buildspec)
	if in.GitCloneDepthOverride != nil {
		v.Source.GitCloneDepth = *in.GitCloneDepthOverride
	}
	env := &v.Environment
	env.Image, env.ComputeType, env.Type = cmp.Or(in.ImageOverride, env.Image), cmp.Or(in.ComputeTypeOverride, env.ComputeType), cmp.Or(in.EnvironmentTypeOverride, env.Type)
	if in.PrivilegedModeOverride != nil {
		env.PrivilegedMode = *in.PrivilegedModeOverride
	}
	for _, o := range in.EnvironmentVariablesOverride {
		env.EnvironmentVariables = append(slices.DeleteFunc(env.EnvironmentVariables, func(e EnvVar) bool { return e.Name == o.Name }), o)
	}
	if in.ArtifactsOverride != nil {
		v.Artifacts = *in.ArtifactsOverride
	}

	pk, id, now := key(c, "project", v.Name), core.UUID(), core.Time(time.Now())
	s.st.Numbers[pk]++
	b := &Build{ID: v.Name + ":" + id, Arn: c.ARN("codebuild", "build/"+v.Name+":"+id), BuildNumber: s.st.Numbers[pk], StartTime: now,
		CurrentPhase: "QUEUED", BuildStatus: "IN_PROGRESS", ProjectName: v.Name, SourceVersion: cmp.Or(in.SourceVersion, v.SourceVersion),
		Source: v.Source, Environment: v.Environment, ServiceRole: cmp.Or(in.ServiceRoleOverride, v.ServiceRole),
		TimeoutInMinutes: cmp.Or(in.TimeoutInMinutesOverride, v.TimeoutInMinutes, 60), QueuedTimeoutInMinutes: cmp.Or(v.QueuedTimeoutInMinutes, 480),
		Initiator: c.Principal[strings.LastIndexAny(c.Principal, ":/")+1:], EncryptionKey: v.EncryptionKey,
		Phases: []*Phase{{Type: "SUBMITTED", Status: "SUCCEEDED", Start: now, End: &now, Duration: new(int64)}, {Type: "QUEUED", Start: now}}}
	j := &job{key: c.Account + "/" + c.Region + "/" + b.ID, account: c.Account, region: c.Region, project: v.Name, uuid: id, art: v.Artifacts}
	if cw := v.LogsConfig.CloudWatchLogs; cw.Status != "DISABLED" {
		group, stream := cmp.Or(cw.GroupName, "/aws/codebuild/"+v.Name), id
		if cw.StreamName != "" {
			stream = cw.StreamName + "/" + id
		}
		j.logs = c.ARN("logs", "log-group:"+group+":log-stream:"+stream)
		b.Logs = &Logs{GroupName: group, StreamName: stream, Arn: j.logs,
			DeepLink: "https://console.aws.amazon.com/cloudwatch/home?region=" + c.Region + "#logEvent:group=" + group + ";stream=" + stream}
	}
	j.b = *b
	s.st.Builds[j.key], s.st.Starts[j.key] = b, in
	ctx, stop := context.WithCancelCause(context.Background())
	s.runs[j.key] = stop
	s.wg.Add(1)
	core.Go("codebuild.run", func() { s.run(ctx, j) })
	return b, nil
}

func (s *Service) stopBuild(c *core.Ctx, in *struct{ Id string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(c, "build", in.Id)
	b := s.st.Builds[k]
	if b == nil {
		return nil, notFound("Build %s does not exist", in.Id)
	}
	if stop := s.runs[k]; stop != nil {
		stop(errStopped)
	}
	return map[string]any{"build": snap(b)}, nil
}

func (s *Service) batchGetBuilds(c *core.Ctx, in *struct{ Ids []string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	found, missing := []*Build{}, []string{}
	for _, id := range in.Ids {
		if b := s.st.Builds[key(c, "build", id)]; b != nil {
			found = append(found, b)
		} else {
			missing = append(missing, id)
		}
	}
	return map[string]any{"builds": snap(found), "buildsNotFound": missing}, nil
}

func (s *Service) listBuilds(c *core.Ctx, in *struct{ SortOrder string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ids(c, "", in.SortOrder), nil
}

func (s *Service) listBuildsForProject(c *core.Ctx, in *struct{ ProjectName, SortOrder string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.project(c, in.ProjectName); err != nil {
		return nil, err
	}
	return s.ids(c, in.ProjectName, in.SortOrder), nil
}

// ids lists the ids of builds, of one project or all, newest first unless
// ASCENDING. Lock held.
// ponytail: one page; CodeBuild pages by 100.
func (s *Service) ids(c *core.Ctx, project, order string) map[string]any {
	var list []*Build
	for k, b := range s.st.Builds {
		if strings.HasPrefix(k, c.Account+"/"+c.Region+"/") && (project == "" || b.ProjectName == project) {
			list = append(list, b)
		}
	}
	slices.SortFunc(list, func(a, b *Build) int {
		return cmp.Or(time.Time(b.StartTime).Compare(time.Time(a.StartTime)), cmp.Compare(b.BuildNumber, a.BuildNumber))
	})
	if order == "ASCENDING" {
		slices.Reverse(list)
	}
	ids := []string{}
	for _, b := range list {
		ids = append(ids, b.ID)
	}
	return map[string]any{"ids": ids}
}

func (s *Service) batchDeleteBuilds(c *core.Ctx, in *struct{ Ids []string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	deleted, kept := []string{}, []map[string]string{}
	for _, id := range in.Ids {
		switch k := key(c, "build", id); {
		case s.st.Builds[k] == nil:
			kept = append(kept, map[string]string{"id": id, "statusCode": "BUILD_NOT_FOUND"})
		case !s.st.Builds[k].BuildComplete:
			kept = append(kept, map[string]string{"id": id, "statusCode": "BUILD_IN_PROGRESS"})
		default:
			delete(s.st.Builds, k)
			delete(s.st.Starts, k)
			deleted = append(deleted, id)
		}
	}
	return map[string]any{"buildsDeleted": deleted, "buildsNotDeleted": kept}, nil
}

// ---- running a build ----

// buildspec is the part of a version 0.2 buildspec that pogi runs.
type buildspec struct {
	Env struct {
		Shell          string            `yaml:"shell"`
		Variables      map[string]string `yaml:"variables"`
		ParameterStore map[string]string `yaml:"parameter-store"`
		SecretsManager map[string]string `yaml:"secrets-manager"`
	} `yaml:"env"`
	Phases map[string]struct {
		OnFailure string   `yaml:"on-failure"`
		Commands  []string `yaml:"commands"`
		Finally   []string `yaml:"finally"`
	} `yaml:"phases"`
	Artifacts struct {
		Files         []string `yaml:"files"`
		BaseDirectory string   `yaml:"base-directory"`
		DiscardPaths  bool     `yaml:"discard-paths"`
	} `yaml:"artifacts"`
}

// run takes a build through its phases to COMPLETED.
func (s *Service) run(ctx context.Context, j *job) {
	defer s.wg.Done()
	ctx, stop := context.WithTimeoutCause(ctx, time.Duration(j.b.TimeoutInMinutes)*time.Minute, errTimedOut)
	defer stop()
	status := s.steps(ctx, j)
	s.mark(j.key, "", "FINALIZING", nil)
	s.mark(j.key, "SUCCEEDED", "COMPLETED", nil)
	s.update(j.key, func(b *Build, now core.Time) { b.BuildStatus, b.BuildComplete, b.EndTime = status, true, &now })
	s.mu.Lock()
	delete(s.runs, j.key)
	s.mu.Unlock()
}

// steps provisions the container, puts the source in, runs the buildspec's
// phases and uploads the artifacts; it returns the build status.
func (s *Service) steps(ctx context.Context, j *job) string {
	s.mark(j.key, "SUCCEEDED", "PROVISIONING", nil)
	d := s.hub.Docker()
	if !d.Available() {
		return s.fail(ctx, j.key, "CLIENT_ERROR", NoDocker)
	}
	host := d.HostAddr()
	ct := &docker.Container{Name: docker.Name("codebuild", j.project+"-"+j.uuid), Image: image(s.hub.Cfg.DockerImages, j.b.Environment.Image),
		Service: "codebuild", Resource: j.project, Entrypoint: []string{"sh"}, Cmd: []string{"/codebuild/output/pogi-build.sh"}}
	if host == "127.0.0.1" { // a Linux engine without a VM reaches pogi's loopback only from the host network
		ct.Network = "host"
	}
	id, err := d.Create(ctx, ct)
	if err != nil {
		return s.fail(ctx, j.key, "CLIENT_ERROR", err.Error())
	}
	defer d.Remove(context.WithoutCancel(ctx), id)

	s.mark(j.key, "SUCCEEDED", "DOWNLOAD_SOURCE", nil)
	bs, err := s.prepare(ctx, d, id, ct, j, host)
	if err == nil {
		err = d.Start(ctx, id)
	}
	if err != nil {
		code, msg := "CLIENT_ERROR", err.Error()
		var e *core.Error
		if errors.As(err, &e) { // YAML_FILE_ERROR
			code, msg = e.Code, e.Msg
		}
		return s.fail(ctx, j.key, code, msg)
	}
	s.mark(j.key, "SUCCEEDED", "", nil)

	done := make(chan struct{})
	core.Go("codebuild.follow", func() { s.follow(j, id, done) })
	code, err := d.Wait(ctx, id)
	if err != nil {
		d.Remove(context.WithoutCancel(ctx), id) // stopped or timed out
	}
	<-done
	if err != nil {
		return s.fail(ctx, j.key, "CLIENT_ERROR", err.Error())
	}
	// a phase the script left by exiting
	s.mark(j.key, "FAILED", "", &Context{"COMMAND_EXECUTION_ERROR", "exit status " + strconv.Itoa(code)})
	status := "SUCCEEDED"
	if code != 0 {
		status = "FAILED"
	}
	s.mu.Lock()
	b := s.st.Builds[j.key]
	posted := b != nil && slices.ContainsFunc(b.Phases, func(p *Phase) bool { return p.Type == "POST_BUILD" })
	s.mu.Unlock()
	if !posted { // CodeBuild uploads artifacts once POST_BUILD has run
		return status
	}
	s.mark(j.key, "", "UPLOAD_ARTIFACTS", nil)
	out, err := s.upload(ctx, d, id, j, bs)
	if err != nil {
		s.log(j, "[Container] "+err.Error())
		return s.fail(ctx, j.key, "CLIENT_ERROR", err.Error())
	}
	s.update(j.key, func(b *Build, _ core.Time) { b.Artifacts = out })
	s.mark(j.key, "SUCCEEDED", "", nil)
	return status
}

// fail ends the open phase as failed, or as stopped or timed out when that
// is why, and returns the build status.
func (s *Service) fail(ctx context.Context, k, code, msg string) string {
	if cause := context.Cause(ctx); cause != nil {
		s.mark(k, cause.Error(), "", nil)
		return cause.Error()
	}
	s.mark(k, "FAILED", "", &Context{code, msg})
	return "FAILED"
}

// update changes a build under the lock, unless it was deleted meanwhile.
func (s *Service) update(k string, f func(b *Build, now core.Time)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if b := s.st.Builds[k]; b != nil {
		f(b, core.Time(time.Now()))
	}
}

// mark ends the open phase with status (when status is not empty) and
// starts the next one (when next is not empty).
func (s *Service) mark(k, status, next string, why *Context) {
	s.update(k, func(b *Build, now core.Time) {
		if p := b.Phases[len(b.Phases)-1]; status != "" && p.End == nil {
			d := int64(time.Time(now).Sub(time.Time(p.Start)).Seconds())
			p.Status, p.End, p.Duration = status, &now, &d
			if why != nil {
				p.Contexts = []Context{*why}
			}
		}
		if next != "" {
			b.Phases = append(b.Phases, &Phase{Type: next, Start: now})
			b.CurrentPhase = next
		}
	})
}

// image is the container image of a build image. CodeBuild's own (gigabytes,
// and not public) become small stand-ins unless pogi.yaml maps them with
// docker.images codebuild.<image>, e.g. codebuild.standard.
func image(overrides map[string]string, img string) string {
	name := img[strings.LastIndex(img, "/")+1:]
	name, _, _ = strings.Cut(name, ":")
	if o := cmp.Or(overrides["codebuild."+img], overrides["codebuild."+name]); o != "" {
		return o
	}
	switch {
	case strings.HasPrefix(img, "aws/codebuild/amazonlinux"):
		return "amazonlinux:2023"
	case strings.HasPrefix(img, "aws/codebuild/"):
		return "ubuntu:24.04"
	}
	return img
}

// prepare puts the source into the created container, reads the buildspec
// and writes the script that runs it.
func (s *Service) prepare(ctx context.Context, d *docker.Client, id string, ct *docker.Container, j *job, host string) (*buildspec, error) {
	if err := d.CopyTo(ctx, id, "/", tarOne("codebuild/output/src/", "")); err != nil {
		return nil, err
	}
	version, err := s.fetch(ctx, d, id, ct, j)
	if err != nil {
		return nil, err
	}
	text, err := s.buildspec(ctx, d, id, j)
	var bs buildspec
	if err == nil {
		err = yaml.Unmarshal([]byte(text), &bs)
	}
	if err != nil {
		return nil, core.Errorf(0, "YAML_FILE_ERROR", "%v", err)
	}
	env, err := s.environment(j, &bs, version, host)
	if err != nil {
		return nil, err
	}
	s.update(j.key, func(b *Build, _ core.Time) { b.ResolvedSourceVersion = version })
	return &bs, d.CopyTo(ctx, id, "/", tarOne("codebuild/output/pogi-build.sh", script(&bs, env)))
}

// fetch puts the source into the container and returns the version it resolved to.
func (s *Service) fetch(ctx context.Context, d *docker.Client, id string, ct *docker.Container, j *job) (string, error) {
	switch t, loc := j.b.Source.Type, j.b.Source.Location; {
	case t == "NO_SOURCE":
		return "", nil
	case t == "S3" && filepath.IsAbs(loc): // ponytail: a local directory, to run a buildspec on a working tree
		return "", put(ctx, d, id, os.DirFS(loc))
	case t == "S3":
		body, version, err := s.object(j, loc)
		if err != nil {
			return "", err
		}
		z, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
		if err != nil {
			return "", fmt.Errorf("the source s3://%s is not a zip file: %v", loc, err)
		}
		return version, put(ctx, d, id, z)
	case t == "CODEPIPELINE":
		return "", errors.New("pogi does not run CODEPIPELINE sources")
	}
	return s.clone(ctx, d, id, ct, j)
}

// put copies the files of fsys into the container's source directory.
func put(ctx context.Context, d *docker.Client, id string, fsys fs.FS) error {
	r, w := io.Pipe()
	core.Go("codebuild.put", func() {
		tw := tar.NewWriter(w)
		err := tw.AddFS(fsys)
		if err == nil {
			err = tw.Close()
		}
		w.CloseWithError(err)
	})
	return d.CopyTo(ctx, id, src, r)
}

// clone fetches a git source (GITHUB, CODECOMMIT, BITBUCKET…) in a throwaway
// container of the build image, which needs git, and copies it into the
// build's container.
func (s *Service) clone(ctx context.Context, d *docker.Client, id string, ct *docker.Container, j *job) (string, error) {
	ver := j.b.SourceVersion
	if n, ok := strings.CutPrefix(ver, "pr/"); ok {
		ver = "refs/pull/" + n + "/head"
	}
	depth := ""
	if n := j.b.Source.GitCloneDepth; n > 0 {
		depth = "--depth " + strconv.Itoa(n)
	}
	git := &docker.Container{Name: ct.Name + "-source", Image: ct.Image, Service: ct.Service, Resource: ct.Resource, Network: ct.Network,
		Env: []string{"URL=" + j.b.Source.Location, "VER=" + ver}, Entrypoint: []string{"sh"}, Cmd: []string{"-c", `
command -v git >/dev/null || { echo "the build image has no git to clone $URL"; exit 1; }
git clone -q ` + depth + ` "$URL" ` + src + ` && cd ` + src + ` &&
if [ -n "$VER" ]; then git checkout -q "$VER" 2>/dev/null || { git fetch -q origin "$VER" && git checkout -q FETCH_HEAD; }; fi &&
git rev-parse HEAD`}}
	gid, err := d.Run(ctx, git)
	if err != nil {
		return "", err
	}
	defer d.Remove(context.WithoutCancel(ctx), gid)
	code, err := d.Wait(ctx, gid)
	var out []string
	d.Logs(ctx, gid, false, func(l string) { out = append(out, l) })
	if err == nil && (code != 0 || len(out) == 0) {
		err = fmt.Errorf("git clone %s failed: %s", j.b.Source.Location, strings.Join(out, "; "))
	}
	if err != nil {
		return "", err
	}
	tr, err := d.CopyFrom(ctx, gid, src)
	if err != nil {
		return "", err
	}
	defer tr.Close()
	return out[len(out)-1], d.CopyTo(ctx, id, path.Dir(src), tr)
}

// object reads bucket/key from pogi's S3 and returns its version id.
func (s *Service) object(j *job, loc string) ([]byte, string, error) {
	status, h, body := s.hub.Do(j.account, j.region, "s3", http.MethodGet, (&url.URL{Path: "/" + loc}).EscapedPath(), nil, nil)
	if status != http.StatusOK {
		return nil, "", fmt.Errorf("cannot get s3://%s (HTTP %d)", loc, status)
	}
	return body, h.Get("X-Amz-Version-Id"), nil
}

// buildspec returns the build's buildspec: inline, an S3 object (by ARN) or
// a file in the source, buildspec.yml unless named.
func (s *Service) buildspec(ctx context.Context, d *docker.Client, id string, j *job) (string, error) {
	spec := cmp.Or(j.b.Source.Buildspec, "buildspec.yml")
	if strings.Contains(spec, "\n") || strings.Contains(spec, ": ") {
		return spec, nil
	}
	if _, _, _, res, ok := core.ParseARN(spec); ok {
		b, _, err := s.object(j, res)
		return string(b), err
	}
	rc, err := d.CopyFrom(ctx, id, path.Join(src, spec))
	if err != nil {
		return "", fmt.Errorf("stat %s: no such file or directory", path.Join(src, spec))
	}
	defer rc.Close()
	tr := tar.NewReader(rc)
	if _, err := tr.Next(); err != nil {
		return "", err
	}
	b, err := io.ReadAll(tr)
	return string(b), err
}

// environment is the build's variables in CodeBuild's order of precedence,
// lowest first: pogi's endpoint and the service role's credentials, the
// buildspec's, the project's with the StartBuild overrides, then CODEBUILD_*.
func (s *Service) environment(j *job, bs *buildspec, version, host string) ([][2]string, error) {
	u, _ := url.Parse(cmp.Or(s.hub.URL, "http://localhost:"+strconv.Itoa(cmp.Or(s.hub.Cfg.Port, 7644))))
	out := append([][2]string{{"AWS_ENDPOINT_URL", "http://" + host + ":" + u.Port()}, {"AWS_REGION", j.region}, {"AWS_DEFAULT_REGION", j.region}},
		creds(s.hub, j.b.ServiceRole, j.region, j.account, "AWSCodeBuild-"+j.uuid)...)
	var vars []EnvVar
	for kind, m := range map[string]map[string]string{"PLAINTEXT": bs.Env.Variables, "PARAMETER_STORE": bs.Env.ParameterStore, "SECRETS_MANAGER": bs.Env.SecretsManager} {
		for k, v := range m {
			vars = append(vars, EnvVar{k, v, kind})
		}
	}
	slices.SortFunc(vars, func(a, b EnvVar) int { return strings.Compare(a.Name, b.Name) })
	for _, ev := range append(vars, j.b.Environment.EnvironmentVariables...) { // the project's win
		v, err := s.resolve(j, ev)
		if err != nil {
			return nil, err
		}
		out = append(out, [2]string{ev.Name, v})
	}
	return append(out, [][2]string{{"CODEBUILD_BUILD_ID", j.b.ID}, {"CODEBUILD_BUILD_ARN", j.b.Arn},
		{"CODEBUILD_BUILD_NUMBER", strconv.FormatInt(j.b.BuildNumber, 10)}, {"CODEBUILD_BUILD_IMAGE", j.b.Environment.Image},
		{"CODEBUILD_INITIATOR", j.b.Initiator}, {"CODEBUILD_SOURCE_VERSION", j.b.SourceVersion}, {"CODEBUILD_RESOLVED_SOURCE_VERSION", version},
		{"CODEBUILD_SRC_DIR", src}, {"CODEBUILD_LOG_PATH", j.uuid}, {"CODEBUILD_BUILD_SUCCEEDING", "1"},
		{"CODEBUILD_START_TIME", strconv.FormatInt(time.Time(j.b.StartTime).UnixMilli(), 10)}}...), nil
}

// resolve is a variable's value: as given, or read from Parameter Store or
// Secrets Manager ("secret-id:json-key:version-stage:version-id", where the
// id may be an ARN).
func (s *Service) resolve(j *job, ev EnvVar) (string, error) {
	switch ev.Type {
	case "PARAMETER_STORE":
		var out struct{ Parameter struct{ Value string } }
		if err := s.hub.Call(j.account, j.region, "AmazonSSM", "GetParameter", map[string]any{"Name": ev.Value, "WithDecryption": true}, &out); err != nil {
			return "", fmt.Errorf("parameter %s for %s: %v", ev.Value, ev.Name, err)
		}
		return out.Parameter.Value, nil
	case "SECRETS_MANAGER":
		p := strings.Split(ev.Value, ":")
		n := 1
		if p[0] == "arn" {
			n = min(7, len(p))
		}
		f := append(p[n:], "", "", "")
		in := map[string]any{"SecretId": strings.Join(p[:n], ":")}
		if f[1] != "" {
			in["VersionStage"] = f[1]
		}
		if f[2] != "" {
			in["VersionId"] = f[2]
		}
		var out struct{ SecretString string }
		if err := s.hub.Call(j.account, j.region, "secretsmanager", "GetSecretValue", in, &out); err != nil {
			return "", fmt.Errorf("secret %s for %s: %v", ev.Value, ev.Name, err)
		}
		if f[0] == "" {
			return out.SecretString, nil
		}
		var m map[string]any
		json.Unmarshal([]byte(out.SecretString), &m)
		v, ok := m[f[0]]
		if !ok {
			return "", fmt.Errorf("secret %s for %s has no key %s", ev.Value, ev.Name, f[0])
		}
		if str, ok := v.(string); ok {
			return str, nil
		}
		return string(snap(v)), nil
	}
	return ev.Value, nil
}

var ident = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// script is the build as one shell program: the environment, then each
// phase's commands, announced the way CodeBuild logs them, which is also how
// pogi follows the phases. A command runs in a subshell, so exit fails only
// that command; what it exports and where it cds carry on, as on CodeBuild.
func script(bs *buildspec, env [][2]string) string {
	var b strings.Builder
	if bs.Env.Shell == "bash" {
		b.WriteString("[ -n \"$BASH_VERSION\" ] || exec bash \"$0\"\n")
	}
	for _, kv := range env {
		if ident.MatchString(kv[0]) {
			fmt.Fprintf(&b, "export %s=%s\n", kv[0], quote(kv[1]))
		}
	}
	b.WriteString(`cd "$CODEBUILD_SRC_DIR"
pogi_e=/codebuild/output/pogi-env pogi_p=/codebuild/output/pogi-pwd pogi_failed=0 pogi_nl='
'
export -p >$pogi_e; pwd >$pogi_p
pogi_cmd() {
	[ $pogi_rc -eq 0 ] || return 0
	printf '[Container] Running command %s\n' "$pogi_c"
	(eval "$pogi_c"; pogi_s=$?; export -p >$pogi_e; pwd >$pogi_p; exit $pogi_s)
	pogi_rc=$?
	[ $pogi_rc -eq 0 ] || printf '[Container] Command did not exit successfully %s exit status %s\n' "${pogi_c%%"$pogi_nl"*}" $pogi_rc
}
`)
	cmd := func(c string) { // the state is read back outside the function: bash's export -p declares, locally in one
		fmt.Fprintf(&b, "pogi_c=%s; pogi_cmd; . $pogi_e; cd \"$(cat $pogi_p)\"\n", quote(c))
	}
	for _, name := range []string{"install", "pre_build", "build", "post_build"} {
		ph, phase := bs.Phases[name], strings.ToUpper(name)
		if name == "post_build" {
			b.WriteString("export CODEBUILD_BUILD_SUCCEEDING=$((1 - pogi_failed))\n")
		}
		fmt.Fprintf(&b, "echo '[Container] Entering phase %s'\npogi_rc=0\n", phase)
		for _, c := range ph.Commands {
			cmd(c)
		}
		if len(ph.Finally) > 0 { // finally runs whatever the commands did
			b.WriteString("pogi_first=$pogi_rc; pogi_rc=0\n")
			for _, c := range ph.Finally {
				cmd(c)
			}
			b.WriteString("[ $pogi_rc -ne 0 ] || pogi_rc=$pogi_first\n")
		}
		fmt.Fprintf(&b, "if [ $pogi_rc -eq 0 ]; then echo '[Container] Phase complete: %[1]s State: SUCCEEDED'; "+
			"else echo '[Container] Phase complete: %[1]s State: FAILED'; pogi_failed=1; fi\n", phase)
		// by default a failed INSTALL or PRE_BUILD ends the build, a failed BUILD still runs POST_BUILD
		onFailure := strings.ToUpper(ph.OnFailure)
		if onFailure == "ABORT" || onFailure == "" && (name == "install" || name == "pre_build") {
			b.WriteString("[ $pogi_rc -eq 0 ] || exit 1\n")
		}
	}
	b.WriteString("exit $pogi_failed\n")
	return b.String()
}

// quote is s as one single-quoted shell word.
func quote(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }

// tarOne is a tar archive of one file, or of one directory when name ends in /.
func tarOne(name, content string) io.Reader {
	var b bytes.Buffer
	tw := tar.NewWriter(&b)
	h := &tar.Header{Name: name, Mode: 0o755, Size: int64(len(content)), Typeflag: tar.TypeReg}
	if strings.HasSuffix(name, "/") {
		h.Typeflag = tar.TypeDir
	}
	tw.WriteHeader(h)
	io.WriteString(tw, content)
	tw.Close()
	return &b
}

// follow sends the build's output to CloudWatch Logs and records the phases
// the script announces, until the container stops.
func (s *Service) follow(j *job, id string, done chan struct{}) {
	defer close(done)
	var why *Context
	s.hub.Docker().Logs(context.Background(), id, true, func(line string) {
		if p, ok := strings.CutPrefix(line, "[Container] Entering phase "); ok {
			s.mark(j.key, "", p, nil)
			why = nil
		}
		if cmd, ok := strings.CutPrefix(line, "[Container] Command did not exit successfully "); ok {
			if i := strings.LastIndex(cmd, " exit status "); i >= 0 {
				why = &Context{"COMMAND_EXECUTION_ERROR", "Error while executing command: " + cmd[:i] + ". Reason: " + cmd[i+1:]}
			}
		}
		if rest, ok := strings.CutPrefix(line, "[Container] Phase complete: "); ok {
			_, st, _ := strings.Cut(rest, " State: ")
			if st == "SUCCEEDED" {
				why = nil
			}
			s.mark(j.key, st, "", why)
		}
		s.log(j, line)
	})
}

func (s *Service) log(j *job, line string) {
	if j.logs != "" {
		s.hub.Deliver(j.logs, &core.Message{Body: line})
	}
}

// upload sends the files the buildspec names to the project's S3 artifacts
// location: one zip, or the files one by one.
func (s *Service) upload(ctx context.Context, d *docker.Client, id string, j *job, bs *buildspec) (*Output, error) {
	a := j.art
	if a.Type != "S3" {
		return nil, nil
	}
	files, err := collect(ctx, d, id, bs)
	if err != nil {
		return nil, err
	}
	ns := ""
	if a.NamespaceType == "BUILD_ID" {
		ns = j.uuid
	}
	key := path.Join(a.Path, ns, cmp.Or(a.Name, j.project))
	out := &Output{Location: core.GlobalARN("s3", j.region, "", a.Location+"/"+key)}
	if a.Packaging != "ZIP" {
		for _, name := range slices.Sorted(maps.Keys(files)) {
			if err := s.putObject(j, a.Location, key+"/"+name, files[name]); err != nil {
				return nil, err
			}
		}
		return out, nil
	}
	var b bytes.Buffer
	zw := zip.NewWriter(&b)
	for _, name := range slices.Sorted(maps.Keys(files)) {
		w, _ := zw.Create(name)
		w.Write(files[name])
	}
	zw.Close()
	sha, md := sha256.Sum256(b.Bytes()), md5.Sum(b.Bytes())
	out.Sha256sum, out.Md5sum = hex.EncodeToString(sha[:]), hex.EncodeToString(md[:])
	return out, s.putObject(j, a.Location, key, b.Bytes())
}

// collect reads the files that match the buildspec's artifacts out of the
// stopped container, by path under the base directory (or by name with
// discard-paths).
func collect(ctx context.Context, d *docker.Client, id string, bs *buildspec) (map[string][]byte, error) {
	a := bs.Artifacts
	var globs []*regexp.Regexp
	for _, f := range a.Files {
		globs = append(globs, glob(strings.TrimPrefix(f, "./")))
	}
	rc, err := d.CopyFrom(ctx, id, path.Join(src, a.BaseDirectory))
	if err != nil {
		return nil, fmt.Errorf("no matching base directory path found for %s", a.BaseDirectory)
	}
	defer rc.Close()
	files := map[string][]byte{}
	for tr := tar.NewReader(rc); ; {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		_, rel, _ := strings.Cut(h.Name, "/") // the archive's root is the directory itself
		if h.Typeflag != tar.TypeReg || !slices.ContainsFunc(globs, func(g *regexp.Regexp) bool { return g.MatchString(rel) }) {
			continue
		}
		name := rel
		if a.DiscardPaths {
			name = path.Base(rel)
		}
		if files[name], err = io.ReadAll(tr); err != nil {
			return nil, err
		}
	}
	if len(files) == 0 {
		return nil, errors.New("no matching artifact paths found")
	}
	return files, nil
}

// glob compiles a buildspec file pattern: ** spans directories, * and ? do not.
func glob(p string) *regexp.Regexp {
	var b strings.Builder
	for i := 0; i < len(p); i++ {
		switch {
		case strings.HasPrefix(p[i:], "**/"):
			b.WriteString("(.*/)?")
			i += 2
		case strings.HasPrefix(p[i:], "**"):
			b.WriteString(".*")
			i++
		case p[i] == '*':
			b.WriteString("[^/]*")
		case p[i] == '?':
			b.WriteString("[^/]")
		default:
			b.WriteString(regexp.QuoteMeta(p[i : i+1]))
		}
	}
	return regexp.MustCompile("^" + b.String() + "$")
}

func (s *Service) putObject(j *job, bucket, key string, data []byte) error {
	status, _, _ := s.hub.Do(j.account, j.region, "s3", http.MethodPut, (&url.URL{Path: "/" + bucket + "/" + key}).EscapedPath(), nil, data)
	if status != http.StatusOK {
		return fmt.Errorf("cannot put s3://%s/%s (HTTP %d)", bucket, key, status)
	}
	return nil
}

// creds are session credentials minted for a role, the way CodeBuild gives
// a build its service role; without one, the account's own key.
func creds(h *core.Hub, role, region, account, session string) [][2]string {
	_, _, acct, res, ok := core.ParseARN(role)
	if !ok || !strings.HasPrefix(res, "role/") {
		return [][2]string{{"AWS_ACCESS_KEY_ID", account}, {"AWS_SECRET_ACCESS_KEY", "test"}}
	}
	akid, secret := "ASIA"+core.RandString(16, core.UpperNum), core.RandString(40, core.AlphaNum)
	h.AddIdentity(akid, core.Identity{Account: acct, UserID: "AROA" + core.RandString(17, core.UpperNum) + ":" + session,
		ARN: core.GlobalARN("sts", region, acct, "assumed-role/"+path.Base(res)+"/"+session), Secret: secret})
	return [][2]string{{"AWS_ACCESS_KEY_ID", akid}, {"AWS_SECRET_ACCESS_KEY", secret}, {"AWS_SESSION_TOKEN", core.RandString(180, core.AlphaNum)}}
}

// ---- lifecycle and console ----

// Close stops the builds still running, removing their containers.
func (s *Service) Close() error {
	s.mu.Lock()
	for _, stop := range s.runs {
		stop(errStopped)
	}
	s.mu.Unlock()
	s.wg.Wait()
	return nil
}

func (s *Service) Reset() {
	s.mu.Lock()
	for _, stop := range s.runs {
		stop(errStopped)
	}
	s.st = newState()
	s.mu.Unlock()
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

// Load restores state; builds that were running have lost their containers and fail.
func (s *Service) Load(b []byte) error {
	st := newState()
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	now := core.Time(time.Now())
	for _, b := range st.Builds {
		if !b.BuildComplete {
			b.BuildStatus, b.BuildComplete, b.EndTime = "FAILED", true, &now
		}
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	projects := core.Table{Name: "Projects", Columns: []string{"Project", "Region", "Source", "Image"}}
	for _, k := range core.SortedKeys(s.st.Projects) {
		var v struct {
			Name        string
			Source      Source
			Environment Environment
		}
		json.Unmarshal(snap(s.st.Projects[k]), &v)
		projects.Rows = append(projects.Rows, []string{v.Name, strings.Split(k, "/")[1], v.Source.Type, v.Environment.Image})
	}
	builds := core.Table{Name: "Builds", Columns: []string{"Build", "Status", "Phase", "Started"}}
	for _, k := range core.SortedKeys(s.st.Builds) {
		b := s.st.Builds[k]
		builds.Rows = append(builds.Rows, []string{b.ID, b.BuildStatus, b.CurrentPhase, time.Time(b.StartTime).Format(time.RFC3339)})
	}
	return []core.Table{projects, builds}
}

func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, p := range s.st.Projects {
		if table == "Projects" && strings.HasSuffix(k, "/"+id) {
			return snap(p)
		}
	}
	for k, b := range s.st.Builds {
		if table == "Builds" && strings.HasSuffix(k, "/"+id) {
			return snap(b)
		}
	}
	return nil
}
