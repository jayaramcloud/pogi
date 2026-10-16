package codepipeline

import (
	"archive/zip"
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Execution is a run of a pipeline.
type Execution struct {
	ID        string                       `json:"id"`
	Pipeline  string                       `json:"pipeline"`
	Version   int                          `json:"version"`
	Status    string                       `json:"status"` // InProgress | Stopping | Stopped | Succeeded | Failed
	Summary   string                       `json:"summary,omitempty"`
	Stopped   string                       `json:"stopped,omitempty"` // why it was stopped
	Mode      string                       `json:"mode"`
	Trigger   string                       `json:"trigger"`
	Started   time.Time                    `json:"started"`
	Updated   time.Time                    `json:"updated"`
	Stages    map[string]string            `json:"stages"`    // stage → status
	Actions   []*ActionRun                 `json:"actions"`   // oldest first
	Artifacts map[string]location          `json:"artifacts"` // artifact → where it is
	Outputs   map[string]map[string]string `json:"outputs"`   // namespace → variables; "variables" are the pipeline's
	Revisions []revision                   `json:"revisions,omitempty"`
}

type location struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
}

type artifact struct {
	Name       string   `json:"name"`
	S3location location `json:"s3location"`
}

type revision struct {
	Name            string    `json:"name"`
	Action          string    `json:"actionName"`
	RevisionID      string    `json:"revisionId"`
	RevisionSummary string    `json:"revisionSummary"`
	Created         core.Time `json:"created"`
}

type errorDetails struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// ActionRun is one execution of an action.
type ActionRun struct {
	ID            string            `json:"id"`
	Stage         string            `json:"stage"`
	Name          string            `json:"name"`
	Status        string            `json:"status"` // InProgress | Succeeded | Failed | Abandoned
	Summary       string            `json:"summary,omitempty"`
	External      string            `json:"external,omitempty"` // build, stack, deployment, job or execution
	Token         string            `json:"token,omitempty"`    // a manual approval's
	Job           string            `json:"job,omitempty"`      // a Lambda invoke's
	Result        string            `json:"result,omitempty"`   // what PutApprovalResult or PutJob*Result said
	ResultSummary string            `json:"resultSummary,omitempty"`
	Error         *errorDetails     `json:"error,omitempty"`
	Started       time.Time         `json:"started"`
	Updated       time.Time         `json:"updated"`
	Input         map[string]any    `json:"input"`
	Outputs       []artifact        `json:"outputs,omitempty"`
	Variables     map[string]string `json:"variables,omitempty"`
}

var errStopped = errors.New("the execution was stopped")

// poll is how often a waiting action looks again.
const poll = 250 * time.Millisecond

var varRef = regexp.MustCompile(`#\{([A-Za-z0-9@_-]+)\.([A-Za-z0-9@_-]+)\}`)

// resolve replaces #{namespace.variable} references; lock held.
func (e *Execution) resolve(v string) string {
	return varRef.ReplaceAllStringFunc(v, func(m string) string {
		p := varRef.FindStringSubmatch(m)
		if p[1] == "codepipeline" && p[2] == "PipelineExecutionId" {
			return e.ID
		}
		if val, ok := e.Outputs[p[1]][p[2]]; ok {
			return val
		}
		return m
	})
}

// set changes an execution under the lock.
func (s *Service) set(e *Execution, f func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f()
	e.Updated = time.Now()
}

func (s *Service) stopping(e *Execution) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return e.Status == "Stopping"
}

// run takes an execution through the stages from the one at from; mode
// FAILED_ACTIONS reruns only that stage's failed actions.
func (s *Service) run(account, region string, e *Execution, d decl, from int, mode string) {
	defer s.wg.Done()
	status, summary := "Succeeded", ""
	for _, st := range d.Stages[from:] {
		if s.stopping(e) {
			status = "Stopped"
			break
		}
		s.set(e, func() { e.Stages[st.Name] = "InProgress" })
		err := s.stage(account, region, e, d, st, mode)
		mode = "ALL_ACTIONS"
		if err != nil {
			status, summary = "Failed", err.Error()
			if errors.Is(err, errStopped) {
				status, summary = "Stopped", ""
			}
			s.set(e, func() { e.Stages[st.Name] = status })
			break
		}
		s.set(e, func() { e.Stages[st.Name] = "Succeeded" })
	}
	s.set(e, func() { e.Status, e.Summary = status, cmp.Or(summary, e.Stopped) })
}

// stage runs a stage's actions by run order, those of one run order at once.
func (s *Service) stage(account, region string, e *Execution, d decl, st stage, mode string) error {
	var orders []int
	for _, a := range st.Actions {
		orders = append(orders, cmp.Or(a.RunOrder, 1))
	}
	slices.Sort(orders)
	for _, o := range slices.Compact(orders) {
		if s.stopping(e) {
			return errStopped
		}
		errs := make([]error, len(st.Actions))
		var wg sync.WaitGroup
		for i, a := range st.Actions {
			if cmp.Or(a.RunOrder, 1) != o || mode == "FAILED_ACTIONS" && s.succeeded(e, st.Name, a.Name) {
				continue
			}
			wg.Add(1)
			core.Go("codepipeline.stage", func() {
				defer wg.Done()
				errs[i] = s.action(account, region, e, d, st.Name, a)
			})
		}
		wg.Wait()
		for _, err := range errs {
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Service) succeeded(e *Execution, stage, name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, r := latest([]*Execution{e}, stage, name)
	return r != nil && r.Status == "Succeeded"
}

// job is one run of an action and what its provider needs.
type job struct {
	s               *Service
	account, region string
	e               *Execution
	r               *ActionRun
	d               decl
	a               action
	config          map[string]string // resolved
	inputs          []artifact
}

// action runs one action and records how it went.
func (s *Service) action(account, region string, e *Execution, d decl, stage string, a action) error {
	now := time.Now()
	s.mu.Lock()
	j := &job{s: s, account: account, region: region, e: e, d: d, a: a, config: map[string]string{}}
	for k, v := range a.Configuration {
		j.config[k] = e.resolve(v)
	}
	var missing []string
	for _, in := range a.InputArtifacts {
		loc, ok := e.Artifacts[in.Name]
		if !ok {
			missing = append(missing, in.Name)
		}
		j.inputs = append(j.inputs, artifact{in.Name, loc})
	}
	j.r = &ActionRun{ID: core.UUID(), Stage: stage, Name: a.Name, Status: "InProgress", Started: now, Updated: now,
		Input: map[string]any{"actionTypeId": a.ActionTypeId, "configuration": a.Configuration, "resolvedConfiguration": j.config,
			"roleArn": a.RoleArn, "region": cmp.Or(a.Region, region), "inputArtifacts": j.inputs, "namespace": a.Namespace}}
	e.Actions = append(e.Actions, j.r)
	s.mu.Unlock()

	err := fmt.Errorf("the input artifacts %v were not produced by an earlier action", missing)
	if len(missing) == 0 {
		err = j.do()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := j.r
	r.Updated = time.Now()
	switch {
	case errors.Is(err, errStopped):
		r.Status = "Abandoned"
	case err != nil:
		r.Status, r.Error = "Failed", &errorDetails{"JobFailed", err.Error()}
	default:
		r.Status = "Succeeded"
		for _, o := range r.Outputs {
			e.Artifacts[o.Name] = o.S3location
		}
		if a.Namespace != "" {
			e.Outputs[a.Namespace] = r.Variables
		}
	}
	return err
}

// do runs the action's provider.
func (j *job) do() error {
	t := j.a.ActionTypeId
	switch t.Category + "/" + t.Owner + "/" + t.Provider {
	case "Source/AWS/S3":
		return j.s3Source()
	case "Source/AWS/ECR":
		return j.ecrSource()
	case "Build/AWS/CodeBuild", "Test/AWS/CodeBuild":
		return j.codeBuild()
	case "Deploy/AWS/CloudFormation":
		return j.cloudFormation()
	case "Deploy/AWS/S3":
		return j.s3Deploy()
	case "Deploy/AWS/CodeDeploy":
		return j.codeDeploy()
	case "Deploy/AWS/ECS":
		return j.ecsDeploy()
	case "Approval/AWS/Manual":
		return j.approval()
	case "Invoke/AWS/Lambda":
		return j.lambda()
	case "Invoke/AWS/StepFunctions":
		return j.stepFunctions()
	}
	return fmt.Errorf("pogi does not run %s actions of %s provider %s", t.Category, t.Owner, t.Provider)
}

// ---- helpers ----

func (j *job) call(target, op string, in, out any) error {
	return j.s.hub.Call(j.account, j.region, target, op, in, out)
}

func (j *job) set(f func()) { j.s.set(j.e, f) }

// wait polls check until it is done, the execution stops or pogi closes.
func (j *job) wait(check func() (bool, error)) error {
	for {
		if done, err := check(); done || err != nil {
			return err
		}
		if j.s.stopping(j.e) {
			return errStopped
		}
		select {
		case <-j.s.quit:
			return errStopped
		case <-time.After(poll):
		}
	}
}

// out is a new location in the artifact store, named as CodePipeline names
// them: <pipeline>/<artifact>/<random>, cut to 20 and 10 characters.
func (j *job) out(name string) location {
	bucket := ""
	if st, ok := j.d.ArtifactStores[j.region]; ok {
		bucket = st.Location
	} else if j.d.ArtifactStore != nil {
		bucket = j.d.ArtifactStore.Location
	}
	return location{bucket, path.Join(j.d.Name[:min(20, len(j.d.Name))], name[:min(10, len(name))], core.RandString(7, core.AlphaNum))}
}

func (j *job) get(l location) ([]byte, http.Header, error) {
	status, h, body := j.s.hub.Do(j.account, j.region, "s3", http.MethodGet, (&url.URL{Path: "/" + l.Bucket + "/" + l.Key}).EscapedPath(), nil, nil)
	if status != http.StatusOK {
		return nil, nil, fmt.Errorf("cannot get s3://%s/%s (HTTP %d)", l.Bucket, l.Key, status)
	}
	return body, h, nil
}

func (j *job) put(l location, body []byte) error {
	status, _, _ := j.s.hub.Do(j.account, j.region, "s3", http.MethodPut, (&url.URL{Path: "/" + l.Bucket + "/" + l.Key}).EscapedPath(), nil, body)
	if status != http.StatusOK {
		return fmt.Errorf("cannot put s3://%s/%s (HTTP %d)", l.Bucket, l.Key, status)
	}
	return nil
}

// emit stores body as each of the action's output artifacts.
func (j *job) emit(body []byte) error {
	for _, o := range j.a.OutputArtifacts {
		l := j.out(o.Name)
		if err := j.put(l, body); err != nil {
			return err
		}
		j.output(o.Name, l)
	}
	return nil
}

func (j *job) output(name string, l location) {
	j.set(func() { j.r.Outputs = append(j.r.Outputs, artifact{name, l}) })
}

func (j *job) vars(v map[string]string) { j.set(func() { j.r.Variables = v }) }

func (j *job) external(id, summary string) {
	j.set(func() { j.r.External, j.r.Summary = id, summary })
}

// file reads a file from an input artifact's zip; ref is "Artifact::path".
func (j *job) file(ref string) ([]byte, error) {
	name, file, ok := strings.Cut(ref, "::")
	for _, in := range j.inputs {
		if ok && in.Name == name {
			return j.fileIn(in, file)
		}
	}
	return nil, fmt.Errorf("%q does not name a file of an input artifact (Artifact::path)", ref)
}

func (j *job) fileIn(in artifact, file string) ([]byte, error) {
	body, _, err := j.get(in.S3location)
	if err != nil {
		return nil, err
	}
	z, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return nil, fmt.Errorf("the artifact %s is not a zip file", in.Name)
	}
	rc, err := z.Open(file)
	if err != nil {
		return nil, fmt.Errorf("the artifact %s has no file %s", in.Name, file)
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

func zipOf(name string, body []byte) []byte {
	var b bytes.Buffer
	zw := zip.NewWriter(&b)
	w, _ := zw.Create(name)
	w.Write(body)
	zw.Close()
	return b.Bytes()
}

// ---- sources ----

func (j *job) s3Source() error {
	src := location{j.config["S3Bucket"], j.config["S3ObjectKey"]}
	body, h, err := j.get(src)
	if err != nil {
		return err
	}
	etag, version := strings.Trim(h.Get("ETag"), `"`), h.Get("X-Amz-Version-Id")
	j.vars(map[string]string{"BucketName": src.Bucket, "ObjectKey": src.Key, "ETag": etag, "VersionId": version})
	j.revision(cmp.Or(version, etag), "s3://"+src.Bucket+"/"+src.Key)
	return j.emit(body)
}

// ecrSource outputs the image's imageDetail.json.
func (j *job) ecrSource() error {
	repo, tag := j.config["RepositoryName"], cmp.Or(j.config["ImageTag"], "latest")
	var imgs struct {
		ImageDetails []struct {
			ImageDigest, RegistryId string
			ImageSizeInBytes        int64
		}
	}
	var repos struct {
		Repositories []struct{ RepositoryUri string }
	}
	err := j.call("AmazonEC2ContainerRegistry_V20150921", "DescribeImages", map[string]any{"repositoryName": repo,
		"imageIds": []map[string]string{{"imageTag": tag}}}, &imgs)
	if err == nil {
		err = j.call("AmazonEC2ContainerRegistry_V20150921", "DescribeRepositories", map[string]any{"repositoryNames": []string{repo}}, &repos)
	}
	if err != nil {
		return err
	}
	img, uri := imgs.ImageDetails[0], repos.Repositories[0].RepositoryUri+":"+tag
	detail, _ := json.Marshal(map[string]any{"ImageSizeInBytes": fmt.Sprint(img.ImageSizeInBytes), "ImageDigest": img.ImageDigest, "Version": "1.0",
		"RegistryId": img.RegistryId, "RepositoryName": repo, "ImageURI": uri, "ImageTags": []string{tag}})
	j.vars(map[string]string{"RegistryId": img.RegistryId, "RepositoryName": repo, "ImageTag": tag, "ImageDigest": img.ImageDigest, "ImageURI": uri})
	j.revision(img.ImageDigest, uri)
	return j.emit(zipOf("imageDetail.json", detail))
}

func (j *job) revision(id, summary string) {
	j.set(func() {
		for _, o := range j.a.OutputArtifacts {
			j.e.Revisions = append(j.e.Revisions, revision{o.Name, j.a.Name, id, summary, core.Time(time.Now())})
		}
	})
}

// ---- builds ----

// codeBuild runs the project on the primary input artifact, with its
// artifacts zipped into the artifact store as the output artifact.
func (j *job) codeBuild() error {
	in := map[string]any{"projectName": j.config["ProjectName"], "artifactsOverride": map[string]string{"type": "NO_ARTIFACTS"}}
	if len(j.inputs) > 0 {
		src := j.inputs[0]
		for _, x := range j.inputs {
			if x.Name == j.config["PrimarySource"] {
				src = x
			}
		}
		in["sourceTypeOverride"], in["sourceLocationOverride"] = "S3", src.S3location.Bucket+"/"+src.S3location.Key
	}
	var l location
	if len(j.a.OutputArtifacts) > 0 {
		l = j.out(j.a.OutputArtifacts[0].Name)
		in["artifactsOverride"] = map[string]string{"type": "S3", "location": l.Bucket, "path": path.Dir(l.Key), "name": path.Base(l.Key),
			"packaging": "ZIP", "namespaceType": "NONE"}
	}
	if ev := j.config["EnvironmentVariables"]; ev != "" {
		var vars []map[string]string
		if err := json.Unmarshal([]byte(ev), &vars); err != nil {
			return fmt.Errorf("EnvironmentVariables is not a JSON list: %v", err)
		}
		in["environmentVariablesOverride"] = vars
	}
	var started struct{ Build struct{ Id string } }
	if err := j.call("CodeBuild_20161006", "StartBuild", in, &started); err != nil {
		return err
	}
	id := started.Build.Id
	j.external(id, "")
	err := j.wait(func() (bool, error) {
		var out struct {
			Builds []struct {
				BuildStatus   string
				BuildComplete bool
			}
		}
		if err := j.call("CodeBuild_20161006", "BatchGetBuilds", map[string]any{"ids": []string{id}}, &out); err != nil || len(out.Builds) == 0 {
			return true, cmp.Or(err, fmt.Errorf("build %s is gone", id))
		}
		if b := out.Builds[0]; b.BuildComplete && b.BuildStatus != "SUCCEEDED" {
			return true, fmt.Errorf("build %s %s", id, b.BuildStatus)
		}
		return out.Builds[0].BuildComplete, nil
	})
	if errors.Is(err, errStopped) {
		j.call("CodeBuild_20161006", "StopBuild", map[string]string{"id": id}, nil)
	}
	if err == nil && len(j.a.OutputArtifacts) > 0 {
		j.output(j.a.OutputArtifacts[0].Name, l)
	}
	return err
}

// ---- deploys ----

type stackOut struct {
	StackStatus, StackStatusReason string
	Outputs                        []struct{ OutputKey, OutputValue string }
}

// stack describes a stack; a missing one has no status.
func (j *job) stack(name string) (stackOut, error) {
	var out struct{ Stacks []stackOut }
	if err := j.call("CloudFormation", "DescribeStacks", map[string]string{"StackName": name}, &out); err != nil {
		if strings.Contains(core.AsError(err).Msg, "does not exist") {
			return stackOut{}, nil
		}
		return stackOut{}, err
	}
	if len(out.Stacks) == 0 {
		return stackOut{}, nil
	}
	return out.Stacks[0], nil
}

// settle waits until a stack is not in progress.
func (j *job) settle(name string) (stackOut, error) {
	var st stackOut
	err := j.wait(func() (bool, error) {
		var err error
		st, err = j.stack(name)
		return !strings.HasSuffix(st.StackStatus, "_IN_PROGRESS"), err
	})
	return st, err
}

// cloudFormation creates, updates or deletes a stack from a template in an
// input artifact. ponytail: change set modes are not supported.
func (j *job) cloudFormation() error {
	name, mode := j.config["StackName"], j.config["ActionMode"]
	if mode != "CREATE_UPDATE" && mode != "REPLACE_ON_FAILURE" && mode != "DELETE_ONLY" {
		return fmt.Errorf("pogi does not run the CloudFormation action mode %s", mode)
	}
	st, err := j.stack(name)
	if err != nil {
		return err
	}
	failed := strings.HasSuffix(st.StackStatus, "_FAILED") || st.StackStatus == "ROLLBACK_COMPLETE"
	if mode == "DELETE_ONLY" || mode == "REPLACE_ON_FAILURE" && failed {
		if st.StackStatus != "" {
			if err := j.call("CloudFormation", "DeleteStack", map[string]string{"StackName": name, "RoleARN": j.config["RoleArn"]}, nil); err != nil {
				return err
			}
			if st, err = j.settle(name); err != nil || st.StackStatus == "DELETE_FAILED" {
				return cmp.Or(err, fmt.Errorf("stack %s DELETE_FAILED: %s", name, st.StackStatus))
			}
		}
		if mode == "DELETE_ONLY" {
			j.external(name, "Stack "+name+" was deleted")
			return nil
		}
		st = stackOut{}
	}
	body, err := j.file(j.config["TemplatePath"])
	if err != nil {
		return err
	}
	params := map[string]string{}
	if p := j.config["ParameterOverrides"]; p != "" {
		if err := json.Unmarshal([]byte(p), &params); err != nil {
			return fmt.Errorf("ParameterOverrides is not a JSON object of strings: %v", err)
		}
	}
	in := map[string]any{"StackName": name, "TemplateBody": string(body), "RoleARN": j.config["RoleArn"], "Parameters": []map[string]string{}}
	for _, k := range core.SortedKeys(params) {
		in["Parameters"] = append(in["Parameters"].([]map[string]string), map[string]string{"ParameterKey": k, "ParameterValue": params[k]})
	}
	if c := j.config["Capabilities"]; c != "" {
		in["Capabilities"] = strings.Split(c, ",")
	}
	op := map[bool]string{true: "CreateStack", false: "UpdateStack"}[st.StackStatus == "" || st.StackStatus == "DELETE_COMPLETE"]
	if err := j.call("CloudFormation", op, in, nil); err != nil && !strings.Contains(err.Error(), "No updates are to be performed") {
		return err
	}
	j.external(name, "")
	if st, err = j.settle(name); err != nil {
		return err
	}
	if st.StackStatus != "CREATE_COMPLETE" && st.StackStatus != "UPDATE_COMPLETE" {
		return fmt.Errorf("stack %s %s: %s", name, st.StackStatus, st.StackStatusReason)
	}
	outputs := map[string]string{}
	for _, o := range st.Outputs {
		outputs[o.OutputKey] = o.OutputValue
	}
	j.vars(outputs)
	if f := j.config["OutputFileName"]; f != "" {
		b, _ := json.Marshal(outputs)
		return j.emit(zipOf(f, b))
	}
	return nil
}

// s3Deploy copies the input artifact into a bucket, extracted or as is.
func (j *job) s3Deploy() error {
	body, _, err := j.get(j.inputs[0].S3location)
	if err != nil {
		return err
	}
	bucket := j.config["BucketName"]
	if j.config["Extract"] != "true" {
		return j.put(location{bucket, j.config["ObjectKey"]}, body)
	}
	z, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return fmt.Errorf("the artifact %s is not a zip file", j.inputs[0].Name)
	}
	for _, f := range z.File {
		if f.FileInfo().IsDir() {
			continue
		}
		rc, err := f.Open()
		if err != nil {
			return err
		}
		b, err := io.ReadAll(rc)
		rc.Close()
		if err == nil {
			err = j.put(location{bucket, path.Join(j.config["ObjectKey"], f.Name)}, b)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// codeDeploy deploys the input artifact as an S3 revision and waits.
func (j *job) codeDeploy() error {
	l := j.inputs[0].S3location
	var out struct{ DeploymentId string }
	err := j.call("CodeDeploy_20141006", "CreateDeployment", map[string]any{"applicationName": j.config["ApplicationName"],
		"deploymentGroupName": j.config["DeploymentGroupName"], "revision": map[string]any{"revisionType": "S3",
			"s3Location": map[string]string{"bucket": l.Bucket, "key": l.Key, "bundleType": "zip"}}}, &out)
	if err != nil {
		return err
	}
	j.external(out.DeploymentId, "")
	return j.wait(func() (bool, error) {
		var d struct{ DeploymentInfo struct{ Status string } }
		if err := j.call("CodeDeploy_20141006", "GetDeployment", map[string]string{"deploymentId": out.DeploymentId}, &d); err != nil {
			return true, err
		}
		switch d.DeploymentInfo.Status {
		case "Succeeded":
			return true, nil
		case "Failed", "Stopped":
			return true, fmt.Errorf("deployment %s %s", out.DeploymentId, d.DeploymentInfo.Status)
		}
		return false, nil
	})
}

// ecsDeploy points a service at a new task definition revision with the
// images of imagedefinitions.json. ponytail: it does not wait for the
// service to become stable.
func (j *job) ecsDeploy() error {
	const ecs = "AmazonEC2ContainerServiceV20141113"
	b, err := j.fileIn(j.inputs[0], cmp.Or(j.config["FileName"], "imagedefinitions.json"))
	if err != nil {
		return err
	}
	var images []struct{ Name, ImageUri string }
	if err := json.Unmarshal(b, &images); err != nil {
		return fmt.Errorf("the image definitions file is not valid JSON: %v", err)
	}
	cluster, service := j.config["ClusterName"], j.config["ServiceName"]
	var svc struct {
		Services []struct{ TaskDefinition string }
	}
	if err := j.call(ecs, "DescribeServices", map[string]any{"cluster": cluster, "services": []string{service}}, &svc); err != nil {
		return err
	}
	if len(svc.Services) == 0 {
		return fmt.Errorf("service %s not found in cluster %s", service, cluster)
	}
	var td struct{ TaskDefinition map[string]any }
	if err := j.call(ecs, "DescribeTaskDefinition", map[string]any{"taskDefinition": svc.Services[0].TaskDefinition}, &td); err != nil {
		return err
	}
	def := td.TaskDefinition
	containers, _ := def["containerDefinitions"].([]any)
	for _, ct := range containers {
		if ct, ok := ct.(map[string]any); ok {
			for _, img := range images {
				if ct["name"] == img.Name {
					ct["image"] = img.ImageUri
				}
			}
		}
	}
	for _, k := range []string{"taskDefinitionArn", "revision", "status", "requiresAttributes", "compatibilities", "registeredAt", "registeredBy", "deregisteredAt"} {
		delete(def, k)
	}
	var reg struct {
		TaskDefinition struct{ TaskDefinitionArn string }
	}
	if err := j.call(ecs, "RegisterTaskDefinition", def, &reg); err != nil {
		return err
	}
	arn := reg.TaskDefinition.TaskDefinitionArn
	j.external(arn, "Service "+service+" now runs "+arn)
	return j.call(ecs, "UpdateService", map[string]string{"cluster": cluster, "service": service, "taskDefinition": arn}, nil)
}

// ---- approvals and invokes ----

// approval waits for PutApprovalResult with the action's token.
func (j *job) approval() error {
	token := core.UUID()
	j.set(func() { j.r.Token = token })
	if topic := j.config["NotificationArn"]; topic != "" {
		msg, _ := json.Marshal(map[string]any{"approval": map[string]string{"pipelineName": j.d.Name, "stageName": j.r.Stage, "actionName": j.a.Name,
			"token": token, "customData": j.config["CustomData"], "externalEntityLink": j.config["ExternalEntityLink"]}})
		j.s.hub.Deliver(topic, &core.Message{Subject: "APPROVAL NEEDED: " + j.d.Name, Body: string(msg)})
	}
	return j.result("Approved", "Rejected")
}

// result waits for the outcome PutApprovalResult or PutJob*Result records.
func (j *job) result(ok, failed string) error {
	return j.wait(func() (bool, error) {
		j.s.mu.Lock()
		defer j.s.mu.Unlock()
		switch j.r.Result {
		case ok:
			j.r.Summary = j.r.ResultSummary
			return true, nil
		case failed:
			j.r.Summary = j.r.ResultSummary
			return true, errors.New(cmp.Or(j.r.ResultSummary, failed))
		}
		return false, nil
	})
}

// lambda invokes a function with a CodePipeline job event and waits for
// it to call PutJobSuccessResult or PutJobFailureResult.
func (j *job) lambda() error {
	id, fn := core.UUID(), j.config["FunctionName"]
	arn := fn
	if !strings.HasPrefix(fn, "arn:") {
		arn = core.ARN("lambda", j.region, j.account, "function:"+fn)
	}
	loc := func(a artifact) map[string]any {
		return map[string]any{"name": a.Name, "location": map[string]any{"type": "S3", "s3Location": map[string]string{
			"bucketName": a.S3location.Bucket, "objectKey": a.S3location.Key}}}
	}
	ins, outs := []map[string]any{}, []map[string]any{}
	var produced []artifact
	for _, in := range j.inputs {
		ins = append(ins, loc(in))
	}
	for _, o := range j.a.OutputArtifacts {
		a := artifact{o.Name, j.out(o.Name)}
		produced, outs = append(produced, a), append(outs, loc(a))
	}
	payload, _ := json.Marshal(map[string]any{"CodePipeline.job": map[string]any{"id": id, "accountId": j.account, "data": map[string]any{
		"actionConfiguration": map[string]any{"configuration": map[string]string{"FunctionName": fn, "UserParameters": j.config["UserParameters"]}},
		"inputArtifacts":      ins, "outputArtifacts": outs,
		"artifactCredentials": map[string]string{"accessKeyId": j.account, "secretAccessKey": "test", "sessionToken": ""}}}})
	j.set(func() { j.r.Job, j.r.External = id, id })
	core.Go("codepipeline.lambda", func() {
		if _, err := j.s.hub.Invoke(arn, payload); err != nil {
			j.set(func() {
				if j.r.Result == "" {
					j.r.Result, j.r.ResultSummary = "Failed", err.Error()
				}
			})
		}
	})
	if err := j.result("Succeeded", "Failed"); err != nil {
		return err
	}
	for _, a := range produced {
		j.output(a.Name, a.S3location)
	}
	return nil
}

// stepFunctions starts a state machine execution and waits for it.
func (j *job) stepFunctions() error {
	input := j.config["Input"]
	if j.config["InputType"] == "FilePath" {
		if len(j.inputs) == 0 {
			return errors.New("InputType FilePath needs an input artifact")
		}
		b, err := j.fileIn(j.inputs[0], input)
		if err != nil {
			return err
		}
		input = string(b)
	}
	var out struct{ ExecutionArn string }
	machine := j.config["StateMachineArn"]
	err := j.call("AWSStepFunctions", "StartExecution", map[string]string{"stateMachineArn": machine,
		"name": cmp.Or(j.config["ExecutionNamePrefix"], j.d.Name) + "-" + j.e.ID, "input": cmp.Or(input, "{}")}, &out)
	if err != nil {
		return err
	}
	j.external(out.ExecutionArn, "")
	j.vars(map[string]string{"StateMachineArn": machine, "ExecutionArn": out.ExecutionArn})
	return j.wait(func() (bool, error) {
		var x struct{ Status, Error, Cause string }
		if err := j.call("AWSStepFunctions", "DescribeExecution", map[string]string{"executionArn": out.ExecutionArn}, &x); err != nil {
			return true, err
		}
		switch x.Status {
		case "RUNNING", "PENDING_REDRIVE":
			return false, nil
		case "SUCCEEDED":
			return true, nil
		}
		return true, fmt.Errorf("execution %s %s: %s %s", out.ExecutionArn, x.Status, x.Error, x.Cause)
	})
}
