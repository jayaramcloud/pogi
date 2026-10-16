package codebuild_test

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/codebuild"
	"github.com/aws/aws-sdk-go-v2/service/codebuild/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	codebuildsvc "github.com/nrndev/pogi/internal/aws/codebuild"
	"github.com/nrndev/pogi/internal/aws/logs"
	s3svc "github.com/nrndev/pogi/internal/aws/s3"
	smsvc "github.com/nrndev/pogi/internal/aws/secretsmanager"
	ssmsvc "github.com/nrndev/pogi/internal/aws/ssm"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

type clients struct {
	cb  *codebuild.Client
	cw  *cloudwatchlogs.Client
	s3  *s3.Client
	ssm *ssm.Client
	sm  *secretsmanager.Client
}

// setup serves CodeBuild with the services builds use; without engine,
// Docker is unreachable.
func setup(t *testing.T, engine bool) clients {
	if !engine {
		t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	}
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { return s3svc.New(h) },
		func(h *core.Hub) core.Service { return codebuildsvc.New(h) },
		func(h *core.Hub) core.Service { return logs.New(h) },
		func(h *core.Hub) core.Service { return ssmsvc.New(h) },
		func(h *core.Hub) core.Service { return smsvc.New(h) })
	cfg := awstest.Config(t, url)
	return clients{codebuild.NewFromConfig(cfg), cloudwatchlogs.NewFromConfig(cfg),
		s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true }), ssm.NewFromConfig(cfg), secretsmanager.NewFromConfig(cfg)}
}

func project(name string, src types.ProjectSource, art types.ProjectArtifacts, vars ...types.EnvironmentVariable) *codebuild.CreateProjectInput {
	return &codebuild.CreateProjectInput{Name: aws.String(name), Source: &src, Artifacts: &art,
		ServiceRole: aws.String("arn:aws:iam::000000000000:role/codebuild"),
		Environment: &types.ProjectEnvironment{Type: types.EnvironmentTypeLinuxContainer, ComputeType: types.ComputeTypeBuildGeneral1Small,
			Image: aws.String("aws/codebuild/standard:7.0"), EnvironmentVariables: vars}}
}

// wait polls until a build completes.
func wait(t *testing.T, c *codebuild.Client, id string) types.Build {
	t.Helper()
	for end := time.Now().Add(3 * time.Minute); ; time.Sleep(200 * time.Millisecond) {
		b := must(c.BatchGetBuilds(ctx, &codebuild.BatchGetBuildsInput{Ids: []string{id}})).Builds[0]
		if b.BuildComplete {
			return b
		}
		if time.Now().After(end) {
			t.Fatalf("build %s did not complete: %+v", id, b)
		}
	}
}

// phases renders a build's phases as TYPE:STATUS.
func phases(b types.Build) string {
	var out []string
	for _, p := range b.Phases {
		out = append(out, string(p.PhaseType)+":"+string(p.PhaseStatus))
	}
	return strings.Join(out, " ")
}

func TestWithoutDocker(t *testing.T) {
	c := setup(t, false).cb
	in := project("app", types.ProjectSource{Type: types.SourceTypeNoSource, Buildspec: aws.String("version: 0.2\nphases:\n  build:\n    commands:\n      - echo hi\n")},
		types.ProjectArtifacts{Type: types.ArtifactsTypeNoArtifacts})
	p := must(c.CreateProject(ctx, in)).Project
	if *p.Arn != "arn:aws:codebuild:us-east-1:000000000000:project/app" || *p.TimeoutInMinutes != 60 || p.Badge.BadgeEnabled ||
		p.LogsConfig.CloudWatchLogs.Status != types.LogsConfigStatusTypeEnabled || p.Cache.Type != types.CacheTypeNoCache {
		t.Fatalf("project %+v", p)
	}
	_, err := c.CreateProject(ctx, in)
	if exists := (*types.ResourceAlreadyExistsException)(nil); !errors.As(err, &exists) {
		t.Fatalf("want ResourceAlreadyExistsException, got %v", err)
	}
	must(c.UpdateProject(ctx, &codebuild.UpdateProjectInput{Name: aws.String("app"), Description: aws.String("the app")}))
	got := must(c.BatchGetProjects(ctx, &codebuild.BatchGetProjectsInput{Names: []string{"app", "nope"}}))
	if len(got.Projects) != 1 || *got.Projects[0].Description != "the app" || *got.Projects[0].Source.Buildspec != *in.Source.Buildspec ||
		!slices.Equal(got.ProjectsNotFound, []string{"nope"}) {
		t.Fatalf("batch get %+v", got)
	}
	if names := must(c.ListProjects(ctx, &codebuild.ListProjectsInput{})).Projects; !slices.Equal(names, []string{"app"}) {
		t.Fatalf("projects %v", names)
	}

	started := must(c.StartBuild(ctx, &codebuild.StartBuildInput{ProjectName: aws.String("app")})).Build
	if *started.BuildNumber != 1 || started.BuildStatus != types.StatusTypeInProgress || !strings.HasPrefix(*started.Id, "app:") ||
		*started.Logs.GroupName != "/aws/codebuild/app" {
		t.Fatalf("started %+v", started)
	}
	b := wait(t, c, *started.Id)
	if b.BuildStatus != types.StatusTypeFailed || phases(b) != "SUBMITTED:SUCCEEDED QUEUED:SUCCEEDED PROVISIONING:FAILED FINALIZING:SUCCEEDED COMPLETED:" ||
		*b.Phases[2].Contexts[0].Message != codebuildsvc.NoDocker {
		t.Fatalf("without Docker a build fails in PROVISIONING: %s %+v", phases(b), b.Phases[2])
	}
	retry := must(c.RetryBuild(ctx, &codebuild.RetryBuildInput{Id: b.Id})).Build
	if *retry.BuildNumber != 2 {
		t.Fatalf("retry %+v", retry)
	}
	wait(t, c, *retry.Id)
	if ids := must(c.ListBuildsForProject(ctx, &codebuild.ListBuildsForProjectInput{ProjectName: aws.String("app")})).Ids; !slices.Equal(ids, []string{*retry.Id, *b.Id}) {
		t.Fatalf("builds newest first %v", ids)
	}
	_, err = c.StartBuild(ctx, &codebuild.StartBuildInput{ProjectName: aws.String("nope")})
	if nf := (*types.ResourceNotFoundException)(nil); !errors.As(err, &nf) {
		t.Fatalf("want ResourceNotFoundException, got %v", err)
	}
	del := must(c.BatchDeleteBuilds(ctx, &codebuild.BatchDeleteBuildsInput{Ids: []string{*b.Id}}))
	if len(del.BuildsDeleted) != 1 || len(must(c.ListBuilds(ctx, &codebuild.ListBuildsInput{})).Ids) != 1 {
		t.Fatalf("delete %+v", del)
	}

	hook := must(c.CreateWebhook(ctx, &codebuild.CreateWebhookInput{ProjectName: aws.String("app"), BranchFilter: aws.String("main")})).Webhook
	if hook.PayloadUrl == nil || *hook.BranchFilter != "main" {
		t.Fatalf("webhook %+v", hook)
	}
	must(c.DeleteWebhook(ctx, &codebuild.DeleteWebhookInput{ProjectName: aws.String("app")}))
	rg := must(c.CreateReportGroup(ctx, &codebuild.CreateReportGroupInput{Name: aws.String("tests"), Type: types.ReportTypeTest,
		ExportConfig: &types.ReportExportConfig{ExportConfigType: types.ReportExportConfigTypeNoExport}})).ReportGroup
	if got := must(c.BatchGetReportGroups(ctx, &codebuild.BatchGetReportGroupsInput{ReportGroupArns: []string{*rg.Arn}})).ReportGroups; len(got) != 1 || got[0].Type != types.ReportTypeTest {
		t.Fatalf("report groups %+v", got)
	}
	must(c.DeleteReportGroup(ctx, &codebuild.DeleteReportGroupInput{Arn: rg.Arn}))
	must(c.DeleteProject(ctx, &codebuild.DeleteProjectInput{Name: aws.String("app")}))
	if nf := must(c.BatchGetProjects(ctx, &codebuild.BatchGetProjectsInput{Names: []string{"app"}})).ProjectsNotFound; len(nf) != 1 {
		t.Fatal("project not deleted")
	}
}

// TestBuildsInDocker runs buildspecs in containers when a container engine answers.
func TestBuildsInDocker(t *testing.T) {
	d := docker.New(docker.Options{})
	if testing.Short() || !d.Available() {
		t.Skip("needs a running container engine")
	}
	c := setup(t, true)
	for _, b := range []string{"artifacts", "sources"} {
		must(c.s3.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(b)}))
	}
	must(c.ssm.PutParameter(ctx, &ssm.PutParameterInput{Name: aws.String("/app/greeting"), Value: aws.String("hi from ssm"), Type: "String"}))
	must(c.sm.CreateSecret(ctx, &secretsmanager.CreateSecretInput{Name: aws.String("app/db"), SecretString: aws.String(`{"user":"admin","port":5432}`)}))

	// an inline buildspec without source, artifacts zipped to S3
	spec := `version: 0.2
env:
  variables:
    WHO: buildspec
    ONLY: buildspec-only
  parameter-store:
    GREETING: /app/greeting
  secrets-manager:
    DB_USER: app/db:user
    DB_PORT: app/db:port
phases:
  install:
    commands:
      - echo installing
  build:
    commands:
      - echo "hello $WHO and $ONLY from $CODEBUILD_BUILD_ID"
      - echo "$GREETING, db $DB_USER:$DB_PORT"
      - mkdir -p out/sub && echo artifact-content > out/sub/hello.txt
      - cd out
    finally:
      - echo "still in $(basename $(pwd))"
  post_build:
    commands:
      - echo "succeeding=$CODEBUILD_BUILD_SUCCEEDING"
artifacts:
  files:
    - out/**/*
`
	must(c.cb.CreateProject(ctx, project("ok", types.ProjectSource{Type: types.SourceTypeNoSource, Buildspec: aws.String(spec)},
		types.ProjectArtifacts{Type: types.ArtifactsTypeS3, Location: aws.String("artifacts"), Name: aws.String("out.zip"),
			Packaging: types.ArtifactPackagingZip, NamespaceType: types.ArtifactNamespaceBuildId},
		types.EnvironmentVariable{Name: aws.String("WHO"), Value: aws.String("project"), Type: types.EnvironmentVariableTypePlaintext})))
	// a zipped S3 source with its own buildspec.yml whose build fails
	var src bytes.Buffer
	zw := zip.NewWriter(&src)
	w := must(zw.Create("buildspec.yml"))
	io.WriteString(w, "version: 0.2\nphases:\n  build:\n    commands:\n      - cat data.txt\n      - exit 3\n      - echo never\n  post_build:\n    commands:\n      - echo \"succeeding=$CODEBUILD_BUILD_SUCCEEDING\"\n")
	w = must(zw.Create("data.txt"))
	io.WriteString(w, "from the zip\n")
	zw.Close()
	must(c.s3.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("sources"), Key: aws.String("app/source.zip"), Body: bytes.NewReader(src.Bytes())}))
	must(c.cb.CreateProject(ctx, project("bad", types.ProjectSource{Type: types.SourceTypeS3, Location: aws.String("sources/app/source.zip")},
		types.ProjectArtifacts{Type: types.ArtifactsTypeNoArtifacts})))

	// a git source needs git in the build image, which the stand-in for aws/codebuild/standard lacks
	must(c.cb.CreateProject(ctx, project("git", types.ProjectSource{Type: types.SourceTypeGithub, Location: aws.String("https://github.com/nrndev/pogi.git")},
		types.ProjectArtifacts{Type: types.ArtifactsTypeNoArtifacts})))

	ok := must(c.cb.StartBuild(ctx, &codebuild.StartBuildInput{ProjectName: aws.String("ok")})).Build
	bad := must(c.cb.StartBuild(ctx, &codebuild.StartBuildInput{ProjectName: aws.String("bad")})).Build
	git := must(c.cb.StartBuild(ctx, &codebuild.StartBuildInput{ProjectName: aws.String("git")})).Build
	b := wait(t, c.cb, *ok.Id)
	all := "SUBMITTED:SUCCEEDED QUEUED:SUCCEEDED PROVISIONING:SUCCEEDED DOWNLOAD_SOURCE:SUCCEEDED INSTALL:SUCCEEDED PRE_BUILD:SUCCEEDED " +
		"BUILD:SUCCEEDED POST_BUILD:SUCCEEDED UPLOAD_ARTIFACTS:SUCCEEDED FINALIZING:SUCCEEDED COMPLETED:"
	if b.BuildStatus != types.StatusTypeSucceeded || phases(b) != all {
		t.Fatalf("build %s: %s %+v", b.BuildStatus, phases(b), b.Phases)
	}
	uuid := strings.TrimPrefix(*b.Id, "ok:")
	if *b.Logs.StreamName != uuid || *b.Artifacts.Location != "arn:aws:s3:::artifacts/"+uuid+"/out.zip" {
		t.Fatalf("logs %+v artifacts %+v", b.Logs, b.Artifacts)
	}
	text := logText(t, c.cw, "/aws/codebuild/ok", uuid)
	for _, want := range []string{"hello project and buildspec-only from " + *b.Id, "hi from ssm, db admin:5432", "still in out", "succeeding=1",
		"[Container] Phase complete: BUILD State: SUCCEEDED"} {
		if !strings.Contains(text, want) {
			t.Fatalf("log lacks %q:\n%s", want, text)
		}
	}
	obj := must(c.s3.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("artifacts"), Key: aws.String(uuid + "/out.zip")}))
	body := must(io.ReadAll(obj.Body))
	zr := must(zip.NewReader(bytes.NewReader(body), int64(len(body))))
	if len(zr.File) != 1 || zr.File[0].Name != "out/sub/hello.txt" {
		t.Fatalf("artifact zip has %+v", zr.File)
	}
	if content := must(io.ReadAll(must(zr.File[0].Open()))); string(content) != "artifact-content\n" {
		t.Fatalf("artifact content %q", content)
	}

	b = wait(t, c.cb, *bad.Id)
	if b.BuildStatus != types.StatusTypeFailed || !strings.Contains(phases(b), "BUILD:FAILED POST_BUILD:SUCCEEDED UPLOAD_ARTIFACTS:SUCCEEDED") {
		t.Fatalf("a failing command fails BUILD, then POST_BUILD runs: %s", phases(b))
	}
	for _, p := range b.Phases {
		if p.PhaseType == types.BuildPhaseTypeBuild && *p.Contexts[0].Message != "Error while executing command: exit 3. Reason: exit status 3" {
			t.Fatalf("BUILD context %+v", p.Contexts)
		}
	}
	text = logText(t, c.cw, "/aws/codebuild/bad", strings.TrimPrefix(*b.Id, "bad:"))
	if !strings.Contains(text, "from the zip") || strings.Contains(text, "never") || !strings.Contains(text, "succeeding=0") {
		t.Fatalf("log of the failed build:\n%s", text)
	}
	b = wait(t, c.cb, *git.Id)
	if p := b.Phases[3]; b.BuildStatus != types.StatusTypeFailed || p.PhaseType != types.BuildPhaseTypeDownloadSource || p.PhaseStatus != types.StatusTypeFailed ||
		!strings.Contains(*p.Contexts[0].Message, "the build image has no git to clone https://github.com/nrndev/pogi.git") {
		t.Fatalf("git source without git: %s %+v", phases(b), b.Phases[3])
	}
	list, err := d.List(ctx, docker.LabelService+"=codebuild")
	for _, ct := range list {
		if r := ct.Labels[docker.LabelResource]; r == "ok" || r == "bad" || r == "git" {
			t.Fatalf("container %v left behind", ct.Names)
		}
	}
	if err != nil {
		t.Fatal(err)
	}
}

func logText(t *testing.T, cw *cloudwatchlogs.Client, group, stream string) string {
	t.Helper()
	var lines []string
	for _, e := range must(cw.GetLogEvents(ctx, &cloudwatchlogs.GetLogEventsInput{LogGroupName: aws.String(group), LogStreamName: aws.String(stream)})).Events {
		lines = append(lines, *e.Message)
	}
	return strings.Join(lines, "\n")
}
