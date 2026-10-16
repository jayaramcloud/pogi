package appconfig_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/appconfig"
	"github.com/aws/aws-sdk-go-v2/service/appconfig/types"
	"github.com/aws/aws-sdk-go-v2/service/appconfigdata"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/smithy-go"
	appsvc "github.com/nrndev/pogi/internal/aws/appconfig"
	cwsvc "github.com/nrndev/pogi/internal/aws/cloudwatch"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func code(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return ""
}

type clients struct {
	svc  *appsvc.Service
	hub  *core.Hub
	ac   *appconfig.Client
	data *appconfigdata.Client
	cfg  aws.Config
}

// setup serves AppConfig and CloudWatch (monitor alarms) on one hub.
func setup(t *testing.T) clients {
	var svc *appsvc.Service
	hub, url := core.ServeTest(t, func(h *core.Hub) core.Service { svc = appsvc.New(h); return svc },
		func(h *core.Hub) core.Service { return cwsvc.New(h) })
	cfg := awstest.Config(t, url)
	return clients{svc, hub, appconfig.NewFromConfig(cfg), appconfigdata.NewFromConfig(cfg), cfg}
}

// fixture creates the application "shop" with an environment and a hosted profile.
func (k clients) fixture(typ string, validators []types.Validator, monitors []types.Monitor) (app, env, prof string) {
	a := must(k.ac.CreateApplication(ctx, &appconfig.CreateApplicationInput{Name: aws.String("shop")}))
	e := must(k.ac.CreateEnvironment(ctx, &appconfig.CreateEnvironmentInput{ApplicationId: a.Id, Name: aws.String("prod"), Monitors: monitors}))
	p := must(k.ac.CreateConfigurationProfile(ctx, &appconfig.CreateConfigurationProfileInput{ApplicationId: a.Id, Name: aws.String("settings"),
		LocationUri: aws.String("hosted"), Type: aws.String(typ), Validators: validators}))
	return *a.Id, *e.Id, *p.Id
}

func (k clients) version(app, prof, content string) string {
	v := must(k.ac.CreateHostedConfigurationVersion(ctx, &appconfig.CreateHostedConfigurationVersionInput{ApplicationId: &app,
		ConfigurationProfileId: &prof, Content: []byte(content), ContentType: aws.String("application/json")}))
	return strconv.Itoa(int(v.VersionNumber))
}

func (k clients) deploy(app, env, prof, version, strategy string) (*appconfig.StartDeploymentOutput, error) {
	return k.ac.StartDeployment(ctx, &appconfig.StartDeploymentInput{ApplicationId: &app, EnvironmentId: &env, ConfigurationProfileId: &prof,
		ConfigurationVersion: &version, DeploymentStrategyId: &strategy})
}

func (k clients) deployment(app, env string, n int32) *appconfig.GetDeploymentOutput {
	return must(k.ac.GetDeployment(ctx, &appconfig.GetDeploymentInput{ApplicationId: &app, EnvironmentId: &env, DeploymentNumber: &n}))
}

// session starts a configuration session and returns a function that polls it.
func (k clients) session(app, env, prof string) func() *appconfigdata.GetLatestConfigurationOutput {
	tok := must(k.data.StartConfigurationSession(ctx, &appconfigdata.StartConfigurationSessionInput{ApplicationIdentifier: &app,
		EnvironmentIdentifier: &env, ConfigurationProfileIdentifier: &prof, RequiredMinimumPollIntervalInSeconds: aws.Int32(15)})).InitialConfigurationToken
	return func() *appconfigdata.GetLatestConfigurationOutput {
		out := must(k.data.GetLatestConfiguration(ctx, &appconfigdata.GetLatestConfigurationInput{ConfigurationToken: tok}))
		tok = out.NextPollConfigurationToken
		return out
	}
}

func TestResourcesAndErrors(t *testing.T) {
	k := setup(t)
	c := k.ac
	a := must(c.CreateApplication(ctx, &appconfig.CreateApplicationInput{Name: aws.String("shop"), Description: aws.String("web shop"),
		Tags: map[string]string{"team": "a"}}))
	must(c.UpdateApplication(ctx, &appconfig.UpdateApplicationInput{ApplicationId: a.Id, Name: aws.String("store")}))
	if l := must(c.ListApplications(ctx, &appconfig.ListApplicationsInput{})); len(l.Items) != 1 || *l.Items[0].Name != "store" ||
		*l.Items[0].Description != "web shop" || len(*a.Id) != 7 {
		t.Fatalf("applications = %+v", l.Items)
	}
	alarm := "arn:aws:cloudwatch:us-east-1:000000000000:alarm:errors"
	e := must(c.CreateEnvironment(ctx, &appconfig.CreateEnvironmentInput{ApplicationId: a.Id, Name: aws.String("prod"),
		Monitors: []types.Monitor{{AlarmArn: &alarm}}}))
	if e.State != types.EnvironmentStateReadyForDeployment || len(e.Monitors) != 1 {
		t.Fatalf("environment = %+v", e)
	}
	if u := must(c.UpdateEnvironment(ctx, &appconfig.UpdateEnvironmentInput{ApplicationId: a.Id, EnvironmentId: e.Id, Monitors: []types.Monitor{}})); len(u.Monitors) != 0 {
		t.Fatalf("monitors not cleared: %+v", u.Monitors)
	}

	p := must(c.CreateConfigurationProfile(ctx, &appconfig.CreateConfigurationProfileInput{ApplicationId: a.Id, Name: aws.String("settings"), LocationUri: aws.String("hosted")}))
	must(c.CreateConfigurationProfile(ctx, &appconfig.CreateConfigurationProfileInput{ApplicationId: a.Id, Name: aws.String("flags"), LocationUri: aws.String("hosted"),
		Type: aws.String("AWS.AppConfig.FeatureFlags")}))
	if *p.Type != "AWS.Freeform" {
		t.Fatalf("type = %s", *p.Type)
	}
	if l := must(c.ListConfigurationProfiles(ctx, &appconfig.ListConfigurationProfilesInput{ApplicationId: a.Id, Type: aws.String("AWS.AppConfig.FeatureFlags")})); len(l.Items) != 1 || *l.Items[0].Name != "flags" {
		t.Fatalf("profiles = %+v", l.Items)
	}
	_, err := c.CreateConfigurationProfile(ctx, &appconfig.CreateConfigurationProfileInput{ApplicationId: a.Id, Name: aws.String("x"), LocationUri: aws.String("ftp://x")})
	if code(err) != "BadRequestException" {
		t.Fatalf("bad LocationUri: %v", err)
	}

	// hosted versions: metadata in headers, a lock on the latest number
	v1 := must(c.CreateHostedConfigurationVersion(ctx, &appconfig.CreateHostedConfigurationVersionInput{ApplicationId: a.Id, ConfigurationProfileId: p.Id,
		Content: []byte(`{"a":1}`), ContentType: aws.String("application/json"), VersionLabel: aws.String("v1"), Description: aws.String("first")}))
	if v1.VersionNumber != 1 || *v1.VersionLabel != "v1" || *v1.ContentType != "application/json" || string(v1.Content) != `{"a":1}` {
		t.Fatalf("version = %+v", v1)
	}
	_, err = c.CreateHostedConfigurationVersion(ctx, &appconfig.CreateHostedConfigurationVersionInput{ApplicationId: a.Id, ConfigurationProfileId: p.Id,
		Content: []byte(`{}`), ContentType: aws.String("application/json"), LatestVersionNumber: aws.Int32(0)})
	if code(err) != "ConflictException" {
		t.Fatalf("stale LatestVersionNumber: %v", err)
	}
	must(c.CreateHostedConfigurationVersion(ctx, &appconfig.CreateHostedConfigurationVersionInput{ApplicationId: a.Id, ConfigurationProfileId: p.Id,
		Content: []byte(`{"a":2}`), ContentType: aws.String("application/json"), LatestVersionNumber: aws.Int32(1)}))
	if g := must(c.GetHostedConfigurationVersion(ctx, &appconfig.GetHostedConfigurationVersionInput{ApplicationId: a.Id, ConfigurationProfileId: p.Id,
		VersionNumber: aws.Int32(1)})); string(g.Content) != `{"a":1}` || *g.Description != "first" || *g.VersionLabel != "v1" {
		t.Fatalf("get version = %+v", g)
	}
	if l := must(c.ListHostedConfigurationVersions(ctx, &appconfig.ListHostedConfigurationVersionsInput{ApplicationId: a.Id, ConfigurationProfileId: p.Id,
		VersionLabel: aws.String("v*")})); len(l.Items) != 1 || l.Items[0].VersionNumber != 1 {
		t.Fatalf("versions labelled v* = %+v", l.Items)
	}
	must(c.DeleteHostedConfigurationVersion(ctx, &appconfig.DeleteHostedConfigurationVersionInput{ApplicationId: a.Id, ConfigurationProfileId: p.Id, VersionNumber: aws.Int32(2)}))
	_, err = c.GetHostedConfigurationVersion(ctx, &appconfig.GetHostedConfigurationVersionInput{ApplicationId: a.Id, ConfigurationProfileId: p.Id, VersionNumber: aws.Int32(2)})
	if code(err) != "ResourceNotFoundException" {
		t.Fatalf("deleted version: %v", err)
	}

	// deployment strategies: AWS's own plus custom ones
	if l := must(c.ListDeploymentStrategies(ctx, &appconfig.ListDeploymentStrategiesInput{})); len(l.Items) != 4 || *l.Items[0].Id != "AppConfig.AllAtOnce" {
		t.Fatalf("strategies = %+v", l.Items)
	}
	st := must(c.CreateDeploymentStrategy(ctx, &appconfig.CreateDeploymentStrategyInput{Name: aws.String("slow"), DeploymentDurationInMinutes: aws.Int32(10),
		GrowthFactor: aws.Float32(25), FinalBakeTimeInMinutes: 5, ReplicateTo: types.ReplicateToNone}))
	if u := must(c.UpdateDeploymentStrategy(ctx, &appconfig.UpdateDeploymentStrategyInput{DeploymentStrategyId: st.Id, GrowthFactor: aws.Float32(50)})); *u.GrowthFactor != 50 ||
		u.DeploymentDurationInMinutes != 10 || u.GrowthType != types.GrowthTypeLinear {
		t.Fatalf("updated strategy = %+v", u)
	}
	if _, err := c.DeleteDeploymentStrategy(ctx, &appconfig.DeleteDeploymentStrategyInput{DeploymentStrategyId: aws.String("AppConfig.AllAtOnce")}); code(err) != "BadRequestException" {
		t.Fatalf("delete a predefined strategy: %v", err)
	}
	must(c.DeleteDeploymentStrategy(ctx, &appconfig.DeleteDeploymentStrategyInput{DeploymentStrategyId: st.Id}))
	if _, err := c.GetDeploymentStrategy(ctx, &appconfig.GetDeploymentStrategyInput{DeploymentStrategyId: st.Id}); code(err) != "ResourceNotFoundException" {
		t.Fatalf("deleted strategy: %v", err)
	}

	// tags, by an ARN without the account id as Terraform builds them
	arn := "arn:aws:appconfig:us-east-1::application/" + *a.Id
	must(c.TagResource(ctx, &appconfig.TagResourceInput{ResourceArn: &arn, Tags: map[string]string{"env": "dev"}}))
	must(c.UntagResource(ctx, &appconfig.UntagResourceInput{ResourceArn: &arn, TagKeys: []string{"team"}}))
	if tags := must(c.ListTagsForResource(ctx, &appconfig.ListTagsForResourceInput{ResourceArn: &arn})).Tags; !reflect.DeepEqual(tags, map[string]string{"env": "dev"}) {
		t.Fatalf("tags = %v", tags)
	}
	if _, err := c.ListTagsForResource(ctx, &appconfig.ListTagsForResourceInput{ResourceArn: aws.String(arn + "x")}); code(err) != "ResourceNotFoundException" {
		t.Fatalf("tags of nothing: %v", err)
	}

	// extensions and associations are metadata
	x := must(c.CreateExtension(ctx, &appconfig.CreateExtensionInput{Name: aws.String("notify"), Actions: map[string][]types.Action{
		"ON_DEPLOYMENT_COMPLETE": {{Name: aws.String("tell"), Uri: aws.String("arn:aws:sqs:us-east-1:000000000000:q"), RoleArn: aws.String("arn:aws:iam::000000000000:role/r")}}}}))
	if again := must(c.CreateExtension(ctx, &appconfig.CreateExtensionInput{Name: aws.String("notify"), LatestVersionNumber: aws.Int32(1),
		Actions: map[string][]types.Action{"ON_DEPLOYMENT_START": {{Name: aws.String("tell"), Uri: aws.String("arn:aws:sqs:us-east-1:000000000000:q")}}}})); again.VersionNumber != 2 ||
		*again.Id != *x.Id || !strings.HasSuffix(*again.Arn, "/2") {
		t.Fatalf("new extension version = %+v", again)
	}
	as := must(c.CreateExtensionAssociation(ctx, &appconfig.CreateExtensionAssociationInput{ExtensionIdentifier: aws.String("notify"), ResourceIdentifier: &arn,
		Parameters: map[string]string{"k": "v"}}))
	if l := must(c.ListExtensionAssociations(ctx, &appconfig.ListExtensionAssociationsInput{ResourceIdentifier: &arn})); len(l.Items) != 1 || *l.Items[0].Id != *as.Id {
		t.Fatalf("associations = %+v", l.Items)
	}
	must(c.DeleteExtensionAssociation(ctx, &appconfig.DeleteExtensionAssociationInput{ExtensionAssociationId: as.Id}))
	must(c.DeleteExtension(ctx, &appconfig.DeleteExtensionInput{ExtensionIdentifier: x.Id}))

	// state survives a snapshot
	b := must(k.hub.Snapshot())
	k.hub.Reset()
	if err := k.hub.Restore(b); err != nil {
		t.Fatal(err)
	}
	if g := must(c.GetEnvironment(ctx, &appconfig.GetEnvironmentInput{ApplicationId: a.Id, EnvironmentId: e.Id})); *g.Name != "prod" {
		t.Fatalf("restored environment = %+v", g)
	}
	must(c.DeleteApplication(ctx, &appconfig.DeleteApplicationInput{ApplicationId: a.Id}))
	if _, err := c.GetApplication(ctx, &appconfig.GetApplicationInput{ApplicationId: a.Id}); code(err) != "ResourceNotFoundException" {
		t.Fatalf("deleted application: %v", err)
	}
}

func TestDeploymentsAndSessions(t *testing.T) {
	k := setup(t)
	k.svc.Close() // the test drives the clock
	app, env, prof := k.fixture("", nil, nil)
	v1 := k.version(app, prof, `{"color":"red"}`)
	d := must(k.deploy(app, env, prof, v1, "AppConfig.AllAtOnce"))
	if d.State != types.DeploymentStateDeploying || d.DeploymentNumber != 1 || *d.GrowthFactor != 100 || d.FinalBakeTimeInMinutes != 10 || *d.ConfigurationName != "settings" {
		t.Fatalf("deployment = %+v", d)
	}
	if _, err := k.deploy(app, env, prof, v1, "AppConfig.AllAtOnce"); code(err) != "ConflictException" {
		t.Fatalf("a second deployment while one is in progress: %v", err)
	}

	poll := k.session("shop", env, prof) // names work as well as ids
	if out := poll(); string(out.Configuration) != `{"color":"red"}` || *out.ContentType != "application/json" || out.NextPollIntervalInSeconds != 15 {
		t.Fatalf("first poll = %q %+v", out.Configuration, out)
	}
	tok := must(k.data.StartConfigurationSession(ctx, &appconfigdata.StartConfigurationSessionInput{ApplicationIdentifier: &app,
		EnvironmentIdentifier: &env, ConfigurationProfileIdentifier: &prof})).InitialConfigurationToken
	must(k.data.GetLatestConfiguration(ctx, &appconfigdata.GetLatestConfigurationInput{ConfigurationToken: tok}))
	if _, err := k.data.GetLatestConfiguration(ctx, &appconfigdata.GetLatestConfigurationInput{ConfigurationToken: tok}); code(err) != "BadRequestException" {
		t.Fatalf("a used token: %v", err)
	}
	if out := poll(); len(out.Configuration) != 0 {
		t.Fatalf("unchanged config should come back empty, got %q", out.Configuration)
	}

	k.svc.Tick(time.Now().Add(time.Hour))
	got := k.deployment(app, env, 1)
	if got.State != types.DeploymentStateComplete || *got.PercentageComplete != 100 || got.CompletedAt == nil ||
		got.EventLog[0].EventType != types.DeploymentEventTypeDeploymentCompleted || got.EventLog[len(got.EventLog)-1].TriggeredBy != types.TriggeredByUser {
		t.Fatalf("after the bake = %+v", got)
	}
	if e := must(k.ac.GetEnvironment(ctx, &appconfig.GetEnvironmentInput{ApplicationId: &app, EnvironmentId: &env})); e.State != types.EnvironmentStateReadyForDeployment {
		t.Fatalf("environment = %s", e.State)
	}

	v2 := k.version(app, prof, `{"color":"blue"}`)
	must(k.deploy(app, env, prof, v2, "AppConfig.Linear50PercentEvery30Seconds"))
	if out := poll(); string(out.Configuration) != `{"color":"blue"}` {
		t.Fatalf("after a new deployment = %q", out.Configuration)
	}
	if l := must(k.ac.ListDeployments(ctx, &appconfig.ListDeploymentsInput{ApplicationId: &app, EnvironmentId: &env})); len(l.Items) != 2 || l.Items[0].DeploymentNumber != 2 {
		t.Fatalf("deployments = %+v", l.Items)
	}
	gc := must(k.ac.GetConfiguration(ctx, &appconfig.GetConfigurationInput{Application: aws.String("shop"), Environment: aws.String("prod"),
		Configuration: aws.String("settings"), ClientId: aws.String("c1")}))
	if string(gc.Content) != `{"color":"blue"}` || *gc.ConfigurationVersion != v2 {
		t.Fatalf("GetConfiguration = %q %v", gc.Content, *gc.ConfigurationVersion)
	}

	// stopping a deployment in progress rolls clients back
	stopped := must(k.ac.StopDeployment(ctx, &appconfig.StopDeploymentInput{ApplicationId: &app, EnvironmentId: &env, DeploymentNumber: aws.Int32(2)}))
	if stopped.State != types.DeploymentStateRolledBack {
		t.Fatalf("stopped = %s", stopped.State)
	}
	if out := poll(); string(out.Configuration) != `{"color":"red"}` {
		t.Fatalf("after the rollback = %q", out.Configuration)
	}
}

func TestFeatureFlags(t *testing.T) {
	k := setup(t)
	app, env, prof := k.fixture("AWS.AppConfig.FeatureFlags", nil, nil)
	_, err := k.ac.CreateHostedConfigurationVersion(ctx, &appconfig.CreateHostedConfigurationVersionInput{ApplicationId: &app, ConfigurationProfileId: &prof,
		Content: []byte(`{"values": {}}`), ContentType: aws.String("application/json")})
	if code(err) != "BadRequestException" {
		t.Fatalf("flags without flags: %v", err)
	}
	v := k.version(app, prof, `{
		"version": "1",
		"flags": {
			"ui_refresh": {"name": "UI refresh", "attributes": {"dark_mode": {"constraints": {"type": "boolean"}}}},
			"beta": {"name": "Beta"}
		},
		"values": {"ui_refresh": {"enabled": true, "dark_mode": true, "_createdAt": "2026-01-01T00:00:00Z"}}
	}`)
	must(k.deploy(app, env, prof, v, "AppConfig.AllAtOnce"))
	out := k.session(app, env, prof)()
	var flags map[string]map[string]any
	if err := json.Unmarshal(out.Configuration, &flags); err != nil || *out.ContentType != "application/json" {
		t.Fatalf("flags %q: %v", out.Configuration, err)
	}
	want := map[string]map[string]any{"ui_refresh": {"enabled": true, "dark_mode": true}, "beta": {"enabled": false}}
	if !reflect.DeepEqual(flags, want) {
		t.Fatalf("flag values = %v, want %v", flags, want)
	}
	// in real time a deployment completes within seconds
	for deadline := time.Now().Add(10 * time.Second); k.deployment(app, env, 1).State != types.DeploymentStateComplete; time.Sleep(100 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("still %s", k.deployment(app, env, 1).State)
		}
	}
}

func TestValidators(t *testing.T) {
	k := setup(t)
	k.svc.Close()
	k.hub.OnInvoke(func(arn string, payload []byte) ([]byte, error) { // the LAMBDA validator
		var ev struct {
			Content []byte `json:"content"`
			Uri     string `json:"uri"`
		}
		json.Unmarshal(payload, &ev)
		if ev.Uri != "hosted" || strings.Contains(string(ev.Content), "forbidden") {
			return nil, &core.FunctionError{Payload: []byte(`{"errorMessage":"forbidden host"}`)}
		}
		return []byte("null"), nil
	})
	schema := `{"type": "object", "required": ["port"], "properties": {
		"port": {"type": "integer", "minimum": 1, "maximum": 65535},
		"mode": {"enum": ["a", "b"]},
		"hosts": {"type": "array", "items": {"type": "string"}, "maxItems": 2}}}`
	app, env, prof := k.fixture("", []types.Validator{{Type: types.ValidatorTypeJsonSchema, Content: aws.String(schema)},
		{Type: types.ValidatorTypeLambda, Content: aws.String("arn:aws:lambda:us-east-1:000000000000:function:check")}}, nil)
	bad := k.version(app, prof, `{"port": 70000, "mode": "c", "hosts": ["x", 1, "z"]}`)
	_, err := k.deploy(app, env, prof, bad, "AppConfig.AllAtOnce")
	if code(err) != "BadRequestException" {
		t.Fatalf("an invalid version deployed: %v", err)
	}
	for _, want := range []string{"$.port is over its maximum", "$.mode is not one of", "$.hosts[1] is not of type string", "$.hosts is over its maxItems"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("%v\nshould say %q", err, want)
		}
	}
	missing := k.version(app, prof, `{"mode": "a"}`)
	if _, err := k.ac.ValidateConfiguration(ctx, &appconfig.ValidateConfigurationInput{ApplicationId: &app, ConfigurationProfileId: &prof,
		ConfigurationVersion: &missing}); code(err) != "BadRequestException" || !strings.Contains(err.Error(), "$.port is required") {
		t.Fatalf("missing port: %v", err)
	}
	forbidden := k.version(app, prof, `{"port": 80, "hosts": ["forbidden"]}`)
	if _, err := k.deploy(app, env, prof, forbidden, "AppConfig.AllAtOnce"); code(err) != "BadRequestException" || !strings.Contains(err.Error(), "forbidden host") {
		t.Fatalf("the lambda validator: %v", err)
	}
	good := k.version(app, prof, `{"port": 8080, "mode": "a", "hosts": ["x"]}`)
	must(k.ac.ValidateConfiguration(ctx, &appconfig.ValidateConfigurationInput{ApplicationId: &app, ConfigurationProfileId: &prof, ConfigurationVersion: &good}))
	must(k.deploy(app, env, prof, good, "AppConfig.AllAtOnce"))

	_, err = k.ac.CreateConfigurationProfile(ctx, &appconfig.CreateConfigurationProfileInput{ApplicationId: &app, Name: aws.String("x"), LocationUri: aws.String("hosted"),
		Validators: []types.Validator{{Type: types.ValidatorTypeJsonSchema, Content: aws.String("{not json")}}})
	if code(err) != "BadRequestException" {
		t.Fatalf("a schema that is not JSON: %v", err)
	}
}

func TestRollbackOnAlarm(t *testing.T) {
	k := setup(t)
	k.svc.Close()
	cw := cloudwatch.NewFromConfig(k.cfg)
	must(cw.PutMetricAlarm(ctx, &cloudwatch.PutMetricAlarmInput{AlarmName: aws.String("errors"), Namespace: aws.String("Shop"), MetricName: aws.String("Errors"),
		Statistic: cwtypes.StatisticSum, Period: aws.Int32(60), EvaluationPeriods: aws.Int32(1), Threshold: aws.Float64(1),
		ComparisonOperator: cwtypes.ComparisonOperatorGreaterThanOrEqualToThreshold}))
	alarm := "arn:aws:cloudwatch:us-east-1:000000000000:alarm:errors"
	app, env, prof := k.fixture("", nil, []types.Monitor{{AlarmArn: &alarm}})
	must(k.deploy(app, env, prof, k.version(app, prof, `"one"`), "AppConfig.AllAtOnce"))
	k.svc.Tick(time.Now().Add(time.Hour))
	if s := k.deployment(app, env, 1).State; s != types.DeploymentStateComplete {
		t.Fatalf("with the alarm OK: %s", s)
	}

	must(cw.SetAlarmState(ctx, &cloudwatch.SetAlarmStateInput{AlarmName: aws.String("errors"), StateValue: cwtypes.StateValueAlarm, StateReason: aws.String("errors!")}))
	must(k.deploy(app, env, prof, k.version(app, prof, `"two"`), "AppConfig.AllAtOnce"))
	k.svc.Tick(time.Now())
	d := k.deployment(app, env, 2)
	if d.State != types.DeploymentStateRolledBack || d.EventLog[1].EventType != types.DeploymentEventTypeRollbackStarted ||
		d.EventLog[1].TriggeredBy != types.TriggeredByCloudwatchAlarm || !strings.Contains(*d.EventLog[1].Description, alarm) {
		t.Fatalf("with the alarm firing = %+v", d)
	}
	if e := must(k.ac.GetEnvironment(ctx, &appconfig.GetEnvironmentInput{ApplicationId: &app, EnvironmentId: &env})); e.State != types.EnvironmentStateRolledBack {
		t.Fatalf("environment = %s", e.State)
	}
	if out := k.session(app, env, prof)(); string(out.Configuration) != `"one"` {
		t.Fatalf("clients get %q after the rollback", out.Configuration)
	}
}

// floci #5673: an Accept of application/ion, quoted or unquoted type, gets Ion; other Accepts keep the stored JSON.
func TestIonAccept(t *testing.T) {
	k := setup(t)
	app, env, prof := k.fixture("AWS.AppConfig.FeatureFlags", nil, nil)
	must(k.deploy(app, env, prof, k.version(app, prof, `{"version":"1","flags":{"f":{"name":"f"}},"values":{"f":{"enabled":true}}}`), "AppConfig.AllAtOnce"))
	poll := func(accept string) (string, string) {
		tok := must(k.data.StartConfigurationSession(ctx, &appconfigdata.StartConfigurationSessionInput{ApplicationIdentifier: &app,
			EnvironmentIdentifier: &env, ConfigurationProfileIdentifier: &prof})).InitialConfigurationToken
		hdr := http.Header{"Accept": {accept}}
		status, h, body := k.hub.Do("000000000000", "us-east-1", "appconfig", "GET", "/configuration?configuration_token="+url.QueryEscape(*tok), hdr, nil)
		if status != 200 {
			t.Fatalf("%s: status %d %s", accept, status, body)
		}
		return h.Get("Content-Type"), string(body)
	}
	for _, accept := range []string{`application/ion; type=AWS.AppConfig.FeatureFlags`, `application/ion; type="AWS.AppConfig.FeatureFlags"`, `text/plain, application/ion`} {
		if ct, body := poll(accept); ct != "application/ion" || !strings.Contains(body, `"enabled":true`) {
			t.Errorf("%s -> %q %s", accept, ct, body)
		}
	}
	for _, accept := range []string{"application/json", "*/*", ""} {
		if ct, _ := poll(accept); ct != "application/json" {
			t.Errorf("%q -> %q", accept, ct)
		}
	}
}
