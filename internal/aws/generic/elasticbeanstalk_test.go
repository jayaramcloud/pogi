package generic_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/elasticbeanstalk"
	"github.com/aws/aws-sdk-go-v2/service/elasticbeanstalk/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/aws/generic"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestElasticBeanstalk(t *testing.T) {
	s := pogi.Start(t)
	ctx := context.Background()
	cfg := awstest.Config(t, s.URL)
	eb := elasticbeanstalk.NewFromConfig(cfg)
	sc := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
	lifecycle(t, time.Hour) // frozen: the transitional states can be seen

	// application
	a, err := eb.CreateApplication(ctx, &elasticbeanstalk.CreateApplicationInput{ApplicationName: aws.String("shop"), Description: aws.String("the shop"),
		Tags: []types.Tag{{Key: aws.String("team"), Value: aws.String("web")}}})
	if err != nil || aws.ToString(a.Application.ApplicationArn) != "arn:aws:elasticbeanstalk:us-east-1:000000000000:application/shop" {
		t.Fatalf("create app: %v %+v", err, a)
	}
	if _, err := eb.CreateApplication(ctx, &elasticbeanstalk.CreateApplicationInput{ApplicationName: aws.String("shop")}); err == nil {
		t.Fatal("duplicate application")
	}

	// a version needs its source bundle in S3
	sc.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("bundles")})
	src := &types.S3Location{S3Bucket: aws.String("bundles"), S3Key: aws.String("shop-1.zip")}
	if _, err := eb.CreateApplicationVersion(ctx, &elasticbeanstalk.CreateApplicationVersionInput{ApplicationName: aws.String("shop"), VersionLabel: aws.String("v1"), SourceBundle: src}); err == nil || !strings.Contains(err.Error(), "InvalidParameterValue") {
		t.Fatalf("missing bundle: %v", err)
	}
	if _, err := sc.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("bundles"), Key: aws.String("shop-1.zip"), Body: strings.NewReader("zip")}); err != nil {
		t.Fatal(err)
	}
	v, err := eb.CreateApplicationVersion(ctx, &elasticbeanstalk.CreateApplicationVersionInput{ApplicationName: aws.String("shop"), VersionLabel: aws.String("v1"), SourceBundle: src, Description: aws.String("first")})
	if err != nil || aws.ToString(v.ApplicationVersion.VersionLabel) != "v1" || aws.ToString(v.ApplicationVersion.SourceBundle.S3Key) != "shop-1.zip" {
		t.Fatalf("version: %v %+v", err, v)
	}
	vs, err := eb.DescribeApplicationVersions(ctx, &elasticbeanstalk.DescribeApplicationVersionsInput{ApplicationName: aws.String("shop")})
	if err != nil || len(vs.ApplicationVersions) != 1 {
		t.Fatalf("versions: %v %+v", err, vs)
	}

	// configuration template
	ct, err := eb.CreateConfigurationTemplate(ctx, &elasticbeanstalk.CreateConfigurationTemplateInput{ApplicationName: aws.String("shop"), TemplateName: aws.String("small"),
		SolutionStackName: aws.String("64bit Amazon Linux 2023 v4.3.0 running Python 3.11"),
		OptionSettings:    []types.ConfigurationOptionSetting{{Namespace: aws.String("aws:autoscaling:asg"), OptionName: aws.String("MaxSize"), Value: aws.String("2")}}})
	if err != nil || aws.ToString(ct.TemplateName) != "small" {
		t.Fatalf("template: %v %+v", err, ct)
	}
	cs, err := eb.DescribeConfigurationSettings(ctx, &elasticbeanstalk.DescribeConfigurationSettingsInput{ApplicationName: aws.String("shop"), TemplateName: aws.String("small")})
	if err != nil || len(cs.ConfigurationSettings) != 1 || len(cs.ConfigurationSettings[0].OptionSettings) != 1 {
		t.Fatalf("template settings: %v %+v", err, cs)
	}

	// environment: Launching, then Ready and Green
	e, err := eb.CreateEnvironment(ctx, &elasticbeanstalk.CreateEnvironmentInput{ApplicationName: aws.String("shop"), EnvironmentName: aws.String("shop-prod"), VersionLabel: aws.String("v1"),
		SolutionStackName: aws.String("64bit Amazon Linux 2023 v4.3.0 running Python 3.11"), Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
		OptionSettings: []types.ConfigurationOptionSetting{{Namespace: aws.String("aws:elasticbeanstalk:environment"), OptionName: aws.String("EnvironmentType"), Value: aws.String("SingleInstance")}}})
	if err != nil {
		t.Fatal(err)
	}
	if e.Status != types.EnvironmentStatusLaunching || e.Health != types.EnvironmentHealthGrey || aws.ToString(e.CNAME) != "shop-prod.us-east-1.elasticbeanstalk.localhost" || !strings.HasPrefix(aws.ToString(e.EnvironmentId), "e-") {
		t.Fatalf("launching: %+v", e)
	}
	eid := aws.ToString(e.EnvironmentId)
	if _, err := eb.CreateEnvironment(ctx, &elasticbeanstalk.CreateEnvironmentInput{ApplicationName: aws.String("shop"), EnvironmentName: aws.String("shop-prod")}); err == nil {
		t.Fatal("duplicate environment")
	}
	generic.SetLifecycleDelay(0)
	d, err := eb.DescribeEnvironments(ctx, &elasticbeanstalk.DescribeEnvironmentsInput{EnvironmentIds: []string{eid}})
	if err != nil || len(d.Environments) != 1 || d.Environments[0].Status != types.EnvironmentStatusReady || d.Environments[0].Health != types.EnvironmentHealthGreen ||
		d.Environments[0].HealthStatus != types.EnvironmentHealthStatusOk || aws.ToString(d.Environments[0].VersionLabel) != "v1" {
		t.Fatalf("ready: %v %+v", err, d)
	}
	l, err := eb.DescribeEnvironments(ctx, &elasticbeanstalk.DescribeEnvironmentsInput{ApplicationName: aws.String("shop")})
	if err != nil || len(l.Environments) != 1 {
		t.Fatalf("list: %v %+v", err, l)
	}
	if l, _ := eb.DescribeEnvironments(ctx, &elasticbeanstalk.DescribeEnvironmentsInput{EnvironmentNames: []string{"nope"}}); len(l.Environments) != 0 {
		t.Fatalf("unknown name: %+v", l)
	}
	if h, err := eb.DescribeEnvironmentHealth(ctx, &elasticbeanstalk.DescribeEnvironmentHealthInput{EnvironmentName: aws.String("shop-prod"), AttributeNames: []types.EnvironmentHealthAttribute{"All"}}); err != nil || aws.ToString(h.Color) != "Green" {
		t.Fatalf("health: %v %+v", err, h)
	}

	// update: the options are stored, and the environment is Updating
	generic.SetLifecycleDelay(time.Hour)
	u, err := eb.UpdateEnvironment(ctx, &elasticbeanstalk.UpdateEnvironmentInput{EnvironmentId: aws.String(eid), Description: aws.String("scaled"),
		OptionSettings: []types.ConfigurationOptionSetting{{Namespace: aws.String("aws:autoscaling:asg"), OptionName: aws.String("MaxSize"), Value: aws.String("4")}}})
	if err != nil || u.Status != types.EnvironmentStatusUpdating || aws.ToString(u.Description) != "scaled" {
		t.Fatalf("update: %v %+v", err, u)
	}
	generic.SetLifecycleDelay(0)
	cs, err = eb.DescribeConfigurationSettings(ctx, &elasticbeanstalk.DescribeConfigurationSettingsInput{ApplicationName: aws.String("shop"), EnvironmentName: aws.String("shop-prod")})
	if err != nil || len(cs.ConfigurationSettings) != 1 || len(cs.ConfigurationSettings[0].OptionSettings) != 1 || aws.ToString(cs.ConfigurationSettings[0].OptionSettings[0].Value) != "4" {
		t.Fatalf("env settings: %v %+v", err, cs)
	}

	// application lists its versions and templates; tags
	da, err := eb.DescribeApplications(ctx, &elasticbeanstalk.DescribeApplicationsInput{})
	if err != nil || len(da.Applications) != 1 || len(da.Applications[0].Versions) != 1 || len(da.Applications[0].ConfigurationTemplates) != 1 {
		t.Fatalf("apps: %v %+v", err, da)
	}
	arn := aws.ToString(e.EnvironmentArn)
	if _, err := eb.UpdateTagsForResource(ctx, &elasticbeanstalk.UpdateTagsForResourceInput{ResourceArn: aws.String(arn), TagsToAdd: []types.Tag{{Key: aws.String("a"), Value: aws.String("b")}}, TagsToRemove: []string{"env"}}); err != nil {
		t.Fatal(err)
	}
	if tg, err := eb.ListTagsForResource(ctx, &elasticbeanstalk.ListTagsForResourceInput{ResourceArn: aws.String(arn)}); err != nil || len(tg.ResourceTags) != 1 || aws.ToString(tg.ResourceTags[0].Key) != "a" {
		t.Fatalf("tags: %v %+v", err, tg)
	}

	// terminate, then the name can be reused
	if _, err := eb.TerminateEnvironment(ctx, &elasticbeanstalk.TerminateEnvironmentInput{EnvironmentId: aws.String(eid)}); err != nil {
		t.Fatal(err)
	}
	if l, _ := eb.DescribeEnvironments(ctx, &elasticbeanstalk.DescribeEnvironmentsInput{}); len(l.Environments) != 0 {
		t.Fatalf("terminated environment listed: %+v", l)
	}
	if l, _ := eb.DescribeEnvironments(ctx, &elasticbeanstalk.DescribeEnvironmentsInput{IncludeDeleted: aws.Bool(true)}); len(l.Environments) != 1 || l.Environments[0].Status != types.EnvironmentStatusTerminated {
		t.Fatalf("include deleted: %+v", l)
	}
	if _, err := eb.CreateEnvironment(ctx, &elasticbeanstalk.CreateEnvironmentInput{ApplicationName: aws.String("shop"), EnvironmentName: aws.String("shop-prod")}); err != nil {
		t.Fatalf("reuse name: %v", err)
	}

	// delete, and the not-found errors
	if _, err := eb.DeleteConfigurationTemplate(ctx, &elasticbeanstalk.DeleteConfigurationTemplateInput{ApplicationName: aws.String("shop"), TemplateName: aws.String("small")}); err != nil {
		t.Fatal(err)
	}
	if _, err := eb.DeleteApplicationVersion(ctx, &elasticbeanstalk.DeleteApplicationVersionInput{ApplicationName: aws.String("shop"), VersionLabel: aws.String("v1")}); err != nil {
		t.Fatal(err)
	}
	if _, err := eb.DeleteApplication(ctx, &elasticbeanstalk.DeleteApplicationInput{ApplicationName: aws.String("shop")}); err != nil {
		t.Fatal(err)
	}
	if _, err := eb.UpdateApplication(ctx, &elasticbeanstalk.UpdateApplicationInput{ApplicationName: aws.String("shop"), Description: aws.String("x")}); err == nil || !strings.Contains(err.Error(), "InvalidParameterValue") {
		t.Fatalf("update missing: %v", err)
	}
	if da, err := eb.DescribeApplications(ctx, &elasticbeanstalk.DescribeApplicationsInput{ApplicationNames: []string{"shop"}}); err != nil || len(da.Applications) != 0 {
		t.Fatalf("describe missing: %v %+v", err, da)
	}
}

// lifecycle sets how long resources stay in a transitional state for the test.
func lifecycle(t *testing.T, d time.Duration) {
	prev := generic.SetLifecycleDelay(d)
	t.Cleanup(func() { generic.SetLifecycleDelay(prev) })
}
