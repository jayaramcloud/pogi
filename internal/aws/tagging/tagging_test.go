package tagging_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/acm"
	acmtypes "github.com/aws/aws-sdk-go-v2/service/acm/types"
	"github.com/aws/aws-sdk-go-v2/service/apigateway"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/ecr"
	ecrtypes "github.com/aws/aws-sdk-go-v2/service/ecr/types"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	rgt "github.com/aws/aws-sdk-go-v2/service/resourcegroupstaggingapi"
	"github.com/aws/aws-sdk-go-v2/service/resourcegroupstaggingapi/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	acmsvc "github.com/nrndev/pogi/internal/aws/acm"
	apigwsvc "github.com/nrndev/pogi/internal/aws/apigateway"
	ec2svc "github.com/nrndev/pogi/internal/aws/ec2"
	ecrsvc "github.com/nrndev/pogi/internal/aws/ecr"
	eventssvc "github.com/nrndev/pogi/internal/aws/events"
	logssvc "github.com/nrndev/pogi/internal/aws/logs"
	sqssvc "github.com/nrndev/pogi/internal/aws/sqs"
	"github.com/nrndev/pogi/internal/aws/tagging"
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

func TestTaggingAcrossServices(t *testing.T) {
	_, url := core.ServeTest(t,
		func(h *core.Hub) core.Service { return acmsvc.New(h) },
		func(h *core.Hub) core.Service { return ecrsvc.New(h) },
		func(h *core.Hub) core.Service { return ec2svc.New(h) },
		func(h *core.Hub) core.Service { return tagging.New(h) })
	cfg := awstest.Config(t, url)
	c := rgt.NewFromConfig(cfg)

	cert := must(acm.NewFromConfig(cfg).RequestCertificate(ctx, &acm.RequestCertificateInput{DomainName: aws.String("example.com"),
		Tags: []acmtypes.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}})).CertificateArn
	repo := must(ecr.NewFromConfig(cfg).CreateRepository(ctx, &ecr.CreateRepositoryInput{RepositoryName: aws.String("web"),
		Tags: []ecrtypes.Tag{{Key: aws.String("env"), Value: aws.String("prod")}, {Key: aws.String("team"), Value: aws.String("web")}}})).Repository.RepositoryArn
	vpc := must(ec2.NewFromConfig(cfg).CreateVpc(ctx, &ec2.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16"),
		TagSpecifications: []ec2types.TagSpecification{{ResourceType: ec2types.ResourceTypeVpc, Tags: []ec2types.Tag{{Key: aws.String("team"), Value: aws.String("net")}}}}})).Vpc

	arns := func(in *rgt.GetResourcesInput) (out []string) {
		for _, m := range must(c.GetResources(ctx, in)).ResourceTagMappingList {
			out = append(out, *m.ResourceARN)
		}
		return out
	}
	if got := arns(&rgt.GetResourcesInput{TagFilters: []types.TagFilter{{Key: aws.String("env")}}}); len(got) != 2 {
		t.Fatalf("env-tagged = %v", got)
	}
	if got := arns(&rgt.GetResourcesInput{TagFilters: []types.TagFilter{{Key: aws.String("env"), Values: []string{"dev"}}}}); len(got) != 1 || got[0] != *cert {
		t.Fatalf("env=dev = %v", got)
	}
	if got := arns(&rgt.GetResourcesInput{ResourceTypeFilters: []string{"ec2:vpc"}}); len(got) != 1 || got[0] != "arn:aws:ec2:us-east-1:000000000000:vpc/"+*vpc.VpcId {
		t.Fatalf("ec2:vpc = %v", got)
	}

	// Tagging goes through each service's own API; SQS queues have no ARN-based JSON tag operation.
	out := must(c.TagResources(ctx, &rgt.TagResourcesInput{ResourceARNList: []string{*cert, "arn:aws:sqs:us-east-1:000000000000:q"},
		Tags: map[string]string{"owner": "me"}}))
	if len(out.FailedResourcesMap) != 1 || out.FailedResourcesMap["arn:aws:sqs:us-east-1:000000000000:q"].ErrorCode != types.ErrorCodeInvalidParameterException {
		t.Fatalf("failed = %+v", out.FailedResourcesMap)
	}
	tags := must(acm.NewFromConfig(cfg).ListTagsForCertificate(ctx, &acm.ListTagsForCertificateInput{CertificateArn: cert})).Tags
	if len(tags) != 2 || *tags[1].Key != "owner" {
		t.Fatalf("certificate tags = %+v", tags)
	}
	must(c.UntagResources(ctx, &rgt.UntagResourcesInput{ResourceARNList: []string{*repo, *cert}, TagKeys: []string{"team", "owner"}}))
	if keys := must(c.GetTagKeys(ctx, &rgt.GetTagKeysInput{})).TagKeys; len(keys) != 2 || keys[0] != "env" || keys[1] != "team" {
		t.Fatalf("keys = %v", keys) // team survives on the VPC only
	}
	if vals := must(c.GetTagValues(ctx, &rgt.GetTagValuesInput{Key: aws.String("env")})).TagValues; len(vals) != 2 || vals[0] != "dev" {
		t.Fatalf("values = %v", vals)
	}

	_, err := c.TagResources(ctx, &rgt.TagResourcesInput{ResourceARNList: []string{*cert}, Tags: map[string]string{}})
	var bad *types.InvalidParameterException
	if !errors.As(err, &bad) {
		t.Fatalf("want InvalidParameterException, got %v", err)
	}
}

// floci #4560 and #1343: GetResources must find tagged queues, log groups, rules on any bus and API keys.
func TestGetResourcesFindsQueuesLogsRulesAndKeys(t *testing.T) {
	_, url := core.ServeTest(t,
		func(h *core.Hub) core.Service { return sqssvc.New(h) },
		func(h *core.Hub) core.Service { return logssvc.New(h) },
		func(h *core.Hub) core.Service { return eventssvc.New(h) },
		func(h *core.Hub) core.Service { return apigwsvc.New(h) },
		func(h *core.Hub) core.Service { return tagging.New(h) })
	cfg := awstest.Config(t, url)
	tags := map[string]string{"team": "a"}
	must(sqs.NewFromConfig(cfg).CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("q"), Tags: tags}))
	must(cloudwatchlogs.NewFromConfig(cfg).CreateLogGroup(ctx, &cloudwatchlogs.CreateLogGroupInput{LogGroupName: aws.String("lg"), Tags: tags}))
	must(apigateway.NewFromConfig(cfg).CreateApiKey(ctx, &apigateway.CreateApiKeyInput{Name: aws.String("k"), Tags: tags}))
	eb := eventbridge.NewFromConfig(cfg)
	must(eb.CreateEventBus(ctx, &eventbridge.CreateEventBusInput{Name: aws.String("bus")}))
	must(eb.PutRule(ctx, &eventbridge.PutRuleInput{Name: aws.String("r"), EventBusName: aws.String("bus"), EventPattern: aws.String(`{"source":["a"]}`),
		Tags: []ebtypes.Tag{{Key: aws.String("team"), Value: aws.String("a")}}}))

	c := rgt.NewFromConfig(cfg)
	for _, rt := range []string{"sqs:queue", "sqs", "logs:log-group", "events:rule", "apigateway:apikeys"} {
		o := must(c.GetResources(ctx, &rgt.GetResourcesInput{ResourceTypeFilters: []string{rt},
			TagFilters: []types.TagFilter{{Key: aws.String("team"), Values: []string{"a"}}}}))
		if len(o.ResourceTagMappingList) != 1 {
			t.Errorf("GetResources %s found %+v, want 1", rt, o.ResourceTagMappingList)
		}
	}
}
