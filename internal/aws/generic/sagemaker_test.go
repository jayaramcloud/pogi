package generic_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sagemaker"
	"github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/aws/aws-sdk-go-v2/service/sagemakerruntime"
	"github.com/aws/smithy-go"
	"github.com/nrndev/pogi"
)

const smRole = "arn:aws:iam::000000000000:role/sagemaker"

// aiNotFound checks the error SageMaker gives for a missing resource: a ValidationException (RecordNotFound for a notebook instance).
func aiNotFound(t *testing.T, err error) {
	t.Helper()
	var ae smithy.APIError
	if !errors.As(err, &ae) || ae.ErrorCode() != "ValidationException" || !strings.Contains(ae.ErrorMessage(), "Could not find") && ae.ErrorMessage() != "RecordNotFound" {
		t.Fatalf("want a 'Could not find' or RecordNotFound ValidationException: %T %v", err, err)
	}
}

func TestSageMakerModelEndpointConfigEndpoint(t *testing.T) {
	ctx := context.Background()
	c := sagemaker.NewFromConfig(aiStart(t, pogi.Config{}))

	m, err := c.CreateModel(ctx, &sagemaker.CreateModelInput{ModelName: aws.String("m1"), ExecutionRoleArn: aws.String(smRole),
		PrimaryContainer: &types.ContainerDefinition{Image: aws.String("123.dkr.ecr.us-east-1.amazonaws.com/img:1"), ModelDataUrl: aws.String("s3://b/model.tar.gz")},
		Tags:             []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}})
	if err != nil || aws.ToString(m.ModelArn) != "arn:aws:sagemaker:us-east-1:000000000000:model/m1" {
		t.Fatalf("create model: %v %s", err, aws.ToString(m.ModelArn))
	}
	dm, err := c.DescribeModel(ctx, &sagemaker.DescribeModelInput{ModelName: aws.String("m1")})
	if err != nil || aws.ToString(dm.PrimaryContainer.ModelDataUrl) != "s3://b/model.tar.gz" || aws.ToString(dm.ExecutionRoleArn) != smRole {
		t.Fatalf("describe model: %v %+v", err, dm)
	}
	lm, err := c.ListModels(ctx, &sagemaker.ListModelsInput{})
	if err != nil || len(lm.Models) != 1 || aws.ToString(lm.Models[0].ModelName) != "m1" {
		t.Fatalf("list models: %v %+v", err, lm)
	}
	if _, err := c.AddTags(ctx, &sagemaker.AddTagsInput{ResourceArn: m.ModelArn, Tags: []types.Tag{{Key: aws.String("team"), Value: aws.String("ai")}}}); err != nil {
		t.Fatal(err)
	}
	if tags, err := c.ListTags(ctx, &sagemaker.ListTagsInput{ResourceArn: m.ModelArn}); err != nil || len(tags.Tags) != 2 {
		t.Fatalf("tags: %v %+v", err, tags)
	}
	if _, err := c.DeleteTags(ctx, &sagemaker.DeleteTagsInput{ResourceArn: m.ModelArn, TagKeys: []string{"env"}}); err != nil {
		t.Fatal(err)
	}
	if tags, _ := c.ListTags(ctx, &sagemaker.ListTagsInput{ResourceArn: m.ModelArn}); len(tags.Tags) != 1 || aws.ToString(tags.Tags[0].Key) != "team" {
		t.Fatalf("tags after delete: %+v", tags)
	}

	// an endpoint needs its configuration, a configuration its model
	_, err = c.CreateEndpoint(ctx, &sagemaker.CreateEndpointInput{EndpointName: aws.String("ep"), EndpointConfigName: aws.String("nope")})
	aiNotFound(t, err)
	_, err = c.CreateEndpointConfig(ctx, &sagemaker.CreateEndpointConfigInput{EndpointConfigName: aws.String("cfg"),
		ProductionVariants: []types.ProductionVariant{{VariantName: aws.String("v"), ModelName: aws.String("no-such-model"), InitialInstanceCount: aws.Int32(1), InstanceType: types.ProductionVariantInstanceTypeMlM5Large}}})
	aiNotFound(t, err)

	variant := func(model string, n int32) []types.ProductionVariant {
		return []types.ProductionVariant{{VariantName: aws.String("AllTraffic"), ModelName: aws.String(model), InitialInstanceCount: aws.Int32(n), InstanceType: types.ProductionVariantInstanceTypeMlM5Large}}
	}
	cfg, err := c.CreateEndpointConfig(ctx, &sagemaker.CreateEndpointConfigInput{EndpointConfigName: aws.String("cfg"), ProductionVariants: variant("m1", 2)})
	if err != nil || aws.ToString(cfg.EndpointConfigArn) == "" {
		t.Fatalf("create config: %v %+v", err, cfg)
	}
	dc, err := c.DescribeEndpointConfig(ctx, &sagemaker.DescribeEndpointConfigInput{EndpointConfigName: aws.String("cfg")})
	if err != nil || len(dc.ProductionVariants) != 1 || aws.ToInt32(dc.ProductionVariants[0].InitialInstanceCount) != 2 {
		t.Fatalf("describe config: %v %+v", err, dc)
	}
	if lc, err := c.ListEndpointConfigs(ctx, &sagemaker.ListEndpointConfigsInput{}); err != nil || len(lc.EndpointConfigs) != 1 {
		t.Fatalf("list configs: %v %+v", err, lc)
	}
	if _, err := c.CreateEndpointConfig(ctx, &sagemaker.CreateEndpointConfigInput{EndpointConfigName: aws.String("cfg2"), ProductionVariants: variant("m1", 1)}); err != nil {
		t.Fatal(err)
	}

	// the endpoint is Creating, then InService
	ep, err := c.CreateEndpoint(ctx, &sagemaker.CreateEndpointInput{EndpointName: aws.String("ep"), EndpointConfigName: aws.String("cfg")})
	if err != nil || aws.ToString(ep.EndpointArn) != "arn:aws:sagemaker:us-east-1:000000000000:endpoint/ep" {
		t.Fatalf("create endpoint: %v %+v", err, ep)
	}
	d, err := c.DescribeEndpoint(ctx, &sagemaker.DescribeEndpointInput{EndpointName: aws.String("ep")})
	if err != nil || d.EndpointStatus != types.EndpointStatusCreating || aws.ToString(d.EndpointConfigName) != "cfg" {
		t.Fatalf("describe endpoint: %v %+v", err, d)
	}
	waiter := sagemaker.NewEndpointInServiceWaiter(c)
	wait := func() *sagemaker.DescribeEndpointOutput {
		out, err := waiter.WaitForOutput(ctx, &sagemaker.DescribeEndpointInput{EndpointName: aws.String("ep")}, 10*time.Second,
			func(o *sagemaker.EndpointInServiceWaiterOptions) {
				o.MinDelay, o.MaxDelay = 100*time.Millisecond, 200*time.Millisecond
			})
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	if d = wait(); len(d.ProductionVariants) != 1 || aws.ToInt32(d.ProductionVariants[0].CurrentInstanceCount) != 2 {
		t.Fatalf("variants: %+v", d.ProductionVariants)
	}
	if _, err := c.UpdateEndpoint(ctx, &sagemaker.UpdateEndpointInput{EndpointName: aws.String("ep"), EndpointConfigName: aws.String("cfg2")}); err != nil {
		t.Fatal(err)
	}
	if d, _ = c.DescribeEndpoint(ctx, &sagemaker.DescribeEndpointInput{EndpointName: aws.String("ep")}); d.EndpointStatus != types.EndpointStatusUpdating || aws.ToString(d.EndpointConfigName) != "cfg2" {
		t.Fatalf("after update: %+v", d)
	}
	if d = wait(); aws.ToInt32(d.ProductionVariants[0].CurrentInstanceCount) != 1 {
		t.Fatalf("variants after update: %+v", d.ProductionVariants)
	}
	if le, err := c.ListEndpoints(ctx, &sagemaker.ListEndpointsInput{}); err != nil || len(le.Endpoints) != 1 || le.Endpoints[0].EndpointStatus != types.EndpointStatusInService {
		t.Fatalf("list endpoints: %v %+v", err, le)
	}

	for _, del := range []func() error{
		func() error {
			_, err := c.DeleteEndpoint(ctx, &sagemaker.DeleteEndpointInput{EndpointName: aws.String("ep")})
			return err
		},
		func() error {
			_, err := c.DeleteEndpointConfig(ctx, &sagemaker.DeleteEndpointConfigInput{EndpointConfigName: aws.String("cfg")})
			return err
		},
		func() error {
			_, err := c.DeleteModel(ctx, &sagemaker.DeleteModelInput{ModelName: aws.String("m1")})
			return err
		},
	} {
		if err := del(); err != nil {
			t.Fatal(err)
		}
	}
	_, err = c.DescribeEndpoint(ctx, &sagemaker.DescribeEndpointInput{EndpointName: aws.String("ep")})
	aiNotFound(t, err)
	if !strings.Contains(err.Error(), "Could not find endpoint") { // the words Terraform looks for
		t.Fatalf("message: %v", err)
	}
	_, err = c.DescribeModel(ctx, &sagemaker.DescribeModelInput{ModelName: aws.String("m1")})
	aiNotFound(t, err)
	if _, err := c.CreateModel(ctx, &sagemaker.CreateModelInput{}); err == nil {
		t.Fatal("empty create succeeded")
	}
}

func TestSageMakerTrainingNotebookDomain(t *testing.T) {
	ctx := context.Background()
	c := sagemaker.NewFromConfig(aiStart(t, pogi.Config{}))

	tj, err := c.CreateTrainingJob(ctx, &sagemaker.CreateTrainingJobInput{TrainingJobName: aws.String("train-1"), RoleArn: aws.String(smRole),
		OutputDataConfig:       &types.OutputDataConfig{S3OutputPath: aws.String("s3://bucket/out")},
		AlgorithmSpecification: &types.AlgorithmSpecification{TrainingInputMode: types.TrainingInputModeFile, TrainingImage: aws.String("img")},
		ResourceConfig:         &types.ResourceConfig{InstanceType: types.TrainingInstanceTypeMlM5Large, InstanceCount: aws.Int32(1), VolumeSizeInGB: aws.Int32(5)},
		StoppingCondition:      &types.StoppingCondition{MaxRuntimeInSeconds: aws.Int32(60)}})
	if err != nil || aws.ToString(tj.TrainingJobArn) == "" {
		t.Fatalf("create training job: %v %+v", err, tj)
	}
	d, err := c.DescribeTrainingJob(ctx, &sagemaker.DescribeTrainingJobInput{TrainingJobName: aws.String("train-1")})
	if err != nil || d.TrainingJobStatus != types.TrainingJobStatusCompleted || d.SecondaryStatus != types.SecondaryStatusCompleted ||
		aws.ToString(d.ModelArtifacts.S3ModelArtifacts) != "s3://bucket/out/train-1/output/model.tar.gz" {
		t.Fatalf("describe training job: %v %+v", err, d)
	}
	if l, err := c.ListTrainingJobs(ctx, &sagemaker.ListTrainingJobsInput{}); err != nil || len(l.TrainingJobSummaries) != 1 || l.TrainingJobSummaries[0].TrainingJobStatus != types.TrainingJobStatusCompleted {
		t.Fatalf("list training jobs: %v %+v", err, l)
	}

	nb, err := c.CreateNotebookInstance(ctx, &sagemaker.CreateNotebookInstanceInput{NotebookInstanceName: aws.String("nb"), InstanceType: types.InstanceTypeMlT3Medium, RoleArn: aws.String(smRole)})
	if err != nil || aws.ToString(nb.NotebookInstanceArn) == "" {
		t.Fatalf("create notebook: %v %+v", err, nb)
	}
	dn, err := c.DescribeNotebookInstance(ctx, &sagemaker.DescribeNotebookInstanceInput{NotebookInstanceName: aws.String("nb")})
	if err != nil || dn.NotebookInstanceStatus != types.NotebookInstanceStatusPending || dn.InstanceType != types.InstanceTypeMlT3Medium {
		t.Fatalf("describe notebook: %v %+v", err, dn)
	}
	err = sagemaker.NewNotebookInstanceInServiceWaiter(c).Wait(ctx, &sagemaker.DescribeNotebookInstanceInput{NotebookInstanceName: aws.String("nb")}, 10*time.Second,
		func(o *sagemaker.NotebookInstanceInServiceWaiterOptions) {
			o.MinDelay, o.MaxDelay = 100*time.Millisecond, 200*time.Millisecond
		})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.StopNotebookInstance(ctx, &sagemaker.StopNotebookInstanceInput{NotebookInstanceName: aws.String("nb")}); err != nil {
		t.Fatal(err)
	}
	if dn, _ = c.DescribeNotebookInstance(ctx, &sagemaker.DescribeNotebookInstanceInput{NotebookInstanceName: aws.String("nb")}); dn.NotebookInstanceStatus != types.NotebookInstanceStatusStopped {
		t.Fatalf("stopped notebook: %v", dn.NotebookInstanceStatus)
	}
	if _, err := c.DeleteNotebookInstance(ctx, &sagemaker.DeleteNotebookInstanceInput{NotebookInstanceName: aws.String("nb")}); err != nil {
		t.Fatal(err)
	}
	_, err = c.DescribeNotebookInstance(ctx, &sagemaker.DescribeNotebookInstanceInput{NotebookInstanceName: aws.String("nb")})
	aiNotFound(t, err)

	dom, err := c.CreateDomain(ctx, &sagemaker.CreateDomainInput{DomainName: aws.String("studio"), AuthMode: types.AuthModeIam, VpcId: aws.String("vpc-1"), SubnetIds: []string{"subnet-1"},
		DefaultUserSettings: &types.UserSettings{ExecutionRole: aws.String(smRole)}})
	if err != nil || !strings.HasPrefix(aws.ToString(dom.DomainId), "d-") || aws.ToString(dom.Url) == "" {
		t.Fatalf("create domain: %v %+v", err, dom)
	}
	dd, err := c.DescribeDomain(ctx, &sagemaker.DescribeDomainInput{DomainId: dom.DomainId})
	if err != nil || aws.ToString(dd.DomainName) != "studio" || aws.ToString(dd.DomainArn) != aws.ToString(dom.DomainArn) {
		t.Fatalf("describe domain: %v %+v", err, dd)
	}
	for i := 0; dd.Status != types.DomainStatusInService; i++ {
		if i > 50 {
			t.Fatalf("domain stuck in %v", dd.Status)
		}
		time.Sleep(100 * time.Millisecond)
		dd, _ = c.DescribeDomain(ctx, &sagemaker.DescribeDomainInput{DomainId: dom.DomainId})
	}
	if l, err := c.ListDomains(ctx, &sagemaker.ListDomainsInput{}); err != nil || len(l.Domains) != 1 || l.Domains[0].Status != types.DomainStatusInService {
		t.Fatalf("list domains: %v %+v", err, l)
	}
	if _, err := c.DeleteDomain(ctx, &sagemaker.DeleteDomainInput{DomainId: dom.DomainId}); err != nil {
		t.Fatal(err)
	}
	_, err = c.DescribeDomain(ctx, &sagemaker.DescribeDomainInput{DomainId: dom.DomainId})
	aiNotFound(t, err)
}

func TestSageMakerRuntime(t *testing.T) {
	ctx := context.Background()
	fm := aiNewFakeModel(t)
	cfg := aiStart(t, pogi.Config{AIEndpoint: fm.URL, AIModels: map[string]string{"chat-*": "m"}})
	c, rt := sagemaker.NewFromConfig(cfg), sagemakerruntime.NewFromConfig(cfg)
	if _, err := c.CreateModel(ctx, &sagemaker.CreateModelInput{ModelName: aws.String("m"), ExecutionRoleArn: aws.String(smRole), PrimaryContainer: &types.ContainerDefinition{Image: aws.String("img")}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateEndpointConfig(ctx, &sagemaker.CreateEndpointConfigInput{EndpointConfigName: aws.String("c"), ProductionVariants: []types.ProductionVariant{{VariantName: aws.String("AllTraffic"),
		ModelName: aws.String("m"), InitialInstanceCount: aws.Int32(1), InstanceType: types.ProductionVariantInstanceTypeMlM5Large}}}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"chat-bot", "other"} {
		if _, err := c.CreateEndpoint(ctx, &sagemaker.CreateEndpointInput{EndpointName: aws.String(name), EndpointConfigName: aws.String("c")}); err != nil {
			t.Fatal(err)
		}
	}

	out, err := rt.InvokeEndpoint(ctx, &sagemakerruntime.InvokeEndpointInput{EndpointName: aws.String("chat-bot"), ContentType: aws.String("application/json"), Body: []byte(`{"inputs":"hello"}`)})
	if err != nil {
		t.Fatal(err)
	}
	var got []map[string]string
	if err := json.Unmarshal(out.Body, &got); err != nil || len(got) != 1 || got[0]["generated_text"] != "local says: hello" || aws.ToString(out.ContentType) != "application/json" {
		t.Fatalf("local answer: %v %s %v", err, out.Body, aws.ToString(out.ContentType))
	}
	// an endpoint no glob maps answers with canned text, the same every time
	var texts []string
	for range 2 {
		out, err = rt.InvokeEndpoint(ctx, &sagemakerruntime.InvokeEndpointInput{EndpointName: aws.String("other"), ContentType: aws.String("text/csv"), Body: []byte("1,2,3")})
		if err != nil {
			t.Fatal(err)
		}
		texts = append(texts, string(out.Body))
	}
	if texts[0] != texts[1] || !strings.Contains(texts[0], "canned") {
		t.Fatalf("canned answer: %q", texts)
	}
	_, err = rt.InvokeEndpoint(ctx, &sagemakerruntime.InvokeEndpointInput{EndpointName: aws.String("missing"), Body: []byte("x")})
	var ae smithy.APIError
	if !errors.As(err, &ae) || ae.ErrorCode() != "ValidationError" {
		t.Fatalf("missing endpoint: %T %v", err, err)
	}
}
