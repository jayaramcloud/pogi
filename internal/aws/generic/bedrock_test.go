package generic_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/bedrock"
	"github.com/aws/aws-sdk-go-v2/service/bedrock/types"
	"github.com/nrndev/pogi"
)

func TestBedrockGuardrail(t *testing.T) {
	ctx := context.Background()
	c := bedrock.NewFromConfig(aiStart(t, pogi.Config{}))

	g, err := c.CreateGuardrail(ctx, &bedrock.CreateGuardrailInput{Name: aws.String("safe"), BlockedInputMessaging: aws.String("no"), BlockedOutputsMessaging: aws.String("no"),
		Description: aws.String("first"), Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}})
	if err != nil {
		t.Fatal(err)
	}
	arn := aws.ToString(g.GuardrailArn)
	if aws.ToString(g.GuardrailId) == "" || arn == "" || aws.ToString(g.Version) != "DRAFT" {
		t.Fatalf("create: %+v", g)
	}
	got, err := c.GetGuardrail(ctx, &bedrock.GetGuardrailInput{GuardrailIdentifier: g.GuardrailId})
	if err != nil || aws.ToString(got.Name) != "safe" || aws.ToString(got.Description) != "first" || got.Status != types.GuardrailStatusReady || aws.ToString(got.Version) != "DRAFT" {
		t.Fatalf("get: %v %+v", err, got)
	}
	if byARN, err := c.GetGuardrail(ctx, &bedrock.GetGuardrailInput{GuardrailIdentifier: aws.String(arn)}); err != nil || aws.ToString(byARN.GuardrailId) != aws.ToString(g.GuardrailId) {
		t.Fatalf("get by arn: %v", err)
	}
	l, err := c.ListGuardrails(ctx, &bedrock.ListGuardrailsInput{})
	if err != nil || len(l.Guardrails) != 1 || aws.ToString(l.Guardrails[0].Name) != "safe" {
		t.Fatalf("list: %v %+v", err, l)
	}
	if _, err := c.UpdateGuardrail(ctx, &bedrock.UpdateGuardrailInput{GuardrailIdentifier: g.GuardrailId, Name: aws.String("safer"),
		BlockedInputMessaging: aws.String("nope"), BlockedOutputsMessaging: aws.String("nope")}); err != nil {
		t.Fatal(err)
	}
	if got, _ = c.GetGuardrail(ctx, &bedrock.GetGuardrailInput{GuardrailIdentifier: g.GuardrailId}); aws.ToString(got.Name) != "safer" || aws.ToString(got.BlockedInputMessaging) != "nope" {
		t.Fatalf("after update: %+v", got)
	}
	v, err := c.CreateGuardrailVersion(ctx, &bedrock.CreateGuardrailVersionInput{GuardrailIdentifier: g.GuardrailId})
	if err != nil || aws.ToString(v.Version) != "1" {
		t.Fatalf("version: %v %+v", err, v)
	}
	if _, err := c.TagResource(ctx, &bedrock.TagResourceInput{ResourceARN: g.GuardrailArn, Tags: []types.Tag{{Key: aws.String("team"), Value: aws.String("ai")}}}); err != nil {
		t.Fatal(err)
	}
	tags, err := c.ListTagsForResource(ctx, &bedrock.ListTagsForResourceInput{ResourceARN: g.GuardrailArn})
	if err != nil || len(tags.Tags) != 2 {
		t.Fatalf("tags: %v %+v", err, tags)
	}
	if _, err := c.UntagResource(ctx, &bedrock.UntagResourceInput{ResourceARN: g.GuardrailArn, TagKeys: []string{"env"}}); err != nil {
		t.Fatal(err)
	}
	if tags, _ = c.ListTagsForResource(ctx, &bedrock.ListTagsForResourceInput{ResourceARN: g.GuardrailArn}); len(tags.Tags) != 1 {
		t.Fatalf("tags after untag: %+v", tags)
	}
	if _, err := c.DeleteGuardrail(ctx, &bedrock.DeleteGuardrailInput{GuardrailIdentifier: g.GuardrailId}); err != nil {
		t.Fatal(err)
	}
	var nf *types.ResourceNotFoundException
	if _, err := c.GetGuardrail(ctx, &bedrock.GetGuardrailInput{GuardrailIdentifier: g.GuardrailId}); !errors.As(err, &nf) {
		t.Fatalf("get deleted: %T %v", err, err)
	}
	if _, err := c.CreateGuardrail(ctx, &bedrock.CreateGuardrailInput{}); err == nil {
		t.Fatal("empty create succeeded")
	}
}

func TestBedrockFoundationModels(t *testing.T) {
	ctx := context.Background()
	c := bedrock.NewFromConfig(aiStart(t, pogi.Config{}))
	all, err := c.ListFoundationModels(ctx, &bedrock.ListFoundationModelsInput{})
	if err != nil || len(all.ModelSummaries) < 20 {
		t.Fatalf("list: %v %d", err, len(all.ModelSummaries))
	}
	an, _ := c.ListFoundationModels(ctx, &bedrock.ListFoundationModelsInput{ByProvider: aws.String("Anthropic")})
	if len(an.ModelSummaries) == 0 || len(an.ModelSummaries) >= len(all.ModelSummaries) {
		t.Fatalf("by provider: %d", len(an.ModelSummaries))
	}
	emb, _ := c.ListFoundationModels(ctx, &bedrock.ListFoundationModelsInput{ByOutputModality: types.ModelModalityEmbedding})
	for _, m := range emb.ModelSummaries {
		if len(m.OutputModalities) != 1 || m.OutputModalities[0] != types.ModelModalityEmbedding {
			t.Fatalf("by output modality: %+v", m)
		}
	}
	if len(emb.ModelSummaries) < 3 {
		t.Fatalf("embedding models: %d", len(emb.ModelSummaries))
	}
	m, err := c.GetFoundationModel(ctx, &bedrock.GetFoundationModelInput{ModelIdentifier: aws.String("anthropic.claude-3-haiku-20240307-v1:0")})
	if err != nil {
		t.Fatal(err)
	}
	d := m.ModelDetails
	if aws.ToString(d.ModelName) != "Claude 3 Haiku" || aws.ToString(d.ProviderName) != "Anthropic" || !aws.ToBool(d.ResponseStreamingSupported) ||
		d.ModelLifecycle.Status != types.FoundationModelLifecycleStatusActive || aws.ToString(d.ModelArn) != "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-haiku-20240307-v1:0" {
		t.Fatalf("details: %+v", d)
	}
	var nf *types.ResourceNotFoundException
	if _, err := c.GetFoundationModel(ctx, &bedrock.GetFoundationModelInput{ModelIdentifier: aws.String("acme.nothing-v1")}); !errors.As(err, &nf) {
		t.Fatalf("unknown model: %T %v", err, err)
	}
}

func TestBedrockProvisionedThroughputAndCustomModels(t *testing.T) {
	ctx := context.Background()
	c := bedrock.NewFromConfig(aiStart(t, pogi.Config{}))

	p, err := c.CreateProvisionedModelThroughput(ctx, &bedrock.CreateProvisionedModelThroughputInput{ProvisionedModelName: aws.String("pt"), ModelId: aws.String("amazon.titan-text-express-v1"),
		ModelUnits: aws.Int32(2), Tags: []types.Tag{{Key: aws.String("a"), Value: aws.String("b")}}})
	if err != nil || aws.ToString(p.ProvisionedModelArn) == "" {
		t.Fatalf("create pt: %v %+v", err, p)
	}
	for _, id := range []string{aws.ToString(p.ProvisionedModelArn), "pt"} { // by ARN and by name
		g, err := c.GetProvisionedModelThroughput(ctx, &bedrock.GetProvisionedModelThroughputInput{ProvisionedModelId: aws.String(id)})
		if err != nil || g.Status != types.ProvisionedModelStatusInService || aws.ToInt32(g.ModelUnits) != 2 || aws.ToString(g.ProvisionedModelName) != "pt" ||
			aws.ToString(g.FoundationModelArn) != "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-text-express-v1" {
			t.Fatalf("get pt %q: %v %+v", id, err, g)
		}
	}
	if _, err := c.UpdateProvisionedModelThroughput(ctx, &bedrock.UpdateProvisionedModelThroughputInput{ProvisionedModelId: p.ProvisionedModelArn, DesiredProvisionedModelName: aws.String("pt2")}); err != nil {
		t.Fatal(err)
	}
	l, err := c.ListProvisionedModelThroughputs(ctx, &bedrock.ListProvisionedModelThroughputsInput{})
	if err != nil || len(l.ProvisionedModelSummaries) != 1 || aws.ToString(l.ProvisionedModelSummaries[0].ProvisionedModelName) != "pt2" {
		t.Fatalf("list pt: %v %+v", err, l)
	}
	if tags, err := c.ListTagsForResource(ctx, &bedrock.ListTagsForResourceInput{ResourceARN: p.ProvisionedModelArn}); err != nil || len(tags.Tags) != 1 {
		t.Fatalf("pt tags: %v %+v", err, tags)
	}
	if _, err := c.DeleteProvisionedModelThroughput(ctx, &bedrock.DeleteProvisionedModelThroughputInput{ProvisionedModelId: p.ProvisionedModelArn}); err != nil {
		t.Fatal(err)
	}
	var nf *types.ResourceNotFoundException
	if _, err := c.GetProvisionedModelThroughput(ctx, &bedrock.GetProvisionedModelThroughputInput{ProvisionedModelId: p.ProvisionedModelArn}); !errors.As(err, &nf) {
		t.Fatalf("get deleted pt: %T %v", err, err)
	}

	// A customization job completes and leaves a custom model.
	job, err := c.CreateModelCustomizationJob(ctx, &bedrock.CreateModelCustomizationJobInput{JobName: aws.String("ft"), CustomModelName: aws.String("my-model"),
		RoleArn: aws.String("arn:aws:iam::000000000000:role/r"), BaseModelIdentifier: aws.String("amazon.titan-text-express-v1"),
		TrainingDataConfig: &types.TrainingDataConfig{S3Uri: aws.String("s3://b/train.jsonl")}, OutputDataConfig: &types.OutputDataConfig{S3Uri: aws.String("s3://b/out/")},
		HyperParameters: map[string]string{"epochCount": "1"}, CustomModelTags: []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}}})
	if err != nil {
		t.Fatal(err)
	}
	j, err := c.GetModelCustomizationJob(ctx, &bedrock.GetModelCustomizationJobInput{JobIdentifier: job.JobArn})
	if err != nil || j.Status != types.ModelCustomizationJobStatusCompleted || aws.ToString(j.OutputModelName) != "my-model" || j.HyperParameters["epochCount"] != "1" {
		t.Fatalf("job: %v %+v", err, j)
	}
	cm, err := c.GetCustomModel(ctx, &bedrock.GetCustomModelInput{ModelIdentifier: aws.String("my-model")})
	if err != nil || aws.ToString(cm.ModelArn) != aws.ToString(j.OutputModelArn) || cm.ModelStatus != types.ModelStatusActive || aws.ToString(cm.JobArn) != aws.ToString(job.JobArn) {
		t.Fatalf("custom model: %v %+v", err, cm)
	}
	if byARN, err := c.GetCustomModel(ctx, &bedrock.GetCustomModelInput{ModelIdentifier: j.OutputModelArn}); err != nil || aws.ToString(byARN.ModelName) != "my-model" {
		t.Fatalf("custom model by arn: %v", err)
	}
	lm, err := c.ListCustomModels(ctx, &bedrock.ListCustomModelsInput{})
	if err != nil || len(lm.ModelSummaries) != 1 || aws.ToString(lm.ModelSummaries[0].ModelName) != "my-model" {
		t.Fatalf("list custom models: %v %+v", err, lm)
	}
	if lj, err := c.ListModelCustomizationJobs(ctx, &bedrock.ListModelCustomizationJobsInput{}); err != nil || len(lj.ModelCustomizationJobSummaries) != 1 {
		t.Fatalf("list jobs: %v %+v", err, lj)
	}
	if tags, err := c.ListTagsForResource(ctx, &bedrock.ListTagsForResourceInput{ResourceARN: j.OutputModelArn}); err != nil || len(tags.Tags) != 1 {
		t.Fatalf("custom model tags: %v %+v", err, tags)
	}
	if _, err := c.DeleteCustomModel(ctx, &bedrock.DeleteCustomModelInput{ModelIdentifier: aws.String("my-model")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetCustomModel(ctx, &bedrock.GetCustomModelInput{ModelIdentifier: aws.String("my-model")}); !errors.As(err, &nf) {
		t.Fatalf("get deleted custom model: %T %v", err, err)
	}
}

func TestBedrockLoggingAndInferenceProfiles(t *testing.T) {
	ctx := context.Background()
	c := bedrock.NewFromConfig(aiStart(t, pogi.Config{}))

	if cfg, err := c.GetModelInvocationLoggingConfiguration(ctx, &bedrock.GetModelInvocationLoggingConfigurationInput{}); err != nil || cfg.LoggingConfig != nil {
		t.Fatalf("empty logging config: %v %+v", err, cfg)
	}
	if _, err := c.PutModelInvocationLoggingConfiguration(ctx, &bedrock.PutModelInvocationLoggingConfigurationInput{LoggingConfig: &types.LoggingConfig{
		TextDataDeliveryEnabled: aws.Bool(true), S3Config: &types.S3Config{BucketName: aws.String("logs"), KeyPrefix: aws.String("bedrock")}}}); err != nil {
		t.Fatal(err)
	}
	cfg, err := c.GetModelInvocationLoggingConfiguration(ctx, &bedrock.GetModelInvocationLoggingConfigurationInput{})
	if err != nil || !aws.ToBool(cfg.LoggingConfig.TextDataDeliveryEnabled) || aws.ToString(cfg.LoggingConfig.S3Config.BucketName) != "logs" {
		t.Fatalf("logging config: %v %+v", err, cfg)
	}
	if _, err := c.DeleteModelInvocationLoggingConfiguration(ctx, &bedrock.DeleteModelInvocationLoggingConfigurationInput{}); err != nil {
		t.Fatal(err)
	}
	if cfg, _ = c.GetModelInvocationLoggingConfiguration(ctx, &bedrock.GetModelInvocationLoggingConfigurationInput{}); cfg.LoggingConfig != nil {
		t.Fatalf("logging config after delete: %+v", cfg)
	}

	ip, err := c.CreateInferenceProfile(ctx, &bedrock.CreateInferenceProfileInput{InferenceProfileName: aws.String("prod"),
		ModelSource: &types.InferenceProfileModelSourceMemberCopyFrom{Value: "arn:aws:bedrock:us-east-1::foundation-model/amazon.nova-lite-v1:0"}})
	if err != nil || aws.ToString(ip.InferenceProfileArn) == "" {
		t.Fatalf("create profile: %v %+v", err, ip)
	}
	g, err := c.GetInferenceProfile(ctx, &bedrock.GetInferenceProfileInput{InferenceProfileIdentifier: ip.InferenceProfileArn})
	if err != nil || aws.ToString(g.InferenceProfileName) != "prod" || g.Type != types.InferenceProfileTypeApplication || g.Status != types.InferenceProfileStatusActive || len(g.Models) != 1 {
		t.Fatalf("get profile: %v %+v", err, g)
	}
	if _, err := c.DeleteInferenceProfile(ctx, &bedrock.DeleteInferenceProfileInput{InferenceProfileIdentifier: ip.InferenceProfileArn}); err != nil {
		t.Fatal(err)
	}
	var nf *types.ResourceNotFoundException
	if _, err := c.GetInferenceProfile(ctx, &bedrock.GetInferenceProfileInput{InferenceProfileIdentifier: ip.InferenceProfileArn}); !errors.As(err, &nf) {
		t.Fatalf("get deleted profile: %T %v", err, err)
	}
}
