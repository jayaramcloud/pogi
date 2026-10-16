package generic_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfg "github.com/aws/aws-sdk-go-v2/service/configservice"
	"github.com/aws/aws-sdk-go-v2/service/configservice/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestConfig(t *testing.T) {
	_, url := start(t, "config")
	ctx := context.Background()
	c := cfg.NewFromConfig(awstest.Config(t, url))

	// recorder: put, describe, status through start and stop
	if _, err := c.PutConfigurationRecorder(ctx, &cfg.PutConfigurationRecorderInput{ConfigurationRecorder: &types.ConfigurationRecorder{
		Name: aws.String("default"), RoleARN: aws.String("arn:aws:iam::000000000000:role/config"),
		RecordingGroup: &types.RecordingGroup{AllSupported: true}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.PutConfigurationRecorder(ctx, &cfg.PutConfigurationRecorderInput{ConfigurationRecorder: &types.ConfigurationRecorder{Name: aws.String("second")}}); err == nil {
		t.Fatal("a second recorder was accepted")
	}
	d, err := c.DescribeConfigurationRecorders(ctx, &cfg.DescribeConfigurationRecordersInput{})
	if err != nil || len(d.ConfigurationRecorders) != 1 || aws.ToString(d.ConfigurationRecorders[0].RoleARN) != "arn:aws:iam::000000000000:role/config" ||
		!d.ConfigurationRecorders[0].RecordingGroup.AllSupported {
		t.Fatalf("recorders: %v %+v", err, d)
	}
	if _, err := c.StartConfigurationRecorder(ctx, &cfg.StartConfigurationRecorderInput{ConfigurationRecorderName: aws.String("default")}); err == nil {
		t.Fatal("recorder started without a delivery channel")
	} else {
		var e *types.NoAvailableDeliveryChannelException
		if !errors.As(err, &e) {
			t.Fatalf("start without channel: %T %v", err, err)
		}
	}
	if _, err := c.PutDeliveryChannel(ctx, &cfg.PutDeliveryChannelInput{DeliveryChannel: &types.DeliveryChannel{Name: aws.String("default"), S3BucketName: aws.String("config-bucket")}}); err != nil {
		t.Fatal(err)
	}
	ch, err := c.DescribeDeliveryChannels(ctx, &cfg.DescribeDeliveryChannelsInput{})
	if err != nil || len(ch.DeliveryChannels) != 1 || aws.ToString(ch.DeliveryChannels[0].S3BucketName) != "config-bucket" {
		t.Fatalf("channels: %v %+v", err, ch)
	}
	st, _ := c.DescribeConfigurationRecorderStatus(ctx, &cfg.DescribeConfigurationRecorderStatusInput{})
	if len(st.ConfigurationRecordersStatus) != 1 || st.ConfigurationRecordersStatus[0].Recording {
		t.Fatalf("status before start: %+v", st)
	}
	if _, err := c.StartConfigurationRecorder(ctx, &cfg.StartConfigurationRecorderInput{ConfigurationRecorderName: aws.String("default")}); err != nil {
		t.Fatal(err)
	}
	st, _ = c.DescribeConfigurationRecorderStatus(ctx, &cfg.DescribeConfigurationRecorderStatusInput{ConfigurationRecorderNames: []string{"default"}})
	if s := st.ConfigurationRecordersStatus[0]; !s.Recording || s.LastStartTime == nil || s.LastStatus != types.RecorderStatusSuccess {
		t.Fatalf("status after start: %+v", s)
	}
	if _, err := c.DeleteDeliveryChannel(ctx, &cfg.DeleteDeliveryChannelInput{DeliveryChannelName: aws.String("default")}); err == nil {
		t.Fatal("channel deleted while recording")
	}

	// rule, evaluations
	if _, err := c.PutConfigRule(ctx, &cfg.PutConfigRuleInput{ConfigRule: &types.ConfigRule{ConfigRuleName: aws.String("s3-encrypted"),
		Source: &types.Source{Owner: types.OwnerCustomLambda, SourceIdentifier: aws.String("arn:aws:lambda:us-east-1:000000000000:function:check")}},
		Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}}); err != nil {
		t.Fatal(err)
	}
	rs, err := c.DescribeConfigRules(ctx, &cfg.DescribeConfigRulesInput{ConfigRuleNames: []string{"s3-encrypted"}})
	if err != nil || len(rs.ConfigRules) != 1 {
		t.Fatalf("rules: %v %+v", err, rs)
	}
	rule := rs.ConfigRules[0]
	if aws.ToString(rule.ConfigRuleArn) == "" || aws.ToString(rule.ConfigRuleId) == "" || rule.ConfigRuleState != types.ConfigRuleStateActive {
		t.Fatalf("rule: %+v", rule)
	}
	if _, err := c.StartConfigRulesEvaluation(ctx, &cfg.StartConfigRulesEvaluationInput{ConfigRuleNames: []string{"s3-encrypted"}}); err != nil {
		t.Fatal(err)
	}
	dc, _ := c.DescribeComplianceByConfigRule(ctx, &cfg.DescribeComplianceByConfigRuleInput{ConfigRuleNames: []string{"s3-encrypted"}})
	if dc.ComplianceByConfigRules[0].Compliance.ComplianceType != types.ComplianceTypeInsufficientData {
		t.Fatalf("compliance before: %+v", dc)
	}
	now := time.Now()
	if _, err := c.PutEvaluations(ctx, &cfg.PutEvaluationsInput{ResultToken: aws.String("bogus"), Evaluations: []types.Evaluation{
		{ComplianceResourceId: aws.String("b1"), ComplianceResourceType: aws.String("AWS::S3::Bucket"), ComplianceType: types.ComplianceTypeCompliant, OrderingTimestamp: &now}}}); err == nil {
		t.Fatal("a bogus result token was accepted")
	} else {
		var e *types.InvalidResultTokenException
		if !errors.As(err, &e) {
			t.Fatalf("bogus token: %T %v", err, err)
		}
	}
	if _, err := c.PutEvaluations(ctx, &cfg.PutEvaluationsInput{ResultToken: rule.ConfigRuleArn, Evaluations: []types.Evaluation{
		{ComplianceResourceId: aws.String("b1"), ComplianceResourceType: aws.String("AWS::S3::Bucket"), ComplianceType: types.ComplianceTypeCompliant, OrderingTimestamp: &now},
		{ComplianceResourceId: aws.String("b2"), ComplianceResourceType: aws.String("AWS::S3::Bucket"), ComplianceType: types.ComplianceTypeNonCompliant, Annotation: aws.String("no encryption"), OrderingTimestamp: &now},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.PutExternalEvaluation(ctx, &cfg.PutExternalEvaluationInput{ConfigRuleName: aws.String("s3-encrypted"), ExternalEvaluation: &types.ExternalEvaluation{
		ComplianceResourceId: aws.String("b1"), ComplianceResourceType: aws.String("AWS::S3::Bucket"), ComplianceType: types.ComplianceTypeNonCompliant, OrderingTimestamp: &now}}); err != nil {
		t.Fatal(err)
	}
	det, err := c.GetComplianceDetailsByConfigRule(ctx, &cfg.GetComplianceDetailsByConfigRuleInput{ConfigRuleName: aws.String("s3-encrypted")})
	if err != nil || len(det.EvaluationResults) != 2 { // b1 was evaluated twice: the latest stays
		t.Fatalf("details: %v %+v", err, det)
	}
	for _, r := range det.EvaluationResults {
		q := r.EvaluationResultIdentifier.EvaluationResultQualifier
		if r.ComplianceType != types.ComplianceTypeNonCompliant || aws.ToString(q.ConfigRuleName) != "s3-encrypted" || aws.ToString(q.ResourceType) != "AWS::S3::Bucket" || r.ResultRecordedTime == nil {
			t.Fatalf("result: %+v", r)
		}
	}
	det, _ = c.GetComplianceDetailsByConfigRule(ctx, &cfg.GetComplianceDetailsByConfigRuleInput{ConfigRuleName: aws.String("s3-encrypted"), ComplianceTypes: []types.ComplianceType{types.ComplianceTypeCompliant}})
	if len(det.EvaluationResults) != 0 {
		t.Fatalf("filtered details: %+v", det)
	}
	dc, _ = c.DescribeComplianceByConfigRule(ctx, &cfg.DescribeComplianceByConfigRuleInput{})
	if dc.ComplianceByConfigRules[0].Compliance.ComplianceType != types.ComplianceTypeNonCompliant {
		t.Fatalf("compliance after: %+v", dc)
	}
	tags, err := c.ListTagsForResource(ctx, &cfg.ListTagsForResourceInput{ResourceArn: rule.ConfigRuleArn})
	if err != nil || len(tags.Tags) != 1 {
		t.Fatalf("tags: %v %+v", err, tags)
	}
	if _, err := c.TagResource(ctx, &cfg.TagResourceInput{ResourceArn: rule.ConfigRuleArn, Tags: []types.Tag{{Key: aws.String("a"), Value: aws.String("b")}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.UntagResource(ctx, &cfg.UntagResourceInput{ResourceArn: rule.ConfigRuleArn, TagKeys: []string{"env"}}); err != nil {
		t.Fatal(err)
	}
	tags, _ = c.ListTagsForResource(ctx, &cfg.ListTagsForResourceInput{ResourceArn: rule.ConfigRuleArn})
	if len(tags.Tags) != 1 || aws.ToString(tags.Tags[0].Key) != "a" {
		t.Fatalf("tags after: %+v", tags)
	}
	// updating a rule keeps its id
	if _, err := c.PutConfigRule(ctx, &cfg.PutConfigRuleInput{ConfigRule: &types.ConfigRule{ConfigRuleName: aws.String("s3-encrypted"), Description: aws.String("v2"),
		Source: &types.Source{Owner: types.OwnerCustomLambda, SourceIdentifier: aws.String("arn:aws:lambda:us-east-1:000000000000:function:check")}}}); err != nil {
		t.Fatal(err)
	}
	rs, _ = c.DescribeConfigRules(ctx, &cfg.DescribeConfigRulesInput{})
	if len(rs.ConfigRules) != 1 || aws.ToString(rs.ConfigRules[0].ConfigRuleId) != aws.ToString(rule.ConfigRuleId) || aws.ToString(rs.ConfigRules[0].Description) != "v2" {
		t.Fatalf("updated rule: %+v", rs.ConfigRules)
	}

	// conformance pack
	pk, err := c.PutConformancePack(ctx, &cfg.PutConformancePackInput{ConformancePackName: aws.String("pack"), TemplateBody: aws.String("Resources: {}"), DeliveryS3Bucket: aws.String("config-bucket")})
	if err != nil || aws.ToString(pk.ConformancePackArn) == "" {
		t.Fatalf("pack: %v %+v", err, pk)
	}
	cp, err := c.DescribeConformancePacks(ctx, &cfg.DescribeConformancePacksInput{ConformancePackNames: []string{"pack"}})
	if err != nil || len(cp.ConformancePackDetails) != 1 || aws.ToString(cp.ConformancePackDetails[0].DeliveryS3Bucket) != "config-bucket" {
		t.Fatalf("describe pack: %v %+v", err, cp)
	}
	cs, err := c.DescribeConformancePackStatus(ctx, &cfg.DescribeConformancePackStatusInput{})
	if err != nil || len(cs.ConformancePackStatusDetails) != 1 || cs.ConformancePackStatusDetails[0].ConformancePackState != types.ConformancePackStateCreateComplete {
		t.Fatalf("pack status: %v %+v", err, cs)
	}
	if _, err := c.DeleteConformancePack(ctx, &cfg.DeleteConformancePackInput{ConformancePackName: aws.String("pack")}); err != nil {
		t.Fatal(err)
	}

	// delete everything; the not-found errors are typed
	if _, err := c.DeleteConfigRule(ctx, &cfg.DeleteConfigRuleInput{ConfigRuleName: aws.String("s3-encrypted")}); err != nil {
		t.Fatal(err)
	}
	_, err = c.DescribeConfigRules(ctx, &cfg.DescribeConfigRulesInput{ConfigRuleNames: []string{"s3-encrypted"}})
	var nfr *types.NoSuchConfigRuleException
	if !errors.As(err, &nfr) {
		t.Fatalf("rule not found: %T %v", err, err)
	}
	if _, err := c.GetComplianceDetailsByConfigRule(ctx, &cfg.GetComplianceDetailsByConfigRuleInput{ConfigRuleName: aws.String("s3-encrypted")}); !errors.As(err, &nfr) {
		t.Fatalf("details of a deleted rule: %T %v", err, err)
	}
	if _, err := c.StopConfigurationRecorder(ctx, &cfg.StopConfigurationRecorderInput{ConfigurationRecorderName: aws.String("default")}); err != nil {
		t.Fatal(err)
	}
	st, _ = c.DescribeConfigurationRecorderStatus(ctx, &cfg.DescribeConfigurationRecorderStatusInput{})
	if st.ConfigurationRecordersStatus[0].Recording || st.ConfigurationRecordersStatus[0].LastStopTime == nil {
		t.Fatalf("status after stop: %+v", st)
	}
	if _, err := c.DeleteDeliveryChannel(ctx, &cfg.DeleteDeliveryChannelInput{DeliveryChannelName: aws.String("default")}); err != nil {
		t.Fatal(err)
	}
	var nfc *types.NoSuchDeliveryChannelException
	if _, err := c.DescribeDeliveryChannels(ctx, &cfg.DescribeDeliveryChannelsInput{DeliveryChannelNames: []string{"default"}}); !errors.As(err, &nfc) {
		t.Fatalf("channel not found: %T %v", err, err)
	}
	if _, err := c.DeleteConfigurationRecorder(ctx, &cfg.DeleteConfigurationRecorderInput{ConfigurationRecorderName: aws.String("default")}); err != nil {
		t.Fatal(err)
	}
	var nfrec *types.NoSuchConfigurationRecorderException
	if _, err := c.DescribeConfigurationRecorders(ctx, &cfg.DescribeConfigurationRecordersInput{ConfigurationRecorderNames: []string{"default"}}); !errors.As(err, &nfrec) {
		t.Fatalf("recorder not found: %T %v", err, err)
	}
}
