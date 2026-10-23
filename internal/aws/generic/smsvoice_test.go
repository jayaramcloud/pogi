package generic_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/pinpointsmsvoicev2"
	"github.com/aws/aws-sdk-go-v2/service/pinpointsmsvoicev2/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestEndUserMessaging(t *testing.T) {
	_, url := start(t, "pinpoint-sms-voice-v2")
	ctx := context.Background()
	c := pinpointsmsvoicev2.NewFromConfig(awstest.Config(t, url))

	if _, err := c.CreateConfigurationSet(ctx, &pinpointsmsvoicev2.CreateConfigurationSetInput{}); err == nil {
		t.Fatal("create without a name succeeded")
	}
	_, err := c.DeleteConfigurationSet(ctx, &pinpointsmsvoicev2.DeleteConfigurationSetInput{ConfigurationSetName: aws.String("none")})
	kCode(t, err, "ResourceNotFoundException")

	s, err := c.CreateConfigurationSet(ctx, &pinpointsmsvoicev2.CreateConfigurationSetInput{
		ConfigurationSetName: aws.String("cs"), Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}},
	})
	if err != nil || aws.ToString(s.ConfigurationSetName) != "cs" || aws.ToString(s.ConfigurationSetArn) == "" {
		t.Fatalf("create: %v %+v", err, s)
	}
	if _, err := c.CreateConfigurationSet(ctx, &pinpointsmsvoicev2.CreateConfigurationSetInput{ConfigurationSetName: aws.String("cs")}); err == nil {
		t.Fatal("duplicate create succeeded")
	}
	d, err := c.DescribeConfigurationSets(ctx, &pinpointsmsvoicev2.DescribeConfigurationSetsInput{ConfigurationSetNames: []string{"cs"}})
	if err != nil || len(d.ConfigurationSets) != 1 || aws.ToString(d.ConfigurationSets[0].ConfigurationSetName) != "cs" {
		t.Fatalf("describe: %v %+v", err, d)
	}
	o, err := c.CreateOptOutList(ctx, &pinpointsmsvoicev2.CreateOptOutListInput{OptOutListName: aws.String("stop")})
	if err != nil || aws.ToString(o.OptOutListName) != "stop" {
		t.Fatalf("opt-out list: %v %+v", err, o)
	}
	ol, err := c.DescribeOptOutLists(ctx, &pinpointsmsvoicev2.DescribeOptOutListsInput{})
	if err != nil || len(ol.OptOutLists) != 1 {
		t.Fatalf("opt-out lists: %v %+v", err, ol)
	}
	tg, err := c.ListTagsForResource(ctx, &pinpointsmsvoicev2.ListTagsForResourceInput{ResourceArn: s.ConfigurationSetArn})
	if err != nil || len(tg.Tags) != 1 {
		t.Fatalf("tags: %v %+v", err, tg)
	}
	if _, err := c.DeleteConfigurationSet(ctx, &pinpointsmsvoicev2.DeleteConfigurationSetInput{ConfigurationSetName: aws.String("cs")}); err != nil {
		t.Fatal(err)
	}
	_, err = c.DeleteConfigurationSet(ctx, &pinpointsmsvoicev2.DeleteConfigurationSetInput{ConfigurationSetName: aws.String("cs")})
	kCode(t, err, "ResourceNotFoundException")
}
