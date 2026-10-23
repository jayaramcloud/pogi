package generic_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/notifications"
	"github.com/aws/aws-sdk-go-v2/service/notifications/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestNotifications(t *testing.T) {
	_, url := start(t, "notifications")
	ctx := context.Background()
	c := notifications.NewFromConfig(awstest.Config(t, url))

	if _, err := c.CreateNotificationConfiguration(ctx, &notifications.CreateNotificationConfigurationInput{Name: aws.String("n")}); err == nil {
		t.Fatal("create without a description succeeded")
	}
	_, err := c.GetNotificationConfiguration(ctx, &notifications.GetNotificationConfigurationInput{Arn: aws.String("arn:aws:notifications::000000000000:configuration/none")})
	kCode(t, err, "ResourceNotFoundException")

	n, err := c.CreateNotificationConfiguration(ctx, &notifications.CreateNotificationConfigurationInput{
		Name: aws.String("ec2"), Description: aws.String("EC2 state changes"), Tags: map[string]string{"env": "dev"},
	})
	if err != nil || n.Status != types.NotificationConfigurationStatusActive {
		t.Fatalf("create: %v %+v", err, n)
	}
	if _, err := c.CreateNotificationConfiguration(ctx, &notifications.CreateNotificationConfigurationInput{Name: aws.String("ec2"), Description: aws.String("again")}); err == nil {
		t.Fatal("duplicate create succeeded")
	}
	g, err := c.GetNotificationConfiguration(ctx, &notifications.GetNotificationConfigurationInput{Arn: n.Arn})
	if err != nil || aws.ToString(g.Name) != "ec2" || aws.ToString(g.Description) != "EC2 state changes" {
		t.Fatalf("get: %v %+v", err, g)
	}
	r, err := c.CreateEventRule(ctx, &notifications.CreateEventRuleInput{
		NotificationConfigurationArn: n.Arn, Source: aws.String("aws.ec2"), EventType: aws.String("EC2 Instance State-change Notification"), Regions: []string{"us-east-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	er, err := c.GetEventRule(ctx, &notifications.GetEventRuleInput{Arn: r.Arn})
	if err != nil || aws.ToString(er.Source) != "aws.ec2" || aws.ToString(er.NotificationConfigurationArn) != aws.ToString(n.Arn) {
		t.Fatalf("rule: %v %+v", err, er)
	}
	l, err := c.ListNotificationConfigurations(ctx, &notifications.ListNotificationConfigurationsInput{})
	if err != nil || len(l.NotificationConfigurations) != 1 {
		t.Fatalf("list: %v %+v", err, l)
	}
	tg, err := c.ListTagsForResource(ctx, &notifications.ListTagsForResourceInput{Arn: n.Arn})
	if err != nil || tg.Tags["env"] != "dev" {
		t.Fatalf("tags: %v %+v", err, tg)
	}
	if _, err := c.DeleteNotificationConfiguration(ctx, &notifications.DeleteNotificationConfigurationInput{Arn: n.Arn}); err != nil {
		t.Fatal(err)
	}
	_, err = c.GetNotificationConfiguration(ctx, &notifications.GetNotificationConfigurationInput{Arn: n.Arn})
	kCode(t, err, "ResourceNotFoundException")
}
