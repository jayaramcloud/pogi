package generic_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/appstream"
	"github.com/aws/aws-sdk-go-v2/service/appstream/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestAppStream(t *testing.T) {
	_, url := start(t, "appstream")
	ctx := context.Background()
	c := appstream.NewFromConfig(awstest.Config(t, url))

	o, err := c.CreateStack(ctx, &appstream.CreateStackInput{Name: aws.String("apps"), Description: aws.String("d"), DisplayName: aws.String("Apps"),
		StorageConnectors: []types.StorageConnector{{ConnectorType: types.StorageConnectorTypeHomefolders}},
		Tags:              map[string]string{"env": "dev"}})
	if err != nil || o.Stack == nil || aws.ToString(o.Stack.Arn) == "" || aws.ToString(o.Stack.Name) != "apps" {
		t.Fatalf("create: %v %+v", err, o)
	}
	if _, err := c.CreateStack(ctx, &appstream.CreateStackInput{Name: aws.String("apps")}); err == nil {
		t.Fatal("duplicate stack created")
	}
	d, err := c.DescribeStacks(ctx, &appstream.DescribeStacksInput{Names: []string{"apps"}})
	if err != nil || len(d.Stacks) != 1 || aws.ToString(d.Stacks[0].Description) != "d" || len(d.Stacks[0].StorageConnectors) != 1 {
		t.Fatalf("describe: %v %+v", err, d)
	}
	if _, err := c.UpdateStack(ctx, &appstream.UpdateStackInput{Name: aws.String("apps"), Description: aws.String("e")}); err != nil {
		t.Fatal(err)
	}
	if d, _ := c.DescribeStacks(ctx, &appstream.DescribeStacksInput{}); len(d.Stacks) != 1 || aws.ToString(d.Stacks[0].Description) != "e" {
		t.Fatalf("update not kept: %+v", d)
	}
	if _, err := c.DescribeStacks(ctx, &appstream.DescribeStacksInput{Names: []string{"nope"}}); err == nil {
		t.Fatal("unknown stack found")
	}
	if _, err := c.CreateStack(ctx, &appstream.CreateStackInput{}); err == nil {
		t.Fatal("empty create succeeded")
	}
	if _, err := c.DeleteStack(ctx, &appstream.DeleteStackInput{Name: aws.String("apps")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteStack(ctx, &appstream.DeleteStackInput{Name: aws.String("apps")}); err == nil {
		t.Fatal("second delete succeeded")
	}
}
