package generic_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/quicksight"
	"github.com/aws/aws-sdk-go-v2/service/quicksight/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestQuickSight(t *testing.T) {
	_, url := start(t, "quicksight")
	ctx := context.Background()
	c := quicksight.NewFromConfig(awstest.Config(t, url))
	acct, ns := aws.String("000000000000"), aws.String("default")

	// a group
	g, err := c.CreateGroup(ctx, &quicksight.CreateGroupInput{AwsAccountId: acct, Namespace: ns, GroupName: aws.String("analysts"), Description: aws.String("read only")})
	if err != nil || aws.ToString(g.Group.GroupName) != "analysts" || aws.ToString(g.Group.Arn) == "" {
		t.Fatalf("group: %v %+v", err, g)
	}
	if _, err := c.CreateGroup(ctx, &quicksight.CreateGroupInput{AwsAccountId: acct, Namespace: ns, GroupName: aws.String("analysts")}); err == nil {
		t.Fatal("duplicate group succeeded")
	}
	dg, err := c.DescribeGroup(ctx, &quicksight.DescribeGroupInput{AwsAccountId: acct, Namespace: ns, GroupName: aws.String("analysts")})
	if err != nil || aws.ToString(dg.Group.Description) != "read only" {
		t.Fatalf("describe group: %v %+v", err, dg)
	}

	// an S3 data source reaches CREATION_SUCCESSFUL at once
	dsIn := &quicksight.CreateDataSourceInput{AwsAccountId: acct, DataSourceId: aws.String("sales"), Name: aws.String("sales"), Type: types.DataSourceTypeS3,
		DataSourceParameters: &types.DataSourceParametersMemberS3Parameters{Value: types.S3Parameters{
			ManifestFileLocation: &types.ManifestFileLocation{Bucket: aws.String("data"), Key: aws.String("manifest.json")}}}}
	ds, err := c.CreateDataSource(ctx, dsIn)
	if err != nil || ds.CreationStatus != types.ResourceStatusCreationSuccessful || aws.ToString(ds.Arn) == "" {
		t.Fatalf("data source: %v %+v", err, ds)
	}
	if _, err := c.CreateDataSource(ctx, dsIn); err == nil {
		t.Fatal("duplicate data source succeeded")
	}
	bad := *dsIn
	bad.DataSourceId, bad.Type = aws.String("other"), "BOGUS"
	if _, err := c.CreateDataSource(ctx, &bad); err == nil {
		t.Fatal("bad data source type succeeded")
	}
	d, err := c.DescribeDataSource(ctx, &quicksight.DescribeDataSourceInput{AwsAccountId: acct, DataSourceId: aws.String("sales")})
	if err != nil || d.DataSource.Status != types.ResourceStatusCreationSuccessful || aws.ToString(d.DataSource.Name) != "sales" {
		t.Fatalf("describe: %v %+v", err, d)
	}
	if _, err := c.UpdateDataSource(ctx, &quicksight.UpdateDataSourceInput{AwsAccountId: acct, DataSourceId: aws.String("sales"), Name: aws.String("sales2")}); err != nil {
		t.Fatal(err)
	}
	if d, _ = c.DescribeDataSource(ctx, &quicksight.DescribeDataSourceInput{AwsAccountId: acct, DataSourceId: aws.String("sales")}); aws.ToString(d.DataSource.Name) != "sales2" {
		t.Fatalf("update not kept: %+v", d.DataSource)
	}
	l, err := c.ListDataSources(ctx, &quicksight.ListDataSourcesInput{AwsAccountId: acct})
	if err != nil || len(l.DataSources) != 1 {
		t.Fatalf("list: %v %+v", err, l)
	}

	// a user with an IAM identity
	reg := &quicksight.RegisterUserInput{AwsAccountId: acct, Namespace: ns, IdentityType: types.IdentityTypeIam, UserRole: types.UserRoleReader,
		Email: aws.String("a@example.com"), IamArn: aws.String("arn:aws:iam::000000000000:user/a"), SessionName: aws.String("s")}
	if _, err := c.RegisterUser(ctx, reg); err == nil { // a session name needs two characters
		t.Fatal("short session name succeeded")
	}
	reg.SessionName = aws.String("sess")
	u, err := c.RegisterUser(ctx, reg)
	if err != nil || u.User == nil {
		t.Fatalf("register user: %v %+v", err, u)
	}

	// deletes, then not found
	if _, err := c.DeleteDataSource(ctx, &quicksight.DeleteDataSourceInput{AwsAccountId: acct, DataSourceId: aws.String("sales")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DescribeDataSource(ctx, &quicksight.DescribeDataSourceInput{AwsAccountId: acct, DataSourceId: aws.String("sales")}); err == nil {
		t.Fatal("deleted data source described")
	}
	if _, err := c.DeleteGroup(ctx, &quicksight.DeleteGroupInput{AwsAccountId: acct, Namespace: ns, GroupName: aws.String("analysts")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteGroup(ctx, &quicksight.DeleteGroupInput{AwsAccountId: acct, Namespace: ns, GroupName: aws.String("analysts")}); err == nil {
		t.Fatal("second group delete succeeded")
	}
}
