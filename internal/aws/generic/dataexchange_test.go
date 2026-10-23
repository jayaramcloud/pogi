package generic_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dataexchange"
	"github.com/aws/aws-sdk-go-v2/service/dataexchange/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestDataExchange(t *testing.T) {
	_, url := start(t, "dataexchange")
	ctx := context.Background()
	c := dataexchange.NewFromConfig(awstest.Config(t, url))

	o, err := c.CreateDataSet(ctx, &dataexchange.CreateDataSetInput{Name: aws.String("ds"), Description: aws.String("d"), AssetType: types.AssetTypeS3Snapshot, Tags: map[string]string{"env": "dev"}})
	if err != nil || aws.ToString(o.Id) == "" || aws.ToString(o.Arn) == "" {
		t.Fatalf("create: %v %+v", err, o)
	}
	g, err := c.GetDataSet(ctx, &dataexchange.GetDataSetInput{DataSetId: o.Id})
	if err != nil || aws.ToString(g.Name) != "ds" || g.AssetType != types.AssetTypeS3Snapshot || g.Tags["env"] != "dev" {
		t.Fatalf("get: %v %+v", err, g)
	}
	r, err := c.CreateRevision(ctx, &dataexchange.CreateRevisionInput{DataSetId: o.Id, Comment: aws.String("first")})
	if err != nil || aws.ToString(r.Id) == "" {
		t.Fatalf("revision: %v %+v", err, r)
	}
	l, err := c.ListDataSets(ctx, &dataexchange.ListDataSetsInput{Origin: aws.String("OWNED")})
	if err != nil || len(l.DataSets) != 1 {
		t.Fatalf("list: %v %+v", err, l)
	}
	if _, err := c.DeleteDataSet(ctx, &dataexchange.DeleteDataSetInput{DataSetId: o.Id}); err != nil {
		t.Fatal(err)
	}
	var nf *types.ResourceNotFoundException
	if _, err := c.GetDataSet(ctx, &dataexchange.GetDataSetInput{DataSetId: o.Id}); !errors.As(err, &nf) {
		t.Fatalf("not found: %T %v", err, err)
	}
	if _, err := c.DeleteDataSet(ctx, &dataexchange.DeleteDataSetInput{DataSetId: o.Id}); !errors.As(err, &nf) {
		t.Fatalf("second delete: %T %v", err, err)
	}
	if _, err := c.CreateDataSet(ctx, &dataexchange.CreateDataSetInput{}); err == nil {
		t.Fatal("empty create succeeded")
	}
}
