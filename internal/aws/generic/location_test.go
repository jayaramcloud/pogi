package generic_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/location"
	"github.com/aws/aws-sdk-go-v2/service/location/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestLocation(t *testing.T) {
	_, url := start(t, "location")
	ctx := context.Background()
	// The SDK puts a host prefix in front of the endpoint (cp.maps.<host>); *.localhost resolves, *.127.0.0.1 does not.
	c := location.NewFromConfig(awstest.Config(t, strings.Replace(url, "127.0.0.1", "localhost", 1)))
	const arnp = "arn:aws:geo:us-east-1:000000000000:"

	m, err := c.CreateMap(ctx, &location.CreateMapInput{MapName: aws.String("city"), Configuration: &types.MapConfiguration{Style: aws.String("VectorEsriNavigation")},
		Tags: map[string]string{"env": "dev"}})
	if err != nil || aws.ToString(m.MapArn) != arnp+"map/city" || m.CreateTime == nil {
		t.Fatalf("map: %v %+v", err, m)
	}
	_, err = c.CreateMap(ctx, &location.CreateMapInput{MapName: aws.String("city"), Configuration: &types.MapConfiguration{Style: aws.String("VectorEsriNavigation")}})
	var cf *types.ConflictException
	if !errors.As(err, &cf) {
		t.Fatalf("duplicate map: %T %v", err, err)
	}
	d, err := c.DescribeMap(ctx, &location.DescribeMapInput{MapName: aws.String("city")})
	if err != nil || aws.ToString(d.Configuration.Style) != "VectorEsriNavigation" || d.Tags["env"] != "dev" || d.UpdateTime == nil {
		t.Fatalf("describe map: %v %+v", err, d)
	}
	ml, err := c.ListMaps(ctx, &location.ListMapsInput{})
	if err != nil || len(ml.Entries) != 1 || aws.ToString(ml.Entries[0].MapName) != "city" {
		t.Fatalf("list maps: %v %+v", err, ml)
	}

	pi, err := c.CreatePlaceIndex(ctx, &location.CreatePlaceIndexInput{IndexName: aws.String("places"), DataSource: aws.String("Esri")})
	if err != nil || aws.ToString(pi.IndexArn) != arnp+"place-index/places" {
		t.Fatalf("place index: %v %+v", err, pi)
	}
	rc, err := c.CreateRouteCalculator(ctx, &location.CreateRouteCalculatorInput{CalculatorName: aws.String("routes"), DataSource: aws.String("Esri")})
	if err != nil || aws.ToString(rc.CalculatorArn) != arnp+"route-calculator/routes" {
		t.Fatalf("route calculator: %v %+v", err, rc)
	}
	gc, err := c.CreateGeofenceCollection(ctx, &location.CreateGeofenceCollectionInput{CollectionName: aws.String("fences")})
	if err != nil || aws.ToString(gc.CollectionArn) != arnp+"geofence-collection/fences" {
		t.Fatalf("geofence collection: %v %+v", err, gc)
	}
	tr, err := c.CreateTracker(ctx, &location.CreateTrackerInput{TrackerName: aws.String("cars")})
	if err != nil || aws.ToString(tr.TrackerArn) != arnp+"tracker/cars" {
		t.Fatalf("tracker: %v %+v", err, tr)
	}
	if _, err := c.AssociateTrackerConsumer(ctx, &location.AssociateTrackerConsumerInput{TrackerName: aws.String("cars"), ConsumerArn: gc.CollectionArn}); err != nil {
		t.Fatal(err)
	}
	cons, err := c.ListTrackerConsumers(ctx, &location.ListTrackerConsumersInput{TrackerName: aws.String("cars")})
	if err != nil || len(cons.ConsumerArns) != 1 {
		t.Fatalf("consumers: %v %+v", err, cons)
	}
	tg, err := c.ListTagsForResource(ctx, &location.ListTagsForResourceInput{ResourceArn: m.MapArn})
	if err != nil || tg.Tags["env"] != "dev" {
		t.Fatalf("tags: %v %+v", err, tg)
	}

	if _, err := c.DeleteMap(ctx, &location.DeleteMapInput{MapName: aws.String("city")}); err != nil {
		t.Fatal(err)
	}
	var nf *types.ResourceNotFoundException
	if _, err := c.DescribeMap(ctx, &location.DescribeMapInput{MapName: aws.String("city")}); !errors.As(err, &nf) {
		t.Fatalf("not found: %T %v", err, err)
	}
	if _, err := c.DeleteMap(ctx, &location.DeleteMapInput{MapName: aws.String("city")}); !errors.As(err, &nf) {
		t.Fatalf("second delete: %T %v", err, err)
	}
	if _, err := c.CreateTracker(ctx, &location.CreateTrackerInput{}); err == nil {
		t.Fatal("empty create succeeded")
	}
	if _, err := c.CreateTracker(ctx, &location.CreateTrackerInput{TrackerName: aws.String("bad name!")}); err == nil {
		t.Fatal("bad name accepted")
	}
}
