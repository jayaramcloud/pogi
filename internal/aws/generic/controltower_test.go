package generic_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/controltower"
	"github.com/aws/aws-sdk-go-v2/service/controltower/document"
	"github.com/aws/aws-sdk-go-v2/service/controltower/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestControlTower(t *testing.T) {
	_, base := start(t, "controltower")
	ctx := context.Background()
	c := controltower.NewFromConfig(awstest.Config(t, base))

	if l, err := c.ListLandingZones(ctx, &controltower.ListLandingZonesInput{}); err != nil || len(l.LandingZones) != 0 {
		t.Fatalf("landing zones before create: %v %+v", err, l)
	}
	manifest := map[string]any{"governedRegions": []any{"us-east-1"}, "organizationStructure": map[string]any{"security": map[string]any{"name": "Security"}}}
	lz, err := c.CreateLandingZone(ctx, &controltower.CreateLandingZoneInput{Version: aws.String("3.3"), Manifest: document.NewLazyDocument(manifest), Tags: map[string]string{"env": "dev"}})
	if err != nil || aws.ToString(lz.Arn) == "" || aws.ToString(lz.OperationIdentifier) == "" {
		t.Fatalf("create landing zone: %v %+v", err, lz)
	}
	if _, err := c.CreateLandingZone(ctx, &controltower.CreateLandingZoneInput{Version: aws.String("3.3"), Manifest: document.NewLazyDocument(manifest)}); !errors.As(err, new(*types.ConflictException)) {
		t.Fatalf("second landing zone: %T %v", err, err)
	}
	op, err := c.GetLandingZoneOperation(ctx, &controltower.GetLandingZoneOperationInput{OperationIdentifier: lz.OperationIdentifier})
	if err != nil || op.OperationDetails.Status != types.LandingZoneOperationStatusSucceeded || op.OperationDetails.OperationType != types.LandingZoneOperationTypeCreate {
		t.Fatalf("operation: %v %+v", err, op)
	}
	g, err := c.GetLandingZone(ctx, &controltower.GetLandingZoneInput{LandingZoneIdentifier: lz.Arn})
	if err != nil || g.LandingZone.Status != types.LandingZoneStatusActive || aws.ToString(g.LandingZone.Version) != "3.3" || g.LandingZone.Manifest == nil || g.LandingZone.DriftStatus.Status != types.LandingZoneDriftStatusInSync {
		t.Fatalf("get landing zone: %v %+v", err, g)
	}
	if l, err := c.ListLandingZones(ctx, &controltower.ListLandingZonesInput{}); err != nil || len(l.LandingZones) != 1 || aws.ToString(l.LandingZones[0].Arn) != aws.ToString(lz.Arn) {
		t.Fatalf("list landing zones: %v %+v", err, l)
	}
	up, err := c.UpdateLandingZone(ctx, &controltower.UpdateLandingZoneInput{LandingZoneIdentifier: lz.Arn, Version: aws.String("3.2"), Manifest: document.NewLazyDocument(manifest)})
	if err != nil {
		t.Fatal(err)
	}
	if g, _ = c.GetLandingZone(ctx, &controltower.GetLandingZoneInput{LandingZoneIdentifier: lz.Arn}); aws.ToString(g.LandingZone.Version) != "3.2" {
		t.Fatalf("update: %+v", g.LandingZone)
	}
	if op, _ = c.GetLandingZoneOperation(ctx, &controltower.GetLandingZoneOperationInput{OperationIdentifier: up.OperationIdentifier}); op.OperationDetails.OperationType != types.LandingZoneOperationTypeUpdate {
		t.Fatalf("update operation: %+v", op.OperationDetails)
	}
	if _, err := c.TagResource(ctx, &controltower.TagResourceInput{ResourceArn: lz.Arn, Tags: map[string]string{"team": "plat"}}); err != nil {
		t.Fatal(err)
	}
	tags, err := c.ListTagsForResource(ctx, &controltower.ListTagsForResourceInput{ResourceArn: lz.Arn})
	if err != nil || len(tags.Tags) != 2 {
		t.Fatalf("tags: %v %+v", err, tags)
	}
	if _, err := c.UntagResource(ctx, &controltower.UntagResourceInput{ResourceArn: lz.Arn, TagKeys: []string{"env"}}); err != nil {
		t.Fatal(err)
	}
	if tags, _ = c.ListTagsForResource(ctx, &controltower.ListTagsForResourceInput{ResourceArn: lz.Arn}); len(tags.Tags) != 1 || tags.Tags["team"] != "plat" {
		t.Fatalf("tags after: %+v", tags.Tags)
	}

	// Controls on an OU.
	const ou = "arn:aws:organizations::000000000000:ou/o-abc123/ou-ab12-cdef3456"
	const ctl = "arn:aws:controltower:us-east-1::control/AWS-GR_EC2_VOLUME_INUSE_CHECK"
	ec, err := c.EnableControl(ctx, &controltower.EnableControlInput{ControlIdentifier: aws.String(ctl), TargetIdentifier: aws.String(ou),
		Parameters: []types.EnabledControlParameter{{Key: aws.String("deleteOnTermination"), Value: document.NewLazyDocument(true)}}})
	if err != nil || aws.ToString(ec.Arn) == "" {
		t.Fatalf("enable control: %v %+v", err, ec)
	}
	if _, err := c.EnableControl(ctx, &controltower.EnableControlInput{ControlIdentifier: aws.String(ctl), TargetIdentifier: aws.String(ou)}); !errors.As(err, new(*types.ConflictException)) {
		t.Fatalf("enable twice: %T %v", err, err)
	}
	co, err := c.GetControlOperation(ctx, &controltower.GetControlOperationInput{OperationIdentifier: ec.OperationIdentifier})
	if err != nil || co.ControlOperation.Status != types.ControlOperationStatusSucceeded || co.ControlOperation.OperationType != types.ControlOperationTypeEnableControl {
		t.Fatalf("control operation: %v %+v", err, co)
	}
	gc, err := c.GetEnabledControl(ctx, &controltower.GetEnabledControlInput{EnabledControlIdentifier: ec.Arn})
	if err != nil || aws.ToString(gc.EnabledControlDetails.TargetIdentifier) != ou || gc.EnabledControlDetails.StatusSummary.Status != types.EnablementStatusSucceeded || len(gc.EnabledControlDetails.Parameters) != 1 {
		t.Fatalf("get control: %v %+v", err, gc)
	}
	lc, err := c.ListEnabledControls(ctx, &controltower.ListEnabledControlsInput{TargetIdentifier: aws.String(ou)})
	if err != nil || len(lc.EnabledControls) != 1 {
		t.Fatalf("list controls: %v %+v", err, lc)
	}
	if lc, _ = c.ListEnabledControls(ctx, &controltower.ListEnabledControlsInput{TargetIdentifier: aws.String("arn:aws:organizations::000000000000:ou/o-abc123/ou-other")}); len(lc.EnabledControls) != 0 {
		t.Fatalf("controls of another OU: %+v", lc.EnabledControls)
	}
	if _, err := c.DisableControl(ctx, &controltower.DisableControlInput{EnabledControlIdentifier: ec.Arn}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetEnabledControl(ctx, &controltower.GetEnabledControlInput{EnabledControlIdentifier: ec.Arn}); !errors.As(err, new(*types.ResourceNotFoundException)) {
		t.Fatalf("get disabled control: %T %v", err, err)
	}
	if b, err := c.ListBaselines(ctx, &controltower.ListBaselinesInput{}); err != nil || len(b.Baselines) == 0 {
		t.Fatalf("baselines: %v %+v", err, b)
	}

	if _, err := c.DeleteLandingZone(ctx, &controltower.DeleteLandingZoneInput{LandingZoneIdentifier: lz.Arn}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetLandingZone(ctx, &controltower.GetLandingZoneInput{LandingZoneIdentifier: lz.Arn}); !errors.As(err, new(*types.ResourceNotFoundException)) {
		t.Fatalf("get after delete: %T %v", err, err)
	}
}
