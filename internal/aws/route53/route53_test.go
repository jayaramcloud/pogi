package route53_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/route53"
	"github.com/aws/aws-sdk-go-v2/service/route53/types"
	r53svc "github.com/nrndev/pogi/internal/aws/route53"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

var ctx = context.Background()

// roundTrip snapshots, resets and restores the hub: state must survive Save and Load.
func roundTrip(t *testing.T, h *core.Hub) {
	t.Helper()
	b, err := h.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	h.Reset()
	if err := h.Restore(b); err != nil {
		t.Fatal(err)
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func change(action types.ChangeAction, rs types.ResourceRecordSet) types.Change {
	return types.Change{Action: action, ResourceRecordSet: &rs}
}

func TestHostedZonesAndRecords(t *testing.T) {
	hub, url := core.ServeTest(t, func(h *core.Hub) core.Service { return r53svc.New(h) })
	c := route53.NewFromConfig(awstest.Config(t, url))

	created := must(c.CreateHostedZone(ctx, &route53.CreateHostedZoneInput{Name: aws.String("Example.com"), CallerReference: aws.String("ref-1"),
		HostedZoneConfig: &types.HostedZoneConfig{Comment: aws.String("test zone")}}))
	zone := created.HostedZone
	if !strings.HasPrefix(*zone.Id, "/hostedzone/Z") || *zone.Name != "example.com." || *zone.ResourceRecordSetCount != 2 ||
		len(created.DelegationSet.NameServers) != 4 || created.ChangeInfo.Status != types.ChangeStatusInsync || *created.Location == "" {
		t.Fatalf("created = %+v %+v %+v", zone, created.DelegationSet, created.ChangeInfo)
	}
	_, err := c.CreateHostedZone(ctx, &route53.CreateHostedZoneInput{Name: aws.String("other.com"), CallerReference: aws.String("ref-1")})
	var dup *types.HostedZoneAlreadyExists
	if !errors.As(err, &dup) {
		t.Fatalf("want HostedZoneAlreadyExists, got %v", err)
	}

	a := types.ResourceRecordSet{Name: aws.String("www.example.com"), Type: types.RRTypeA, TTL: aws.Int64(60),
		ResourceRecords: []types.ResourceRecord{{Value: aws.String("10.0.0.1")}}}
	weighted := types.ResourceRecordSet{Name: aws.String("api.example.com."), Type: types.RRTypeCname, TTL: aws.Int64(60), SetIdentifier: aws.String("blue"),
		Weight: aws.Int64(10), ResourceRecords: []types.ResourceRecord{{Value: aws.String("blue.example.net")}}}
	ch := must(c.ChangeResourceRecordSets(ctx, &route53.ChangeResourceRecordSetsInput{HostedZoneId: zone.Id,
		ChangeBatch: &types.ChangeBatch{Changes: []types.Change{change(types.ChangeActionCreate, a), change(types.ChangeActionUpsert, weighted)}}}))
	if got := must(c.GetChange(ctx, &route53.GetChangeInput{Id: ch.ChangeInfo.Id})); got.ChangeInfo.Status != types.ChangeStatusInsync {
		t.Fatalf("change = %+v", got.ChangeInfo)
	}
	_, err = c.ChangeResourceRecordSets(ctx, &route53.ChangeResourceRecordSetsInput{HostedZoneId: zone.Id,
		ChangeBatch: &types.ChangeBatch{Changes: []types.Change{change(types.ChangeActionCreate, a)}}})
	var bad *types.InvalidChangeBatch
	if !errors.As(err, &bad) || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("want InvalidChangeBatch, got %v", err)
	}

	roundTrip(t, hub)
	sets := must(c.ListResourceRecordSets(ctx, &route53.ListResourceRecordSetsInput{HostedZoneId: zone.Id})).ResourceRecordSets
	var order []string
	for _, rs := range sets {
		order = append(order, *rs.Name+" "+string(rs.Type))
	}
	if strings.Join(order, ",") != "example.com. NS,example.com. SOA,api.example.com. CNAME,www.example.com. A" || *sets[2].Weight != 10 || *sets[2].SetIdentifier != "blue" {
		t.Fatalf("record sets = %v (weight %v)", order, sets[2].Weight)
	}
	page := must(c.ListResourceRecordSets(ctx, &route53.ListResourceRecordSetsInput{HostedZoneId: zone.Id,
		StartRecordName: aws.String("api.example.com"), MaxItems: aws.Int32(1)}))
	if len(page.ResourceRecordSets) != 1 || !page.IsTruncated || *page.NextRecordName != "www.example.com." {
		t.Fatalf("page = %+v", page)
	}

	byName := must(c.ListHostedZonesByName(ctx, &route53.ListHostedZonesByNameInput{DNSName: aws.String("example.com")}))
	if len(byName.HostedZones) != 1 || *byName.HostedZones[0].Id != *zone.Id {
		t.Fatalf("by name = %+v", byName.HostedZones)
	}
	if n := must(c.GetHostedZoneCount(ctx, &route53.GetHostedZoneCountInput{})); *n.HostedZoneCount != 1 {
		t.Fatalf("count = %d", *n.HostedZoneCount)
	}

	id := strings.TrimPrefix(*zone.Id, "/hostedzone/")
	must(c.ChangeTagsForResource(ctx, &route53.ChangeTagsForResourceInput{ResourceType: types.TagResourceTypeHostedzone, ResourceId: aws.String(id),
		AddTags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}}))
	if tags := must(c.ListTagsForResource(ctx, &route53.ListTagsForResourceInput{ResourceType: types.TagResourceTypeHostedzone, ResourceId: aws.String(id)})); len(tags.ResourceTagSet.Tags) != 1 {
		t.Fatalf("tags = %+v", tags.ResourceTagSet)
	}

	_, err = c.DeleteHostedZone(ctx, &route53.DeleteHostedZoneInput{Id: zone.Id})
	var notEmpty *types.HostedZoneNotEmpty
	if !errors.As(err, &notEmpty) {
		t.Fatalf("want HostedZoneNotEmpty, got %v", err)
	}
	must(c.ChangeResourceRecordSets(ctx, &route53.ChangeResourceRecordSetsInput{HostedZoneId: zone.Id,
		ChangeBatch: &types.ChangeBatch{Changes: []types.Change{change(types.ChangeActionDelete, a), change(types.ChangeActionDelete, weighted)}}}))
	must(c.DeleteHostedZone(ctx, &route53.DeleteHostedZoneInput{Id: zone.Id}))
	_, err = c.GetHostedZone(ctx, &route53.GetHostedZoneInput{Id: zone.Id})
	var noZone *types.NoSuchHostedZone
	if !errors.As(err, &noZone) {
		t.Fatalf("want NoSuchHostedZone, got %v", err)
	}

	hc := must(c.CreateHealthCheck(ctx, &route53.CreateHealthCheckInput{CallerReference: aws.String("hc-1"),
		HealthCheckConfig: &types.HealthCheckConfig{Type: types.HealthCheckTypeHttps, FullyQualifiedDomainName: aws.String("example.com"), Port: aws.Int32(443)}})).HealthCheck
	got := must(c.GetHealthCheck(ctx, &route53.GetHealthCheckInput{HealthCheckId: hc.Id})).HealthCheck
	if got.HealthCheckConfig.Type != types.HealthCheckTypeHttps || *got.HealthCheckConfig.Port != 443 {
		t.Fatalf("health check = %+v", got.HealthCheckConfig)
	}
	if l := must(c.ListHealthChecks(ctx, &route53.ListHealthChecksInput{})); len(l.HealthChecks) != 1 {
		t.Fatalf("health checks = %+v", l.HealthChecks)
	}
	must(c.DeleteHealthCheck(ctx, &route53.DeleteHealthCheckInput{HealthCheckId: hc.Id}))
	_, err = c.GetHealthCheck(ctx, &route53.GetHealthCheckInput{HealthCheckId: hc.Id})
	var noCheck *types.NoSuchHealthCheck
	if !errors.As(err, &noCheck) {
		t.Fatalf("want NoSuchHealthCheck, got %v", err)
	}
}

func TestAssociateVPC(t *testing.T) {
	hub, url := core.ServeTest(t, func(h *core.Hub) core.Service { return r53svc.New(h) })
	_ = hub
	c := route53.NewFromConfig(awstest.Config(t, url))
	v := func(id string) *types.VPC {
		return &types.VPC{VPCId: aws.String(id), VPCRegion: types.VPCRegionUsEast1}
	}
	priv := must(c.CreateHostedZone(ctx, &route53.CreateHostedZoneInput{Name: aws.String("p.test"), CallerReference: aws.String("p"), VPC: v("vpc-a")})).HostedZone
	pub := must(c.CreateHostedZone(ctx, &route53.CreateHostedZoneInput{Name: aws.String("pub.test"), CallerReference: aws.String("q")})).HostedZone

	out := must(c.AssociateVPCWithHostedZone(ctx, &route53.AssociateVPCWithHostedZoneInput{HostedZoneId: priv.Id, VPC: v("vpc-b")}))
	if out.ChangeInfo == nil || out.ChangeInfo.Status != types.ChangeStatusInsync {
		t.Fatalf("change info: %+v", out.ChangeInfo)
	}
	if l := must(c.ListHostedZonesByVPC(ctx, &route53.ListHostedZonesByVPCInput{VPCId: aws.String("vpc-b"), VPCRegion: types.VPCRegionUsEast1})); len(l.HostedZoneSummaries) != 1 || *l.HostedZoneSummaries[0].Name != "p.test." {
		t.Fatalf("by VPC: %+v", l.HostedZoneSummaries)
	}
	if z := must(c.GetHostedZone(ctx, &route53.GetHostedZoneInput{Id: priv.Id})); len(z.VPCs) != 2 {
		t.Fatalf("VPCs: %+v", z.VPCs)
	}
	var dup *types.ConflictingDomainExists
	if _, err := c.AssociateVPCWithHostedZone(ctx, &route53.AssociateVPCWithHostedZoneInput{HostedZoneId: priv.Id, VPC: v("vpc-b")}); !errors.As(err, &dup) {
		t.Fatalf("duplicate: %v", err)
	}
	var notPrivate *types.PublicZoneVPCAssociation
	if _, err := c.AssociateVPCWithHostedZone(ctx, &route53.AssociateVPCWithHostedZoneInput{HostedZoneId: pub.Id, VPC: v("vpc-b")}); !errors.As(err, &notPrivate) {
		t.Fatalf("public zone: %v", err)
	}
	var none *types.NoSuchHostedZone
	if _, err := c.AssociateVPCWithHostedZone(ctx, &route53.AssociateVPCWithHostedZoneInput{HostedZoneId: aws.String("ZNOPE"), VPC: v("vpc-b")}); !errors.As(err, &none) {
		t.Fatalf("unknown zone: %v", err)
	}
	var absent *types.VPCAssociationNotFound
	if _, err := c.DisassociateVPCFromHostedZone(ctx, &route53.DisassociateVPCFromHostedZoneInput{HostedZoneId: priv.Id, VPC: v("vpc-z")}); !errors.As(err, &absent) {
		t.Fatalf("not associated: %v", err)
	}
	must(c.DisassociateVPCFromHostedZone(ctx, &route53.DisassociateVPCFromHostedZoneInput{HostedZoneId: priv.Id, VPC: v("vpc-a")}))
	var last *types.LastVPCAssociation
	if _, err := c.DisassociateVPCFromHostedZone(ctx, &route53.DisassociateVPCFromHostedZoneInput{HostedZoneId: priv.Id, VPC: v("vpc-b")}); !errors.As(err, &last) {
		t.Fatalf("last VPC: %v", err)
	}
}
