package generic_test

import (
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/dns/armdns"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armresources"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/armtest"
	"github.com/nrndev/pogi/internal/core"
)

var (
	ctx = armtest.Ctx
	sub = core.AzureSubscription
	rg  = core.AzureResourceGroup
)

func TestDNS(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azdns")
	zones := armtest.Must(armdns.NewZonesClient(sub, e.Cred, e.Opts))
	sets := armtest.Must(armdns.NewRecordSetsClient(sub, e.Cred, e.Opts))

	z := armtest.Must(zones.CreateOrUpdate(ctx, rg, "example.test", armdns.Zone{Location: to.Ptr("global"), Tags: map[string]*string{"env": to.Ptr("dev")}}, nil)).Zone
	if *z.Name != "example.test" || *z.Properties.ZoneType != armdns.ZoneTypePublic || len(z.Properties.NameServers) != 4 ||
		!strings.HasPrefix(*z.Properties.NameServers[0], "ns1-") || !strings.HasSuffix(*z.Properties.NameServers[0], ".azure-dns.com.") {
		t.Fatalf("zone: %+v %v", z.Properties, z.Name)
	}
	if *z.Properties.NumberOfRecordSets != 2 { // NS and SOA, as in Azure
		t.Fatalf("a new zone has %d record sets", *z.Properties.NumberOfRecordSets)
	}
	soa := armtest.Must(sets.Get(ctx, rg, "example.test", "@", armdns.RecordTypeSOA, nil)).RecordSet
	if *soa.Properties.Fqdn != "example.test." || *soa.Properties.SoaRecord.Host != *z.Properties.NameServers[0] {
		t.Fatalf("soa: %+v", soa.Properties)
	}

	// record sets of the types a user makes
	put := func(name string, typ armdns.RecordType, p armdns.RecordSetProperties) {
		t.Helper()
		p.TTL = to.Ptr[int64](300)
		armtest.Must(sets.CreateOrUpdate(ctx, rg, "example.test", name, typ, armdns.RecordSet{Properties: &p}, nil))
	}
	put("www", armdns.RecordTypeA, armdns.RecordSetProperties{ARecords: []*armdns.ARecord{{IPv4Address: to.Ptr("10.0.0.4")}}})
	put("v6", armdns.RecordTypeAAAA, armdns.RecordSetProperties{AaaaRecords: []*armdns.AaaaRecord{{IPv6Address: to.Ptr("fd00::4")}}})
	put("app", armdns.RecordTypeCNAME, armdns.RecordSetProperties{CnameRecord: &armdns.CnameRecord{Cname: to.Ptr("www.example.test")}})
	put("@", armdns.RecordTypeMX, armdns.RecordSetProperties{MxRecords: []*armdns.MxRecord{{Exchange: to.Ptr("mail.example.test"), Preference: to.Ptr[int32](10)}}})
	put("@", armdns.RecordTypeTXT, armdns.RecordSetProperties{TxtRecords: []*armdns.TxtRecord{{Value: []*string{to.Ptr("v=spf1 -all")}}}})
	a := armtest.Must(sets.Get(ctx, rg, "example.test", "www", armdns.RecordTypeA, nil)).RecordSet
	if *a.Properties.Fqdn != "www.example.test." || *a.Properties.ARecords[0].IPv4Address != "10.0.0.4" || *a.Properties.TTL != 300 ||
		*a.Type != "Microsoft.Network/dnsZones/A" {
		t.Fatalf("A record set: %+v %s", a.Properties, *a.Type)
	}
	if *armtest.Must(sets.Get(ctx, rg, "example.test", "@", armdns.RecordTypeTXT, nil)).Properties.Fqdn != "example.test." {
		t.Fatal("the apex record set has the zone's name")
	}
	if n := drain(t, sets.NewListByTypePager(rg, "example.test", armdns.RecordTypeA, nil), func(r armdns.RecordSetsClientListByTypeResponse) int { return len(r.Value) }); n != 1 {
		t.Fatalf("A record sets: %d", n)
	}
	if n := drain(t, sets.NewListByDNSZonePager(rg, "example.test", nil), func(r armdns.RecordSetsClientListByDNSZoneResponse) int { return len(r.Value) }); n != 7 {
		t.Fatalf("record sets of the zone: %d, want NS, SOA and the five just made", n)
	}

	// PATCH keeps what it does not name; the zone is listed by group and by subscription
	up := armtest.Must(zones.Update(ctx, rg, "example.test", armdns.ZoneUpdate{Tags: map[string]*string{"env": to.Ptr("prod")}}, nil)).Zone
	if *up.Tags["env"] != "prod" || *up.Location != "global" || *up.Properties.NumberOfRecordSets != 7 {
		t.Fatalf("update: %+v", up)
	}
	armtest.Must(zones.CreateOrUpdate(ctx, rg, "other.test", armdns.Zone{Location: to.Ptr("global")}, nil))
	if n := drain(t, zones.NewListByResourceGroupPager(rg, nil), func(r armdns.ZonesClientListByResourceGroupResponse) int { return len(r.Value) }); n != 2 {
		t.Fatalf("zones of the group: %d", n)
	}
	if n := drain(t, zones.NewListPager(nil), func(r armdns.ZonesClientListResponse) int { return len(r.Value) }); n != 2 {
		t.Fatalf("zones of the subscription: %d", n)
	}

	// delete is a long-running operation and takes the record sets along
	pl := armtest.Must(zones.BeginDelete(ctx, rg, "example.test", nil))
	armtest.Must(pl.PollUntilDone(ctx, armtest.Fast))
	_, err := zones.Get(ctx, rg, "example.test", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = sets.Get(ctx, rg, "example.test", "www", armdns.RecordTypeA, nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	armtest.Must(sets.Delete(ctx, rg, "example.test", "www", armdns.RecordTypeA, nil)) // deleting what is gone is fine

	// the provider lists the spec's API versions for the type
	prov := armtest.Must(armresources.NewProvidersClient(sub, e.Cred, e.Opts)).Get
	var versions []*string
	for _, rt := range armtest.Must(prov(ctx, "Microsoft.Network", nil)).ResourceTypes {
		if strings.EqualFold(*rt.ResourceType, "dnsZones") {
			versions = rt.APIVersions
		}
	}
	if len(versions) != 1 || *versions[0] != "2018-05-01" {
		t.Fatalf("API versions of dnsZones: %v", versions)
	}
}

func TestDNSFailures(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azdns")
	zones := armtest.Must(armdns.NewZonesClient(sub, e.Cred, e.Opts))
	sets := armtest.Must(armdns.NewRecordSetsClient(sub, e.Cred, e.Opts))

	_, err := zones.CreateOrUpdate(ctx, rg, "nolocation.test", armdns.Zone{}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	if _, err := zones.Get(ctx, rg, "nolocation.test", nil); err == nil {
		t.Fatal("a refused zone must not be stored")
	}
	_, err = sets.CreateOrUpdate(ctx, rg, "missing.test", "www", armdns.RecordTypeA, armdns.RecordSet{Properties: &armdns.RecordSetProperties{}}, nil)
	armtest.WantErr(t, err, 404, "ParentResourceNotFound")
	_, err = sets.Get(ctx, rg, "missing.test", "www", armdns.RecordTypeA, nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = zones.Get(ctx, rg, "missing.test", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	armtest.Must(zones.CreateOrUpdate(ctx, rg, "here.test", armdns.Zone{Location: to.Ptr("global")}, nil))
	_, err = sets.Get(ctx, rg, "here.test", "nope", armdns.RecordTypeTXT, nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = zones.Update(ctx, rg, "missing.test", armdns.ZoneUpdate{}, nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}

// Resources survive a save and load, and a reset forgets them.
func TestPersistAndReset(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azdns")
	zones := armtest.Must(armdns.NewZonesClient(sub, e.Cred, e.Opts))
	armtest.Must(zones.CreateOrUpdate(ctx, rg, "keep.test", armdns.Zone{Location: to.Ptr("global")}, nil))
	var svc core.Stateful
	for _, s := range e.Srv.Hub.Services() {
		if s.Info().Name == "azdns" {
			svc = s.(core.Stateful)
		}
	}
	saved := armtest.Must(svc.Save())
	svc.Reset()
	_, err := zones.Get(ctx, rg, "keep.test", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	if err := svc.Load(saved); err != nil {
		t.Fatal(err)
	}
	if z := armtest.Must(zones.Get(ctx, rg, "keep.test", nil)).Zone; *z.Properties.NumberOfRecordSets != 2 {
		t.Fatalf("after load: %+v", z.Properties)
	}
}
