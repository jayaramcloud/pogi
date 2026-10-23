package bigtable_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"cloud.google.com/go/bigtable"
	"github.com/nrndev/pogi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func wantCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if status.Code(err) != want {
		t.Fatalf("got %v, want %v", err, want)
	}
}

func start(t *testing.T) *pogi.Running {
	t.Helper()
	s := pogi.Start(t, pogi.Config{Services: []string{"bigtable", "operations"}})
	t.Setenv("BIGTABLE_EMULATOR_HOST", strings.TrimPrefix(s.URL, "http://"))
	return s
}

func rest(t *testing.T, s *pogi.Running, method, path, body string) (int, map[string]any) {
	t.Helper()
	req := must(http.NewRequest(method, s.URL+"/bigtableadmin/v2/projects/p/"+path, strings.NewReader(body)))
	resp := must(http.DefaultClient.Do(req))
	defer resp.Body.Close()
	var out map[string]any
	json.Unmarshal(must(io.ReadAll(resp.Body)), &out)
	return resp.StatusCode, out
}

func TestInstancesTablesAndData(t *testing.T) {
	s := start(t)
	ia := must(bigtable.NewInstanceAdminClient(ctx, "p"))
	defer ia.Close()
	conf := func(id string) *bigtable.InstanceWithClustersConfig {
		return &bigtable.InstanceWithClustersConfig{InstanceID: id, DisplayName: "Orders " + id, InstanceType: bigtable.PRODUCTION,
			Labels: map[string]string{"env": "test"}, Clusters: []bigtable.ClusterConfig{{ClusterID: "orders-c1", Zone: "us-central1-b", NumNodes: 3, StorageType: bigtable.SSD}}}
	}
	if err := ia.CreateInstanceWithClusters(ctx, conf("orders-inst")); err != nil {
		t.Fatal(err)
	}
	info := must(ia.InstanceInfo(ctx, "orders-inst"))
	if info.DisplayName != "Orders orders-inst" || info.InstanceType != bigtable.PRODUCTION || info.Labels["env"] != "test" || info.InstanceState != bigtable.Ready {
		t.Fatalf("instance: %+v", info)
	}
	cl := must(ia.GetCluster(ctx, "orders-inst", "orders-c1"))
	if cl.ServeNodes != 3 || cl.Zone != "us-central1-b" || cl.StorageType != bigtable.SSD || cl.State != "READY" {
		t.Fatalf("cluster: %+v", cl)
	}
	if err := ia.UpdateCluster(ctx, "orders-inst", "orders-c1", 4); err != nil {
		t.Fatal(err)
	}
	if cl := must(ia.GetCluster(ctx, "orders-inst", "orders-c1")); cl.ServeNodes != 4 {
		t.Fatalf("cluster after update: %+v", cl)
	}
	list := must(ia.Instances(ctx))
	if len(list) != 1 || list[0].Name != "orders-inst" {
		t.Fatalf("instances: %+v", list)
	}
	if _, err := ia.CreateAppProfile(ctx, bigtable.ProfileConf{ProfileID: "batch", InstanceID: "orders-inst", RoutingPolicy: bigtable.MultiClusterRouting}); err != nil {
		t.Fatal(err)
	}
	var profiles []string
	for it := ia.ListAppProfiles(ctx, "orders-inst"); ; {
		p, err := it.Next()
		if err != nil {
			break
		}
		profiles = append(profiles, p.Name)
	}
	if len(profiles) != 2 || !strings.HasSuffix(profiles[0], "/batch") || !strings.HasSuffix(profiles[1], "/default") {
		t.Fatalf("app profiles: %v", profiles)
	}

	// failures
	wantCode(t, ia.CreateInstanceWithClusters(ctx, conf("orders-inst")), codes.AlreadyExists)
	wantCode(t, ia.CreateInstanceWithClusters(ctx, conf("Bad_ID")), codes.InvalidArgument)
	noName := conf("nameless-inst")
	noName.DisplayName = ""
	wantCode(t, ia.CreateInstanceWithClusters(ctx, noName), codes.InvalidArgument)
	noCluster := conf("clusterless-inst")
	noCluster.Clusters = nil
	wantCode(t, ia.CreateInstanceWithClusters(ctx, noCluster), codes.InvalidArgument)
	_, err := ia.InstanceInfo(ctx, "no-such-inst")
	wantCode(t, err, codes.NotFound)
	wantCode(t, ia.DeleteInstance(ctx, "no-such-inst"), codes.NotFound)
	_, err = ia.GetCluster(ctx, "orders-inst", "nope-cluster")
	wantCode(t, err, codes.NotFound)

	// tables: a column family, then rows written and read back
	ad := must(bigtable.NewAdminClient(ctx, "p", "orders-inst"))
	defer ad.Close()
	if err := ad.CreateTableFromConf(ctx, &bigtable.TableConf{TableID: "orders", Families: map[string]bigtable.GCPolicy{"cf": bigtable.MaxVersionsPolicy(2)}}); err != nil {
		t.Fatal(err)
	}
	wantCode(t, ad.CreateTable(ctx, "orders"), codes.AlreadyExists)
	wantCode(t, ad.CreateTable(ctx, "bad table!"), codes.InvalidArgument)
	if tbls := must(ad.Tables(ctx)); len(tbls) != 1 || tbls[0] != "orders" {
		t.Fatalf("tables: %v", tbls)
	}
	ti := must(ad.TableInfo(ctx, "orders"))
	if len(ti.FamilyInfos) != 1 || ti.FamilyInfos[0].Name != "cf" {
		t.Fatalf("table info: %+v", ti)
	}
	lost := must(bigtable.NewAdminClient(ctx, "p", "no-such-inst"))
	defer lost.Close()
	wantCode(t, lost.CreateTable(ctx, "t1"), codes.NotFound) // an instance that does not exist
	_, err = lost.Tables(ctx)
	wantCode(t, err, codes.NotFound)

	dc := must(bigtable.NewClient(ctx, "p", "orders-inst"))
	defer dc.Close()
	tbl := dc.Open("orders")
	for _, id := range []string{"o#1", "o#2", "o#3"} {
		m := bigtable.NewMutation()
		m.Set("cf", "total", bigtable.Now(), []byte("10-"+id))
		if err := tbl.Apply(ctx, id, m); err != nil {
			t.Fatal(err)
		}
	}
	var got []string
	err = tbl.ReadRows(ctx, bigtable.PrefixRange("o#"), func(r bigtable.Row) bool {
		got = append(got, r.Key()+"="+string(r["cf"][0].Value))
		return true
	})
	if err != nil || strings.Join(got, ",") != "o#1=10-o#1,o#2=10-o#2,o#3=10-o#3" {
		t.Fatalf("rows %v: %v", got, err)
	}
	row := must(tbl.ReadRow(ctx, "o#2"))
	if len(row["cf"]) != 1 {
		t.Fatalf("row: %v", row)
	}
	// data for an instance that does not exist, and a family that does not exist
	lt := must(bigtable.NewClient(ctx, "p", "no-such-inst"))
	defer lt.Close()
	m := bigtable.NewMutation()
	m.Set("cf", "x", bigtable.Now(), []byte("v"))
	wantCode(t, lt.Open("orders").Apply(ctx, "k", m), codes.NotFound)
	bad := bigtable.NewMutation()
	bad.Set("nofamily", "x", bigtable.Now(), []byte("v"))
	if err := tbl.Apply(ctx, "k", bad); err == nil {
		t.Fatal("a mutation of a missing column family worked")
	}

	// deleting an instance takes its tables with it; the name can be used again
	if err := ia.DeleteInstance(ctx, "orders-inst"); err != nil {
		t.Fatal(err)
	}
	if err := ia.CreateInstanceWithClusters(ctx, conf("orders-inst")); err != nil {
		t.Fatal(err)
	}
	if tbls := must(ad.Tables(ctx)); len(tbls) != 0 {
		t.Fatalf("tables of the deleted instance came back: %v", tbls)
	}

	// REST, the form /bigtableadmin/v2/ serves
	if code, out := rest(t, s, "GET", "instances/orders-inst", ""); code != 200 || out["state"] != "READY" || out["type"] != "PRODUCTION" {
		t.Fatalf("REST get: %d %v", code, out)
	}
	code, out := rest(t, s, "GET", "instances/no-such-inst", "")
	if e, _ := out["error"].(map[string]any); code != 404 || e["status"] != "NOT_FOUND" {
		t.Fatalf("REST missing: %d %v", code, out)
	}
	if code, _ := rest(t, s, "GET", "instances/orders-inst/clusters", ""); code != 200 {
		t.Fatalf("REST clusters: %d", code)
	}
	body := `{"instanceId":"rest-inst","instance":{"displayName":"r","type":"DEVELOPMENT"},"clusters":{"rest-c1":{"location":"projects/p/locations/us-central1-b"}}}`
	code, out = rest(t, s, "POST", "instances", body)
	if code != 200 || out["done"] != true {
		t.Fatalf("REST create: %d %v", code, out)
	}
	if code, _ := rest(t, s, "GET", strings.TrimPrefix(out["name"].(string), "projects/p/"), ""); code != 200 {
		t.Fatalf("REST poll of %v: %d", out["name"], code)
	}
	if code, _ := rest(t, s, "POST", "instances", body); code != 409 {
		t.Fatalf("REST duplicate: %d", code)
	}
}
