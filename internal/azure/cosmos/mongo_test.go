package cosmos_test

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	azarm "github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/cosmos/armcosmos/v3"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/core"
)

const (
	sub = core.AzureSubscription
	rg  = core.AzureResourceGroup
)

var fast = &runtime.PollUntilDoneOptions{Frequency: 100 * time.Millisecond}

type mongoEnv struct {
	srv      *pogi.Running
	accounts *armcosmos.DatabaseAccountsClient
	mongo    *armcosmos.MongoDBResourcesClient
}

// startARM runs pogi with the Resource Manager and Cosmos DB, and returns the SDK clients.
func startARM(t *testing.T, cfg pogi.Config) *mongoEnv {
	t.Helper()
	cfg.Services = []string{"arm", "entra", "graph", "cosmos"}
	srv := pogi.Start(t, cfg)
	base := "https://localhost:" + srv.URL[strings.LastIndexByte(srv.URL, ':')+1:]
	ca, _, _ := srv.Hub.CertPaths()
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(must(os.ReadFile(ca)))
	hc := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: "localhost"}}}
	co := azcore.ClientOptions{Transport: hc, Cloud: cloud.Configuration{ActiveDirectoryAuthorityHost: base + "/",
		Services: map[cloud.ServiceName]cloud.ServiceConfiguration{cloud.ResourceManager: {Endpoint: base, Audience: base}}}}
	cred := must(azidentity.NewClientSecretCredential(arm.TenantID, "app", "secret",
		&azidentity.ClientSecretCredentialOptions{ClientOptions: co, DisableInstanceDiscovery: true}))
	opts := &azarm.ClientOptions{ClientOptions: co}
	return &mongoEnv{srv, must(armcosmos.NewDatabaseAccountsClient(sub, cred, opts)), must(armcosmos.NewMongoDBResourcesClient(sub, cred, opts))}
}

func wantErr(t *testing.T, err error, status int, code string) {
	t.Helper()
	var re *azcore.ResponseError
	if !errors.As(err, &re) || re.StatusCode != status || re.ErrorCode != code {
		t.Fatalf("want %d %s, got %v", status, code, err)
	}
}

func (e *mongoEnv) account(name string) {
	must(must(e.accounts.BeginCreateOrUpdate(ctx, rg, name, armcosmos.DatabaseAccountCreateUpdateParameters{
		Location: to.Ptr("eastus"), Kind: to.Ptr(armcosmos.DatabaseAccountKindMongoDB),
		Properties: &armcosmos.DatabaseAccountCreateUpdateProperties{
			DatabaseAccountOfferType: to.Ptr("Standard"),
			Locations:                []*armcosmos.Location{{LocationName: to.Ptr("eastus"), FailoverPriority: to.Ptr(int32(0))}},
			APIProperties:            &armcosmos.APIProperties{ServerVersion: to.Ptr(armcosmos.ServerVersionFour2)},
			Capabilities:             []*armcosmos.Capability{{Name: to.Ptr("EnableMongo")}},
		}}, nil)).PollUntilDone(ctx, fast))
}

func (e *mongoEnv) database(account, name string, throughput *int32) error {
	p, err := e.mongo.BeginCreateUpdateMongoDBDatabase(ctx, rg, account, name, armcosmos.MongoDBDatabaseCreateUpdateParameters{
		Properties: &armcosmos.MongoDBDatabaseCreateUpdateProperties{Resource: &armcosmos.MongoDBDatabaseResource{ID: &name},
			Options: &armcosmos.CreateUpdateOptions{Throughput: throughput}}}, nil)
	if err == nil {
		_, err = p.PollUntilDone(ctx, fast)
	}
	return err
}

func (e *mongoEnv) collection(account, db, name string) error {
	p, err := e.mongo.BeginCreateUpdateMongoDBCollection(ctx, rg, account, db, name, armcosmos.MongoDBCollectionCreateUpdateParameters{
		Properties: &armcosmos.MongoDBCollectionCreateUpdateProperties{
			Resource: &armcosmos.MongoDBCollectionResource{ID: &name, ShardKey: map[string]*string{"city": to.Ptr("Hash")},
				Indexes: []*armcosmos.MongoIndex{
					{Key: &armcosmos.MongoIndexKeys{Keys: []*string{to.Ptr("_id")}}},
					{Key: &armcosmos.MongoIndexKeys{Keys: []*string{to.Ptr("email")}}, Options: &armcosmos.MongoIndexOptions{Unique: to.Ptr(true)}}}}}}, nil)
	if err == nil {
		_, err = p.PollUntilDone(ctx, fast)
	}
	return err
}

// TestMongoMetadata covers the ARM side without Docker: databases and
// collections are stored and read back, and the connection string says Docker is needed.
func TestMongoMetadata(t *testing.T) {
	e := startARM(t, pogi.Config{DockerHost: "/nonexistent/docker.sock"})
	e.account("mongoacct")

	got := must(e.accounts.Get(ctx, rg, "mongoacct", nil))
	if *got.Kind != armcosmos.DatabaseAccountKindMongoDB || *got.Properties.APIProperties.ServerVersion != armcosmos.ServerVersionFour2 ||
		len(got.Properties.Capabilities) != 1 || *got.Properties.Capabilities[0].Name != "EnableMongo" {
		t.Fatalf("account: %+v", got.Properties)
	}
	if err := e.database("mongoacct", "shop", to.Ptr(int32(400))); err != nil {
		t.Fatal(err)
	}
	if err := e.collection("mongoacct", "shop", "users"); err != nil {
		t.Fatal(err)
	}
	c := must(e.mongo.GetMongoDBCollection(ctx, rg, "mongoacct", "shop", "users", nil))
	r := c.Properties.Resource
	if *r.ID != "users" || *r.ShardKey["city"] != "Hash" || len(r.Indexes) != 2 || !*r.Indexes[1].Options.Unique {
		t.Fatalf("collection: %+v", r)
	}
	if th := must(e.mongo.GetMongoDBDatabaseThroughput(ctx, rg, "mongoacct", "shop", nil)); *th.Properties.Resource.Throughput != 400 {
		t.Fatalf("throughput: %+v", th.Properties.Resource)
	}
	_, err := e.mongo.GetMongoDBCollectionThroughput(ctx, rg, "mongoacct", "shop", "users", nil)
	wantErr(t, err, 404, "NotFound")
	must(must(e.mongo.BeginUpdateMongoDBCollectionThroughput(ctx, rg, "mongoacct", "shop", "users", armcosmos.ThroughputSettingsUpdateParameters{
		Properties: &armcosmos.ThroughputSettingsUpdateProperties{Resource: &armcosmos.ThroughputSettingsResource{Throughput: to.Ptr(int32(800))}}}, nil)).PollUntilDone(ctx, fast))
	if th := must(e.mongo.GetMongoDBCollectionThroughput(ctx, rg, "mongoacct", "shop", "users", nil)); *th.Properties.Resource.Throughput != 800 {
		t.Fatalf("collection throughput: %+v", th.Properties.Resource)
	}

	var dbs, colls int
	for pg := e.mongo.NewListMongoDBDatabasesPager(rg, "mongoacct", nil); pg.More(); {
		dbs += len(must(pg.NextPage(ctx)).Value)
	}
	for pg := e.mongo.NewListMongoDBCollectionsPager(rg, "mongoacct", "shop", nil); pg.More(); {
		colls += len(must(pg.NextPage(ctx)).Value)
	}
	if dbs != 1 || colls != 1 {
		t.Fatalf("lists: %d databases, %d collections", dbs, colls)
	}

	// failures
	wantErr(t, e.database("nosuch", "shop", nil), 404, "ResourceNotFound")
	wantErr(t, e.collection("mongoacct", "nosuch", "users"), 404, "ResourceNotFound")
	wantErr(t, e.database("mongoacct", "bad.name", nil), 400, "BadRequest")
	wantErr(t, e.collection("mongoacct", "shop", "bad$name"), 400, "BadRequest")
	_, err = e.mongo.GetMongoDBDatabase(ctx, rg, "mongoacct", "nosuch", nil)
	wantErr(t, err, 404, "ResourceNotFound")
	_, err = e.accounts.ListConnectionStrings(ctx, rg, "nosuch", nil)
	wantErr(t, err, 404, "ResourceNotFound")
	_, err = e.accounts.ListConnectionStrings(ctx, rg, "mongoacct", nil)
	wantErr(t, err, 400, "BadRequest")
	if !strings.Contains(err.Error(), "Docker is needed") {
		t.Fatalf("error: %v", err)
	}
	if keys := must(e.accounts.ListKeys(ctx, rg, "mongoacct", nil)); *keys.PrimaryMasterKey == "" {
		t.Fatal("no primary key")
	}

	must(must(e.mongo.BeginDeleteMongoDBCollection(ctx, rg, "mongoacct", "shop", "users", nil)).PollUntilDone(ctx, fast))
	_, err = e.mongo.GetMongoDBCollection(ctx, rg, "mongoacct", "shop", "users", nil)
	wantErr(t, err, 404, "ResourceNotFound")
}

// TestMongoContainer checks the account's real MongoDB with mongosh in the container: the
// connection string carries the primary key, the collection and its unique index are there,
// and a wrong key fails to log in. (mongo_driver_test.go does the same with the Go driver.)
func TestMongoContainer(t *testing.T) {
	e := startARM(t, pogi.Config{})
	d := e.srv.Hub.Docker()
	if !d.Available() {
		t.Skip("Docker is not available")
	}
	e.account("mongoctr")
	if err := e.database("mongoctr", "shop", nil); err != nil {
		t.Fatal(err)
	}
	if err := e.collection("mongoctr", "shop", "users"); err != nil {
		t.Fatal(err)
	}
	cs := must(e.accounts.ListConnectionStrings(ctx, rg, "mongoctr", nil))
	keys := must(e.accounts.ListKeys(ctx, rg, "mongoctr", nil))
	if len(cs.ConnectionStrings) != 1 {
		t.Fatalf("connection strings: %+v", cs.ConnectionStrings)
	}
	if uri := *cs.ConnectionStrings[0].ConnectionString; !strings.HasPrefix(uri, "mongodb://mongoctr:"+url.QueryEscape(*keys.PrimaryMasterKey)+"@localhost:") {
		t.Fatalf("connection string: %s", uri)
	}
	sh := func(key, js string) (string, int) {
		out, code, err := d.Exec(ctx, "pogi-cosmos-mongoctr", "mongosh", "--quiet", "-u", "mongoctr", "-p", key, "--authenticationDatabase", "admin", "--eval", js)
		if err != nil {
			t.Fatal(err)
		}
		return string(out), code
	}
	// the collection is made by commands queued behind the server's start
	var out string
	for end := time.Now().Add(90 * time.Second); time.Now().Before(end); time.Sleep(time.Second) {
		if out, _ = sh(*keys.PrimaryMasterKey, `print(db.getSiblingDB("shop").getCollection("users").getIndexes().map(i=>i.name))`); strings.Contains(out, "email") {
			break
		}
	}
	if !strings.Contains(out, "email") {
		t.Fatalf("indexes of users: %q", out)
	}
	js := `const c=db.getSiblingDB("shop").users; c.insertOne({email:"a@example.com"}); let dup="none"; try{c.insertOne({email:"a@example.com"})}catch(e){dup=e.code}; print(c.countDocuments({})+" "+dup)`
	if out, code := sh(*keys.PrimaryMasterKey, js); code != 0 || strings.TrimSpace(out) != "1 11000" {
		t.Fatalf("insert: %d %q", code, out)
	}
	if out, code := sh("wrong", `db.getSiblingDB("shop").users.countDocuments({})`); code == 0 || !strings.Contains(out, "uthentication") {
		t.Fatalf("a wrong key: %d %q", code, out)
	}
}
