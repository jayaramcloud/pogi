package cosmos_test

import (
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/nrndev/pogi"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// TestMongoDriver connects a MongoDB driver to the account's container with the
// listed connection string, and sees the collection the ARM API made.
func TestMongoDriver(t *testing.T) {
	e := startARM(t, pogi.Config{})
	if !e.srv.Hub.Docker().Available() {
		t.Skip("Docker is not available")
	}
	e.account("mongodrv")
	if err := e.database("mongodrv", "shop", nil); err != nil {
		t.Fatal(err)
	}
	if err := e.collection("mongodrv", "shop", "users"); err != nil {
		t.Fatal(err)
	}
	cs := must(e.accounts.ListConnectionStrings(ctx, rg, "mongodrv", nil))
	if len(cs.ConnectionStrings) != 1 || !strings.HasPrefix(*cs.ConnectionStrings[0].ConnectionString, "mongodb://mongodrv:") {
		t.Fatalf("connection strings: %+v", cs.ConnectionStrings)
	}
	uri := *cs.ConnectionStrings[0].ConnectionString
	keys := must(e.accounts.ListKeys(ctx, rg, "mongodrv", nil))
	if u := must(url.Parse(uri)); must(url.PathUnescape(strings.TrimPrefix(u.User.String(), "mongodrv:"))) == "" || !strings.Contains(uri, url.QueryEscape(*keys.PrimaryMasterKey)) {
		t.Fatalf("the string does not carry the primary key: %s", uri)
	}

	connect := func(uri string) *mongo.Client {
		c, err := mongo.Connect(options.Client().ApplyURI(uri).SetServerSelectionTimeout(5 * time.Second))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Disconnect(ctx) })
		return c
	}
	// The collection and its unique index are in the real MongoDB; the container may still be
	// running the commands, so give it a moment.
	users := connect(uri).Database("shop").Collection("users")
	var names []string
	for end := time.Now().Add(90 * time.Second); time.Now().Before(end); time.Sleep(time.Second) {
		names, _ = users.Database().ListCollectionNames(ctx, bson.D{})
		if len(names) > 0 {
			break
		}
	}
	if len(names) != 1 || names[0] != "users" {
		t.Fatalf("collections: %v", names)
	}
	if _, err := users.InsertOne(ctx, bson.M{"email": "a@example.com", "city": "Oslo"}); err != nil {
		t.Fatal(err)
	}
	if _, err := users.InsertOne(ctx, bson.M{"email": "a@example.com"}); !mongo.IsDuplicateKeyError(err) {
		t.Fatalf("the unique index: %v", err)
	}
	var found []bson.M
	cur, err := users.Find(ctx, bson.M{"city": "Oslo"})
	if err != nil || cur.All(ctx, &found) != nil || len(found) != 1 || found[0]["email"] != "a@example.com" {
		t.Fatalf("find: %v %v", found, err)
	}

	// a wrong key: the driver's authentication fails
	wrong := strings.Replace(uri, url.QueryEscape(*keys.PrimaryMasterKey), "wrong", 1)
	if _, err := connect(wrong).Database("shop").Collection("users").CountDocuments(ctx, bson.M{}); err == nil || !strings.Contains(err.Error(), "uth") {
		t.Fatalf("a wrong key: %v", err)
	}
}
