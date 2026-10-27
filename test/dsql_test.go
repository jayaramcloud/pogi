package test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/dsql"
	"github.com/jackc/pgx/v5"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
)

// TestDSQLSQL: a DSQL cluster is a real PostgreSQL on pogi's main port. The client logs in with an auth token,
// runs SQL, and the container goes with the cluster. HTTP shares the port. It skips without Docker.
func TestDSQLSQL(t *testing.T) {
	if testing.Short() {
		t.Skip("starts a container")
	}
	srv := pogi.Start(t, pogi.Config{ValidateSigs: true})
	if !srv.Hub.Docker().Available() {
		t.Skip("no container engine")
	}
	ctx := context.Background()
	c := dsql.NewFromConfig(awstest.Config(t, srv.URL))
	n, err := c.CreateCluster(ctx, &dsql.CreateClusterInput{DeletionProtectionEnabled: aws.Bool(false)})
	if err != nil {
		t.Fatal(err)
	}
	id := aws.ToString(n.Identifier)
	g, err := c.GetCluster(ctx, &dsql.GetClusterInput{Identifier: aws.String(id)})
	if err != nil || aws.ToString(g.Endpoint) != id+".dsql.us-east-1.on.aws" {
		t.Fatalf("get: %v %+v", err, g)
	}

	req, _ := http.NewRequest(http.MethodGet, "https://"+aws.ToString(g.Endpoint)+"/?Action=DbConnectAdmin&X-Amz-Expires=900", nil)
	empty := sha256.Sum256(nil)
	tok, _, err := v4.NewSigner().PresignHTTP(ctx, aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, req, hex.EncodeToString(empty[:]), "dsql", "us-east-1", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	token := strings.TrimPrefix(tok, "https://")
	dsn := "host=127.0.0.1 port=" + srv.URL[strings.LastIndexByte(srv.URL, ':')+1:] + " user=admin dbname=postgres sslmode=require password="

	var conn *pgx.Conn
	for end := time.Now().Add(5 * time.Minute); ; time.Sleep(time.Second) { // the first run pulls the image
		if conn, err = pgx.Connect(ctx, dsn+token); err == nil {
			break
		}
		if !strings.Contains(err.Error(), "not ready") || time.Now().After(end) {
			t.Fatalf("connect: %v", err)
		}
	}
	defer conn.Close(ctx)
	var got string
	if _, err := conn.Exec(ctx, `CREATE TABLE t (name text)`); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO t VALUES ('pogi')`); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, `SELECT name FROM t`).Scan(&got); err != nil || got != "pogi" {
		t.Fatalf("select: %q, %v", got, err)
	}
	if _, err := pgx.Connect(ctx, dsn+strings.Replace(token, "X-Amz-Signature=", "X-Amz-Signature=0", 1)); err == nil {
		t.Fatal("a token with a bad signature was accepted")
	}

	if _, err := c.DeleteCluster(ctx, &dsql.DeleteClusterInput{Identifier: aws.String(id)}); err != nil {
		t.Fatal(err)
	}
	for end := time.Now().Add(time.Minute); ; time.Sleep(500 * time.Millisecond) {
		out, _ := exec.Command("docker", "ps", "-aq", "--filter", "name="+id).Output()
		if len(strings.TrimSpace(string(out))) == 0 {
			break
		}
		if time.Now().After(end) {
			t.Fatalf("the container of %s is still there", id)
		}
	}
}
