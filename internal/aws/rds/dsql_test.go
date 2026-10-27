package rds_test

import (
	"crypto/sha256"
	"encoding/hex"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/jackc/pgx/v5"

	"github.com/nrndev/pogi/internal/aws/rds"
	"github.com/nrndev/pogi/internal/core"
)

// dsqlToken is what `aws dsql generate-db-connect-admin-auth-token` makes: a presigned URL without its scheme.
func dsqlToken(t *testing.T, host, action, service string, at time.Time, expires int) string {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, "https://"+host+"/?Action="+action+"&X-Amz-Expires="+strconv.Itoa(expires), nil)
	empty := sha256.Sum256(nil)
	u, _, err := v4.NewSigner().PresignHTTP(ctx, aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, req, hex.EncodeToString(empty[:]), service, "us-east-1", at)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimPrefix(u, "https://")
}

// TestDSQLNoDockerAndTokens: without Docker a cluster's SQL endpoint answers every good token with an error that
// says why, and every bad token is refused as a failed login; a client that does not start TLS is refused.
func TestDSQLNoDockerAndTokens(t *testing.T) {
	fake(t, true)
	h := core.NewHub(core.Config{ValidateSigs: true, DataDir: t.TempDir()}, nil)
	svc := rds.New(h)
	h.Register(svc)
	t.Cleanup(h.Close)
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	h.Sniff(ln, nil)
	port := ln.Addr().(*net.TCPAddr).Port

	const id = "abcdefghijklmnopqrstuvwxyz"
	svc.AddSQL(core.DefaultAccount, "us-east-1", id)
	host := id + ".dsql.us-east-1.on.aws"
	now := time.Now()
	connect := func(user, token, ssl string) string {
		c, err := pgx.Connect(ctx, "host=127.0.0.1 port="+strconv.Itoa(port)+" user="+user+" dbname=postgres sslmode="+ssl+" password="+token)
		if err == nil {
			c.Close(ctx)
			return ""
		}
		return err.Error()
	}
	good := dsqlToken(t, host, "DbConnectAdmin", "dsql", now, 900)
	tampered := strings.Replace(good, "X-Amz-Signature=", "X-Amz-Signature=0", 1)
	for _, c := range []struct{ name, user, token, ssl, want string }{
		{"no Docker", "admin", good, "require", "needs Docker"},
		{"a week long token", "admin", dsqlToken(t, host, "DbConnectAdmin", "dsql", now, 604800), "require", "needs Docker"},
		{"wrong action", "admin", dsqlToken(t, host, "DbConnect", "dsql", now, 900), "require", "PAM authentication failed"},
		{"admin action for a role", "app", good, "require", "PAM authentication failed"},
		{"wrong service", "admin", dsqlToken(t, host, "DbConnectAdmin", "rds-db", now, 900), "require", "PAM authentication failed"},
		{"expired", "admin", dsqlToken(t, host, "DbConnectAdmin", "dsql", now.Add(-time.Hour), 900), "require", "PAM authentication failed"},
		{"too long", "admin", dsqlToken(t, host, "DbConnectAdmin", "dsql", now, 604801), "require", "PAM authentication failed"},
		{"unknown cluster", "admin", dsqlToken(t, "zyxwvutsrqponmlkjihgfedcba.dsql.us-east-1.on.aws", "DbConnectAdmin", "dsql", now, 900), "require", "no cluster"},
		{"bad signature", "admin", tampered, "require", "PAM authentication failed"},
		{"not a token", "admin", "hunter2", "require", "PAM authentication failed"},
		{"not a DSQL host", "admin", dsqlToken(t, "db.example.com", "DbConnectAdmin", "dsql", now, 900), "require", "PAM authentication failed"},
		{"plain connection", "admin", good, "disable", "SSL/TLS connection is required"},
	} {
		got := connect(c.user, c.token, c.ssl)
		if c.want == "no cluster" { // the client is told a failed login; the reason is only logged
			c.want = "PAM authentication failed"
		}
		if !strings.Contains(got, c.want) {
			t.Errorf("%s: %q, want %q", c.name, got, c.want)
		}
	}
	svc.DropSQL(core.DefaultAccount, "us-east-1", id)
	if got := connect("admin", good, "require"); !strings.Contains(got, "PAM authentication failed") {
		t.Errorf("deleted cluster: %q", got)
	}
}
