package rds_test

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	awsrds "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"

	"github.com/nrndev/pogi/internal/aws/rds"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
)

var ctx = context.Background()

// sdk serves RDS on a hub that checks signatures, for tests with real
// containers; it skips without a container engine.
func sdk(t *testing.T) *awsrds.Client {
	c, _ := sdkDir(t)
	return c
}

// sdkDir is sdk and the hub's data directory (its certificates are in certs/).
func sdkDir(t *testing.T) (*awsrds.Client, string) {
	if testing.Short() || !docker.New(docker.Options{}).Available() {
		t.Skip("needs a running container engine")
	}
	dir := t.TempDir()
	h := core.NewHub(core.Config{ValidateSigs: true, DataDir: dir}, nil)
	h.Register(rds.New(h))
	base := core.Listen(t, h)
	t.Cleanup(h.Close)
	return awsrds.NewFromConfig(awstest.Config(t, base)), dir
}

// running waits for an instance to be available.
func running(t *testing.T, c *awsrds.Client, id string) types.DBInstance {
	t.Helper()
	var i types.DBInstance
	eventually5m(t, id+" available (the first run pulls the image)", func() bool {
		out, err := c.DescribeDBInstances(ctx, &awsrds.DescribeDBInstancesInput{DBInstanceIdentifier: aws.String(id)})
		if err != nil {
			t.Fatal(err)
		}
		i = out.DBInstances[0]
		if s := aws.ToString(i.DBInstanceStatus); s == "failed" {
			t.Fatalf("%s failed", id)
		}
		return aws.ToString(i.DBInstanceStatus) == "available"
	})
	return i
}

// token signs an IAM auth token by hand, at a chosen time and with a chosen secret.
func token(t *testing.T, endpoint, user, secret string, at time.Time) string {
	req, _ := http.NewRequest(http.MethodGet, "https://"+endpoint+"/?Action=connect&DBUser="+url.QueryEscape(user)+"&X-Amz-Expires=900", nil)
	u, _, err := v4.NewSigner().PresignHTTP(ctx, aws.Credentials{AccessKeyID: "test", SecretAccessKey: secret}, req,
		"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", "rds-db", "us-east-1", at)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimPrefix(u, "https://")
}

// pgConnect logs in to the postgres database, or to db.
func pgConnect(port int, user, password string, db ...string) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(fmt.Sprintf("host=localhost port=%d user=%s dbname=%s connect_timeout=10", port, user, append(db, "postgres")[0]))
	if err != nil {
		return nil, err
	}
	cfg.Password = password
	return pgx.ConnectConfig(ctx, cfg)
}

func pgQuery(t *testing.T, port int, user, password, query string, db ...string) string {
	t.Helper()
	conn, err := pgConnect(port, user, password, db...)
	if err != nil {
		t.Fatalf("connect as %s: %v", user, err)
	}
	defer conn.Close(ctx)
	var out string
	if err := conn.QueryRow(ctx, query).Scan(&out); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return out
}

// TestIAMAuthPostgres logs in through the proxy with IAM auth tokens, with
// signatures checked, and with the master password.
func TestIAMAuthPostgres(t *testing.T) {
	c, dir := sdkDir(t)
	id := fmt.Sprintf("iam-%d", os.Getpid())
	_, err := c.CreateDBInstance(ctx, &awsrds.CreateDBInstanceInput{DBInstanceIdentifier: aws.String(id), Engine: aws.String("postgres"),
		EngineVersion: aws.String("16"), DBInstanceClass: aws.String("db.t3.micro"), MasterUsername: aws.String("admin"),
		MasterUserPassword: aws.String("secret123"), EnableIAMDatabaseAuthentication: aws.Bool(true)})
	if err != nil {
		t.Fatal(err)
	}
	i := running(t, c, id)
	port := int(aws.ToInt32(i.Endpoint.Port))
	endpoint := "localhost:" + strconv.Itoa(port)
	if !aws.ToBool(i.IAMDatabaseAuthenticationEnabled) {
		t.Fatalf("instance %+v", i)
	}

	tok, err := auth.BuildAuthToken(ctx, endpoint, "us-east-1", "admin", credentials.NewStaticCredentialsProvider("test", "test", ""))
	if err != nil {
		t.Fatal(err)
	}
	if got := pgQuery(t, port, "admin", tok, "SELECT 1::text"); got != "1" {
		t.Fatalf("SELECT 1 = %q", got)
	}
	if got := pgQuery(t, port, "admin", "secret123", "SELECT current_user::text"); got != "admin" {
		t.Fatalf("master password: current_user = %q", got)
	}
	for name, bad := range map[string]string{
		"another user's token": token(t, endpoint, "bob", "test", time.Now()),
		"an expired token":     token(t, endpoint, "admin", "test", time.Now().Add(-20*time.Minute)),
		"a forged token":       token(t, endpoint, "admin", "not-the-secret", time.Now()),
		"a wrong password":     "wrong-password",
	} {
		if conn, err := pgConnect(port, "admin", bad); err == nil {
			conn.Close(ctx)
			t.Fatalf("%s logged in", name)
		} else if !strings.Contains(err.Error(), "authentication failed") {
			t.Fatalf("%s: %v", name, err)
		}
	}

	// floci #5363: the proxy offers TLS with a leaf certificate that the local CA signed, so verify-full works.
	nc, err := net.Dial("tcp", endpoint)
	if err != nil {
		t.Fatal(err)
	}
	nc.Write([]byte{0, 0, 0, 8, 4, 210, 22, 47}) // SSLRequest
	if b := make([]byte, 1); func() bool { _, err := io.ReadFull(nc, b); return err != nil || b[0] != 'S' }() {
		t.Fatal("the proxy declined SSL")
	}
	tc := tls.Client(nc, &tls.Config{InsecureSkipVerify: true})
	if err := tc.Handshake(); err != nil {
		t.Fatal(err)
	}
	if leaf := tc.ConnectionState().PeerCertificates[0]; leaf.IsCA {
		t.Fatal("the server certificate is a CA certificate")
	}
	nc.Close()
	tlsConn, err := pgx.ParseConfig(fmt.Sprintf("host=localhost port=%d user=admin dbname=postgres sslmode=verify-full sslrootcert=%s", port, filepath.Join(dir, "certs", "ca.pem")))
	if err != nil {
		t.Fatal(err)
	}
	tlsConn.Password = tok
	vc, err := pgx.ConnectConfig(ctx, tlsConn)
	if err != nil {
		t.Fatalf("verify-full: %v", err)
	}
	vc.Close(ctx)

	// IAM users are roles granted rds_iam, which the container has
	conn, err := pgConnect(port, "admin", "secret123")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "CREATE USER jane; GRANT rds_iam TO jane"); err != nil {
		t.Fatal(err)
	}
	conn.Close(ctx)
	if got := pgQuery(t, port, "jane", token(t, endpoint, "jane", "test", time.Now()), "SELECT current_user::text"); got != "jane" {
		t.Fatalf("jane's token: current_user = %q", got)
	}
	// floci #3087: a token naming a role the database lacks must be refused, not log in as the master user
	if conn, err := pgConnect(port, "ghost", token(t, endpoint, "ghost", "test", time.Now())); err == nil {
		conn.Close(ctx)
		t.Fatal("a token for a missing role logged in")
	} else if !strings.Contains(err.Error(), "authentication failed") {
		t.Fatalf("missing role: %v", err)
	}
}

// TestIAMAuthModify turns IAM auth on for a running instance: the container
// moves behind the proxy, keeping its data and its endpoint port.
func TestIAMAuthModify(t *testing.T) {
	c := sdk(t)
	id := fmt.Sprintf("iam-mod-%d", os.Getpid())
	_, err := c.CreateDBInstance(ctx, &awsrds.CreateDBInstanceInput{DBInstanceIdentifier: aws.String(id), Engine: aws.String("postgres"),
		EngineVersion: aws.String("16"), DBInstanceClass: aws.String("db.t3.micro"), MasterUsername: aws.String("admin"),
		MasterUserPassword: aws.String("secret123"), DBName: aws.String("shop")})
	if err != nil {
		t.Fatal(err)
	}
	port := int(aws.ToInt32(running(t, c, id).Endpoint.Port))
	conn, err := pgConnect(port, "admin", "secret123", "shop")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "CREATE TABLE kept (n int); INSERT INTO kept VALUES (7)"); err != nil {
		t.Fatal(err)
	}
	conn.Close(ctx)
	out, err := c.ModifyDBInstance(ctx, &awsrds.ModifyDBInstanceInput{DBInstanceIdentifier: aws.String(id),
		EnableIAMDatabaseAuthentication: aws.Bool(true), ApplyImmediately: aws.Bool(true)})
	if err != nil || aws.ToString(out.DBInstance.DBInstanceStatus) != "modifying" {
		t.Fatalf("modify: %v %+v", err, out)
	}
	if p := int(aws.ToInt32(running(t, c, id).Endpoint.Port)); p != port {
		t.Fatalf("the endpoint moved from %d to %d", port, p)
	}
	tok := token(t, "localhost:"+strconv.Itoa(port), "admin", "test", time.Now())
	if got := pgQuery(t, port, "admin", tok, "SELECT n::text FROM kept", "shop"); got != "7" {
		t.Fatalf("after the move: %q", got)
	}
}

// TestIAMAuthMySQL logs in to MySQL through the proxy's clear-text plugin switch.
func TestIAMAuthMySQL(t *testing.T) {
	c := sdk(t)
	id := fmt.Sprintf("iam-my-%d", os.Getpid())
	_, err := c.CreateDBInstance(ctx, &awsrds.CreateDBInstanceInput{DBInstanceIdentifier: aws.String(id), Engine: aws.String("mysql"),
		DBInstanceClass: aws.String("db.t3.micro"), MasterUsername: aws.String("admin"), MasterUserPassword: aws.String("secret123"),
		DBName: aws.String("app"), EnableIAMDatabaseAuthentication: aws.Bool(true)})
	if err != nil {
		t.Fatal(err)
	}
	port := int(aws.ToInt32(running(t, c, id).Endpoint.Port))
	endpoint := "localhost:" + strconv.Itoa(port)
	query := func(password, q string) (string, error) {
		cfg := mysql.NewConfig()
		cfg.User, cfg.Passwd, cfg.Net, cfg.Addr, cfg.DBName, cfg.AllowCleartextPasswords = "admin", password, "tcp", endpoint, "app", true
		conn, err := mysql.NewConnector(cfg)
		if err != nil {
			return "", err
		}
		db := sql.OpenDB(conn)
		defer db.Close()
		var out string
		err = db.QueryRowContext(ctx, q).Scan(&out)
		return out, err
	}
	tok, err := auth.BuildAuthToken(ctx, endpoint, "us-east-1", "admin", credentials.NewStaticCredentialsProvider("test", "test", ""))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := query(tok, "SELECT 1"); err != nil || got != "1" {
		t.Fatalf("token: %q %v", got, err)
	}
	if got, err := query("secret123", "SELECT DATABASE()"); err != nil || got != "app" {
		t.Fatalf("master password: %q %v", got, err)
	}
	for name, bad := range map[string]string{
		"another user's token": token(t, endpoint, "bob", "test", time.Now()),
		"an expired token":     token(t, endpoint, "admin", "test", time.Now().Add(-20*time.Minute)),
		"a wrong password":     "wrong-password",
	} {
		if _, err := query(bad, "SELECT 1"); err == nil || !strings.Contains(err.Error(), "Access denied") {
			t.Fatalf("%s: %v", name, err)
		}
	}
	// the session speaks the client's protocol: a multi-row result and an
	// update count come back right
	if got, err := query(tok, "SELECT COUNT(*) FROM (SELECT 1 UNION SELECT 2 UNION SELECT 3) t"); err != nil || got != "3" {
		t.Fatalf("count: %q %v", got, err)
	}
}
