package web_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/docker"
)

// TestSQLEditor runs statements in a real PostgreSQL container made by RDS;
// it skips without a container engine.
func TestSQLEditor(t *testing.T) {
	if testing.Short() || !docker.New(docker.Options{}).Available() {
		t.Skip("needs a running container engine")
	}
	srv := pogi.Start(t, pogi.Config{Services: []string{"rds"}})
	c := rds.NewFromConfig(awstest.Config(t, srv.URL))
	ctx := context.Background()
	id := fmt.Sprintf("websql-%d", os.Getpid())
	if _, err := c.CreateDBInstance(ctx, &rds.CreateDBInstanceInput{DBInstanceIdentifier: aws.String(id), Engine: aws.String("postgres"), DBInstanceClass: aws.String("db.t3.micro"),
		MasterUsername: aws.String("admin"), MasterUserPassword: aws.String("secret123"), DBName: aws.String("shop"), AllocatedStorage: aws.Int32(20)}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c.DeleteDBInstance(ctx, &rds.DeleteDBInstanceInput{DBInstanceIdentifier: aws.String(id), SkipFinalSnapshot: aws.Bool(true)})
	})
	for deadline := time.Now().Add(5 * time.Minute); ; time.Sleep(time.Second) { // the first run pulls the image
		out, err := c.DescribeDBInstances(ctx, &rds.DescribeDBInstancesInput{DBInstanceIdentifier: aws.String(id)})
		if err != nil {
			t.Fatal(err)
		}
		if s := aws.ToString(out.DBInstances[0].DBInstanceStatus); s == "available" {
			break
		} else if s == "failed" || time.Now().After(deadline) {
			t.Fatalf("instance is %s", s)
		}
	}

	target := "rds|" + id
	has(t, page(t, srv, "/_pogi/sql"), `value="`+target+`"`)
	run := func(sql string) string {
		t.Helper()
		var p string
		for i := 0; i < 30; i++ { // the server may still be starting after it first answers
			p = form(t, srv, "/_pogi/sql", url.Values{"target": {target}, "sql": {sql}})
			if !strings.Contains(p, "starting up") && !strings.Contains(p, "No such file") {
				break
			}
			time.Sleep(time.Second)
		}
		return p
	}
	shows(t, run("CREATE TABLE pets (id int, name text, note text)"), "CREATE TABLE")
	shows(t, run("INSERT INTO pets VALUES (1, 'Rex', 'good, boy'), (2, 'Tom', NULL)"), "INSERT 0 2")
	p := run("SELECT id, name, note FROM pets ORDER BY id")
	shows(t, p, "2 rows", ">Rex<", ">good, boy<", ">Tom<", "<th>name</th>")
	shows(t, run("SELECT count(*) AS n FROM pets WHERE id > 1"), "1 rows", "<th>n</th>", ">1<")
	shows(t, run("SELECT * FROM missing"), `class="flash"`, `relation &#34;missing&#34; does not exist`)
	shows(t, form(t, srv, "/_pogi/sql", url.Values{"target": {"rds|nope"}, "sql": {"SELECT 1"}}), `class="flash"`)
}
