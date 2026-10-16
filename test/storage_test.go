package test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

const (
	flushEvery = 300 * time.Millisecond
	writes     = 25
)

// TestStorageChild is the pogi process that TestKillNine starts and kills.
func TestStorageChild(t *testing.T) {
	dir := os.Getenv("POGI_TEST_DATA_DIR")
	if dir == "" {
		t.Skip("started by TestKillNine")
	}
	srv := pogi.New(pogi.Config{Port: freePort(t), DataDir: dir, Storage: os.Getenv("POGI_TEST_STORAGE"),
		FlushInterval: flushEvery, NoTLS: true, NoPorts: true}, nil)
	t.Fatal(srv.Run(context.Background(), func(u string) { fmt.Println("pogi-url", u) }))
}

func freePort(t *testing.T) int {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// startPogi runs a pogi process on dir; kill is kill -9.
func startPogi(t *testing.T, dir, mode string) (url string, kill func()) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestStorageChild$")
	cmd.Env = append(os.Environ(), "POGI_TEST_DATA_DIR="+dir, "POGI_TEST_STORAGE="+mode)
	cmd.Stderr = os.Stderr
	out, _ := cmd.StdoutPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	kill = func() {
		cmd.Process.Kill() // SIGKILL: no shutdown, no last save
		cmd.Wait()
	}
	t.Cleanup(kill)
	sc := bufio.NewScanner(out)
	for sc.Scan() {
		if u, ok := strings.CutPrefix(sc.Text(), "pogi-url "); ok {
			go io.Copy(io.Discard, out)
			return u, kill
		}
	}
	t.Fatalf("pogi exited before it was ready: %v", cmd.Wait())
	return "", nil
}

// writeData makes acknowledged writes to the journaled stores (DynamoDB items,
// SQS messages, S3 objects), deletes one of each, and creates an SNS topic,
// a service without a journal.
func writeData(t *testing.T, url string) {
	ctx := context.Background()
	cfg := awstest.Config(t, url)
	db := dynamodb.NewFromConfig(cfg)
	_, err := db.CreateTable(ctx, &dynamodb.CreateTableInput{TableName: aws.String("items"), BillingMode: ddbtypes.BillingModePayPerRequest,
		AttributeDefinitions: []ddbtypes.AttributeDefinition{{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS}},
		KeySchema:            []ddbtypes.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash}}})
	noErr(t, err)
	q := sqs.NewFromConfig(cfg)
	qu, err := q.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("jobs")})
	noErr(t, err)
	bk := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
	_, err = bk.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("files")})
	noErr(t, err)
	_, err = sns.NewFromConfig(cfg).CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("alerts")})
	noErr(t, err)
	for i := range writes {
		id := strconv.Itoa(i)
		_, err = db.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("items"), Item: map[string]ddbtypes.AttributeValue{
			"id": &ddbtypes.AttributeValueMemberS{Value: id}}})
		noErr(t, err)
		_, err = q.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: qu.QueueUrl, MessageBody: aws.String("m" + id)})
		noErr(t, err)
		_, err = bk.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("files"), Key: aws.String("k" + id), Body: strings.NewReader("v" + id)})
		noErr(t, err)
	}
	_, err = db.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: aws.String("items"), Key: map[string]ddbtypes.AttributeValue{
		"id": &ddbtypes.AttributeValueMemberS{Value: "0"}}})
	noErr(t, err)
	got, err := q.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: qu.QueueUrl})
	noErr(t, err)
	_, err = q.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: qu.QueueUrl, ReceiptHandle: got.Messages[0].ReceiptHandle})
	noErr(t, err)
	_, err = bk.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String("files"), Key: aws.String("k0")})
	noErr(t, err)
}

// waitFlushed waits until hybrid mode has written what writeData did (and its
// deletes) to the state files: a flush takes as long as the machine is busy, so
// no fixed sleep says when.
func waitFlushed(t *testing.T, dir string) {
	t.Helper()
	marks := []struct{ file, has, hasNot string }{
		{"s3", `"k24":`, `"k0":`},
		{"sqs", `"body":"m24"`, `"body":"m0"`},
		{"dynamodb", `S:24\u0000`, `S:0\u0000`},
		{"sns", `"alerts"`, ""},
	}
	for deadline := time.Now().Add(time.Minute); ; time.Sleep(20 * time.Millisecond) {
		missing := ""
		for _, m := range marks {
			b, _ := os.ReadFile(filepath.Join(dir, "state", m.file+".json"))
			if !strings.Contains(string(b), m.has) || m.hasNot != "" && strings.Contains(string(b), m.hasNot) {
				missing = m.file
				break
			}
		}
		if missing == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("hybrid never wrote the state of %s", missing)
		}
	}
}

// checkData checks that everything writeData did is there, or with empty
// that nothing is.
func checkData(t *testing.T, url string, empty bool) {
	t.Helper()
	ctx := context.Background()
	cfg := awstest.Config(t, url)
	want := func(what string, got, n int) {
		t.Helper()
		if empty {
			n = 0
		}
		if got != n {
			t.Errorf("%s: got %d, want %d", what, got, n)
		}
	}
	db := dynamodb.NewFromConfig(cfg)
	tables, err := db.ListTables(ctx, &dynamodb.ListTablesInput{})
	noErr(t, err)
	want("tables", len(tables.TableNames), 1)
	q := sqs.NewFromConfig(cfg)
	queues, err := q.ListQueues(ctx, &sqs.ListQueuesInput{})
	noErr(t, err)
	want("queues", len(queues.QueueUrls), 1)
	bk := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
	buckets, err := bk.ListBuckets(ctx, &s3.ListBucketsInput{})
	noErr(t, err)
	want("buckets", len(buckets.Buckets), 1)
	topics, err := sns.NewFromConfig(cfg).ListTopics(ctx, &sns.ListTopicsInput{})
	noErr(t, err)
	want("topics", len(topics.Topics), 1)
	if empty || t.Failed() {
		return
	}
	scan, err := db.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String("items")})
	noErr(t, err)
	want("items", int(scan.Count), writes-1)
	attrs, err := q.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: &queues.QueueUrls[0],
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameApproximateNumberOfMessages}})
	noErr(t, err)
	msgs, _ := strconv.Atoi(attrs.Attributes["ApproximateNumberOfMessages"])
	want("visible messages", msgs, writes-1)
	objs, err := bk.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String("files")})
	noErr(t, err)
	want("objects", int(aws.ToInt32(objs.KeyCount)), writes-1)
	obj, err := bk.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("files"), Key: aws.String("k7")})
	noErr(t, err)
	if b, _ := io.ReadAll(obj.Body); string(b) != "v7" {
		t.Errorf("object k7 = %q", b)
	}
}

func noErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func postOK(t *testing.T, url string) {
	t.Helper()
	if code, body := call(t, "POST", url); code != 200 {
		t.Fatalf("POST %s: %d %s", url, code, body)
	}
}

// TestKillNine checks that acknowledged writes survive kill -9: at once in
// persistent and wal modes, after a flush interval in hybrid mode. A reset
// and a snapshot load must stick as well.
func TestKillNine(t *testing.T) {
	if testing.Short() {
		t.Skip("starts pogi processes")
	}
	for _, mode := range []string{"persistent", "wal", "hybrid"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			url, kill := startPogi(t, dir, mode)
			writeData(t, url)
			if mode == "hybrid" {
				waitFlushed(t, dir)
			}
			kill()

			url, kill = startPogi(t, dir, mode)
			checkData(t, url, false)
			_, info := call(t, "GET", url+"/_pogi/api/info")
			var st core.Status
			json.Unmarshal([]byte(info), &st)
			if !st.Persist {
				t.Error("info: persist = false")
			}
			postOK(t, url+"/_pogi/api/snapshots/before")
			postOK(t, url+"/_pogi/api/reset")
			kill()

			url, kill = startPogi(t, dir, mode)
			checkData(t, url, true)
			postOK(t, url+"/_pogi/api/snapshots/before/load")
			kill()

			url, _ = startPogi(t, dir, mode)
			checkData(t, url, false)
		})
	}
}

// TestStorageShutdown checks the last write on a clean exit: hybrid long
// before its flush interval, wal by folding the journal into the file.
func TestStorageShutdown(t *testing.T) {
	for _, mode := range []string{"hybrid", "wal"} {
		dir := t.TempDir()
		ctx, stop := context.WithCancel(context.Background())
		srv := pogi.New(pogi.Config{Port: freePort(t), DataDir: dir, Storage: mode, FlushInterval: time.Hour, NoTLS: true, NoPorts: true}, nil)
		ready, done := make(chan string), make(chan error)
		go func() { done <- srv.Run(ctx, func(u string) { ready <- u }) }()
		bk := s3.NewFromConfig(awstest.Config(t, <-ready), func(o *s3.Options) { o.UsePathStyle = true })
		_, err := bk.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("kept")})
		noErr(t, err)
		stop()
		noErr(t, <-done)
		if st, err := os.Stat(filepath.Join(dir, "state", "s3.wal")); mode == "wal" && (err != nil || st.Size() != 0) {
			t.Errorf("wal: the journal should be compacted on exit: %v %v", st, err)
		}

		again := pogi.New(pogi.Config{DataDir: dir, Storage: mode}, nil)
		noErr(t, again.Hub.OpenStorage())
		again.Hub.Close()
		if rows := again.Hub.Resources("s3")["s3"][0].Rows; len(rows) != 1 || rows[0][0] != "kept" {
			t.Errorf("%s: s3 after a clean exit: %v", mode, rows)
		}
	}
}

// TestStorageMigration splits the single state.json of older versions into
// per-service files; --persist now means hybrid.
func TestStorageMigration(t *testing.T) {
	dir := t.TempDir()
	old := pogi.Start(t)
	bk := s3.NewFromConfig(awstest.Config(t, old.URL), func(o *s3.Options) { o.UsePathStyle = true })
	_, err := bk.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String("legacy")})
	noErr(t, err)
	noErr(t, old.Hub.SaveFile(filepath.Join(dir, "state.json")))

	srv := pogi.New(pogi.Config{DataDir: dir, Persist: true}, nil)
	noErr(t, srv.Hub.OpenStorage())
	t.Cleanup(srv.Hub.Close)
	for _, f := range []string{"state/s3.json", "state/sqs.json", "state.json.migrated"} {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			t.Error(err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "state.json")); err == nil {
		t.Error("state.json is still there")
	}
	if rows := srv.Hub.Resources("s3")["s3"][0].Rows; len(rows) != 1 || rows[0][0] != "legacy" {
		t.Errorf("s3 after migration: %v", rows)
	}
}
