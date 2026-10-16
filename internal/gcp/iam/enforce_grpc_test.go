package iam_test

import (
	"context"
	"strings"
	"testing"
	"time"

	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	"google.golang.org/api/iam/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// gRPC calls are checked the same way: the token comes from the metadata, the resource from the request.
func TestStrictGRPC(t *testing.T) {
	s := pogi.Start(t, pogi.Config{Services: []string{"gcpiam", "resourcemanager", "pubsub"}, IAM: "strict"})
	sa := must(rest(t, s).Projects.ServiceAccounts.Create("projects/pogi", &iam.CreateServiceAccountRequest{AccountId: "orders", ServiceAccount: &iam.ServiceAccount{}}).Do())
	conn := must(grpc.NewClient(strings.TrimPrefix(s.URL, "http://"), grpc.WithTransportCredentials(insecure.NewCredentials())))
	defer conn.Close()
	pub := pb.NewPublisherClient(conn)
	as := func(token string) context.Context {
		return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	}
	if _, err := pub.CreateTopic(as(""), &pb.Topic{Name: "projects/pogi/topics/orders"}); err != nil {
		t.Fatalf("default credentials: %v", err)
	}
	tok := gapi.AccessToken(sa.Email, "", time.Hour)
	msg := &pb.PublishRequest{Topic: "projects/pogi/topics/orders", Messages: []*pb.PubsubMessage{{Data: []byte("hi")}}}
	if _, err := pub.Publish(as(tok), msg); status.Code(err) != codes.PermissionDenied || !strings.Contains(err.Error(), "pubsub.topics.publish") {
		t.Fatalf("publish without a binding: %v", err)
	}
	call(t, s, "POST", "/v1/projects/pogi:setIamPolicy", "", `{"policy":{"bindings":[{"role":"roles/pubsub.publisher","members":["serviceAccount:`+sa.Email+`"]}]}}`)
	if _, err := pub.Publish(as(tok), msg); err != nil {
		t.Fatalf("publish with the binding: %v", err)
	}
}

// Cloud Storage's JSON API names its resource in the path, and buckets keep their own policy.
func TestStrictStorage(t *testing.T) {
	s := pogi.Start(t, pogi.Config{Services: []string{"gcpiam", "resourcemanager", "gcs"}, IAM: "strict"})
	sa := must(rest(t, s).Projects.ServiceAccounts.Create("projects/pogi", &iam.CreateServiceAccountRequest{AccountId: "reader", ServiceAccount: &iam.ServiceAccount{}}).Do())
	tok := gapi.AccessToken(sa.Email, "", time.Hour)
	if code, b := call(t, s, "POST", "/storage/v1/b?project=pogi", "", `{"name":"data"}`); code != 200 {
		t.Fatalf("create bucket: %d %s", code, b)
	}
	if code, _ := call(t, s, "POST", "/storage/v1/b?project=pogi", tok, `{"name":"other"}`); code != 403 {
		t.Fatalf("create bucket without a binding: %d", code)
	}
	if code, b := call(t, s, "GET", "/storage/v1/b/data/o", tok, ""); code != 403 || !strings.Contains(b, "storage.objects.list") {
		t.Fatalf("list without a binding: %d %s", code, b)
	}
	member := "serviceAccount:" + sa.Email
	if code, b := call(t, s, "PUT", "/storage/v1/b/data/iam", "", `{"bindings":[{"role":"roles/storage.objectViewer","members":["`+member+`"]}]}`); code != 200 {
		t.Fatalf("set bucket policy: %d %s", code, b)
	}
	if code, b := call(t, s, "GET", "/storage/v1/b/data/o", tok, ""); code != 200 {
		t.Fatalf("list with the binding: %d %s", code, b)
	}
	if code, _ := call(t, s, "DELETE", "/storage/v1/b/data/o/x", tok, ""); code != 403 {
		t.Fatalf("delete with a viewer binding: %d", code)
	}
}
