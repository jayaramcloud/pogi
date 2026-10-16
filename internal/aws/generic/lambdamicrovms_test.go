package generic_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambdamicrovms"
	"github.com/aws/aws-sdk-go-v2/service/lambdamicrovms/types"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestLambdaMicrovms(t *testing.T) {
	s := pogi.Start(t)
	ctx := context.Background()
	c := lambdamicrovms.NewFromConfig(awstest.Config(t, s.URL))
	lifecycle(t, time.Hour)

	img, err := c.CreateMicrovmImage(ctx, &lambdamicrovms.CreateMicrovmImageInput{Name: aws.String("img1"), BaseImageArn: aws.String("arn:aws:lambda:us-east-1::microvm-image:base"),
		BuildRoleArn: aws.String("arn:aws:iam::000000000000:role/build"), Description: aws.String("first"), Tags: map[string]string{"a": "b"},
		CodeArtifact: &types.CodeArtifactMemberUri{Value: "s3://code/img.zip"}})
	if err != nil || aws.ToString(img.Name) != "img1" || !strings.Contains(aws.ToString(img.ImageArn), "img1") || img.State != types.MicrovmImageStateCreating {
		t.Fatalf("create image: %v %+v", err, img)
	}
	if _, err := c.CreateMicrovmImage(ctx, &lambdamicrovms.CreateMicrovmImageInput{Name: aws.String("img1"), BaseImageArn: aws.String("x"), BuildRoleArn: aws.String("x"),
		CodeArtifact: &types.CodeArtifactMemberUri{Value: "s3://code/img.zip"}}); err == nil {
		t.Fatal("duplicate image")
	}
	lifecycle(t, 0)
	g, err := c.GetMicrovmImage(ctx, &lambdamicrovms.GetMicrovmImageInput{ImageIdentifier: aws.String("img1")})
	if err != nil || g.State != types.MicrovmImageStateCreated || g.Tags["a"] != "b" || aws.ToString(g.LatestActiveImageVersion) != "1" {
		t.Fatalf("get image: %v %+v", err, g)
	}
	if l, err := c.ListMicrovmImages(ctx, &lambdamicrovms.ListMicrovmImagesInput{}); err != nil || len(l.Items) != 1 {
		t.Fatalf("list images: %v %+v", err, l)
	}
	lifecycle(t, time.Hour)
	up, err := c.UpdateMicrovmImage(ctx, &lambdamicrovms.UpdateMicrovmImageInput{ImageIdentifier: aws.String("img1"), Description: aws.String("second"), BaseImageArn: aws.String("arn:aws:lambda:us-east-1::microvm-image:base"),
		BuildRoleArn: aws.String("arn:aws:iam::000000000000:role/build"), CodeArtifact: &types.CodeArtifactMemberUri{Value: "s3://code/img2.zip"}})
	if err != nil || up.State != types.MicrovmImageStateUpdating || aws.ToString(up.ImageVersion) != "2" {
		t.Fatalf("update image: %v %+v", err, up)
	}
	lifecycle(t, 0)
	if g, _ := c.GetMicrovmImage(ctx, &lambdamicrovms.GetMicrovmImageInput{ImageIdentifier: aws.String("img1")}); g.State != types.MicrovmImageStateUpdated || aws.ToString(g.LatestActiveImageVersion) != "2" {
		t.Fatalf("updated image: %+v", g)
	}

	// a microVM: pending, running, suspended, resumed, terminated
	lifecycle(t, time.Hour)
	vm, err := c.RunMicrovm(ctx, &lambdamicrovms.RunMicrovmInput{ImageIdentifier: aws.String("img1"), MaximumDurationInSeconds: aws.Int32(600)})
	if err != nil || !strings.HasPrefix(aws.ToString(vm.MicrovmId), "mvm-") || vm.State != types.MicrovmStatePending || aws.ToString(vm.Endpoint) == "" || aws.ToInt32(vm.MaximumDurationInSeconds) != 600 {
		t.Fatalf("run: %v %+v", err, vm)
	}
	state := func() types.MicrovmState {
		g, err := c.GetMicrovm(ctx, &lambdamicrovms.GetMicrovmInput{MicrovmIdentifier: vm.MicrovmId})
		if err != nil {
			t.Fatal(err)
		}
		return g.State
	}
	lifecycle(t, 0)
	if st := state(); st != types.MicrovmStateRunning {
		t.Fatalf("running: %s", st)
	}
	if l, err := c.ListMicrovms(ctx, &lambdamicrovms.ListMicrovmsInput{ImageIdentifier: aws.String("img1")}); err != nil || len(l.Items) != 1 || l.Items[0].State != types.MicrovmStateRunning {
		t.Fatalf("list: %v %+v", err, l)
	}
	if l, _ := c.ListMicrovms(ctx, &lambdamicrovms.ListMicrovmsInput{ImageIdentifier: aws.String("other")}); len(l.Items) != 0 {
		t.Fatalf("list other image: %+v", l)
	}
	lifecycle(t, time.Hour)
	if _, err := c.SuspendMicrovm(ctx, &lambdamicrovms.SuspendMicrovmInput{MicrovmIdentifier: vm.MicrovmId}); err != nil {
		t.Fatal(err)
	}
	if st := state(); st != types.MicrovmStateSuspending {
		t.Fatalf("suspending: %s", st)
	}
	lifecycle(t, 0)
	if st := state(); st != types.MicrovmStateSuspended {
		t.Fatalf("suspended: %s", st)
	}
	if _, err := c.ResumeMicrovm(ctx, &lambdamicrovms.ResumeMicrovmInput{MicrovmIdentifier: vm.MicrovmId}); err != nil {
		t.Fatal(err)
	}
	if st := state(); st != types.MicrovmStateRunning {
		t.Fatalf("resumed: %s", st)
	}
	if _, err := c.TerminateMicrovm(ctx, &lambdamicrovms.TerminateMicrovmInput{MicrovmIdentifier: vm.MicrovmId}); err != nil {
		t.Fatal(err)
	}
	if g, _ := c.GetMicrovm(ctx, &lambdamicrovms.GetMicrovmInput{MicrovmIdentifier: vm.MicrovmId}); g.State != types.MicrovmStateTerminated || g.TerminatedAt == nil {
		t.Fatalf("terminated: %+v", g)
	}
	if _, err := c.SuspendMicrovm(ctx, &lambdamicrovms.SuspendMicrovmInput{MicrovmIdentifier: vm.MicrovmId}); err == nil {
		t.Fatal("suspended a terminated microvm")
	}

	// tags, delete, typed not-found
	if _, err := c.TagResource(ctx, &lambdamicrovms.TagResourceInput{Resource: img.ImageArn, Tags: map[string]string{"c": "d"}}); err != nil {
		t.Fatal(err)
	}
	if l, err := c.ListTags(ctx, &lambdamicrovms.ListTagsInput{Resource: img.ImageArn}); err != nil || len(l.Tags) != 2 {
		t.Fatalf("tags: %v %+v", err, l)
	}
	if _, err := c.DeleteMicrovmImage(ctx, &lambdamicrovms.DeleteMicrovmImageInput{ImageIdentifier: aws.String("img1")}); err != nil {
		t.Fatal(err)
	}
	var nf *types.ResourceNotFoundException
	if _, err := c.GetMicrovmImage(ctx, &lambdamicrovms.GetMicrovmImageInput{ImageIdentifier: aws.String("img1")}); !errors.As(err, &nf) {
		t.Fatalf("get deleted image: %v", err)
	}
	if _, err := c.RunMicrovm(ctx, &lambdamicrovms.RunMicrovmInput{ImageIdentifier: aws.String("img1")}); !errors.As(err, &nf) {
		t.Fatalf("run a deleted image: %v", err)
	}
}
