package ecr_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecr"
	"github.com/aws/aws-sdk-go-v2/service/ecr/types"
	ecrsvc "github.com/nrndev/pogi/internal/aws/ecr"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

var ctx = context.Background()

// roundTrip snapshots, resets and restores the hub: state must survive Save and Load.
func roundTrip(t *testing.T, h *core.Hub) {
	t.Helper()
	b, err := h.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	h.Reset()
	if err := h.Restore(b); err != nil {
		t.Fatal(err)
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func TestRepositories(t *testing.T) {
	hub, url := core.ServeTest(t, func(h *core.Hub) core.Service { return ecrsvc.New(h) })
	c := ecr.NewFromConfig(awstest.Config(t, url))
	name := aws.String("app/web")

	repo := must(c.CreateRepository(ctx, &ecr.CreateRepositoryInput{RepositoryName: name,
		ImageScanningConfiguration: &types.ImageScanningConfiguration{ScanOnPush: true},
		Tags:                       []types.Tag{{Key: aws.String("team"), Value: aws.String("web")}}})).Repository
	if *repo.RepositoryUri != strings.TrimPrefix(url, "http://")+"/app/web" || *repo.RepositoryArn != "arn:aws:ecr:us-east-1:000000000000:repository/app/web" ||
		repo.ImageTagMutability != types.ImageTagMutabilityMutable || !repo.ImageScanningConfiguration.ScanOnPush || repo.CreatedAt == nil {
		t.Fatalf("repository = %+v", repo)
	}
	_, err := c.CreateRepository(ctx, &ecr.CreateRepositoryInput{RepositoryName: name})
	var exists *types.RepositoryAlreadyExistsException
	if !errors.As(err, &exists) {
		t.Fatalf("want RepositoryAlreadyExistsException, got %v", err)
	}
	_, err = c.DescribeRepositories(ctx, &ecr.DescribeRepositoriesInput{RepositoryNames: []string{"missing"}})
	var nf *types.RepositoryNotFoundException
	if !errors.As(err, &nf) {
		t.Fatalf("want RepositoryNotFoundException, got %v", err)
	}

	must(c.PutImageTagMutability(ctx, &ecr.PutImageTagMutabilityInput{RepositoryName: name, ImageTagMutability: types.ImageTagMutabilityImmutable}))
	if l := must(c.DescribeRepositories(ctx, &ecr.DescribeRepositoriesInput{})).Repositories; len(l) != 1 || l[0].ImageTagMutability != types.ImageTagMutabilityImmutable {
		t.Fatalf("repositories = %+v", l)
	}

	policy := aws.String(`{"rules":[{"rulePriority":1,"selection":{"tagStatus":"any","countType":"imageCountMoreThan","countNumber":5},"action":{"type":"expire"}}]}`)
	must(c.PutLifecyclePolicy(ctx, &ecr.PutLifecyclePolicyInput{RepositoryName: name, LifecyclePolicyText: policy}))
	if got := must(c.GetLifecyclePolicy(ctx, &ecr.GetLifecyclePolicyInput{RepositoryName: name})); *got.LifecyclePolicyText != *policy {
		t.Fatalf("lifecycle policy = %v", *got.LifecyclePolicyText)
	}
	_, err = c.GetRepositoryPolicy(ctx, &ecr.GetRepositoryPolicyInput{RepositoryName: name})
	var noPolicy *types.RepositoryPolicyNotFoundException
	if !errors.As(err, &noPolicy) {
		t.Fatalf("want RepositoryPolicyNotFoundException, got %v", err)
	}
	must(c.SetRepositoryPolicy(ctx, &ecr.SetRepositoryPolicyInput{RepositoryName: name, PolicyText: aws.String(`{"Version":"2012-10-17","Statement":[]}`)}))
	must(c.DeleteRepositoryPolicy(ctx, &ecr.DeleteRepositoryPolicyInput{RepositoryName: name}))
	roundTrip(t, hub)
	if got := must(c.GetLifecyclePolicy(ctx, &ecr.GetLifecyclePolicyInput{RepositoryName: name})); *got.LifecyclePolicyText != *policy {
		t.Fatalf("lifecycle policy after restore = %v", *got.LifecyclePolicyText)
	}

	auth := must(c.GetAuthorizationToken(ctx, &ecr.GetAuthorizationTokenInput{})).AuthorizationData[0]
	if tok := must(base64.StdEncoding.DecodeString(*auth.AuthorizationToken)); string(tok) != "AWS:password" || *auth.ProxyEndpoint != url {
		t.Fatalf("auth = %s %s", tok, *auth.ProxyEndpoint)
	}
	if imgs := must(c.ListImages(ctx, &ecr.ListImagesInput{RepositoryName: name})); len(imgs.ImageIds) != 0 {
		t.Fatalf("images = %+v", imgs.ImageIds)
	}
	_, err = c.DescribeImages(ctx, &ecr.DescribeImagesInput{RepositoryName: name, ImageIds: []types.ImageIdentifier{{ImageTag: aws.String("latest")}}})
	var noImage *types.ImageNotFoundException
	if !errors.As(err, &noImage) {
		t.Fatalf("want ImageNotFoundException, got %v", err)
	}

	must(c.TagResource(ctx, &ecr.TagResourceInput{ResourceArn: repo.RepositoryArn, Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}}))
	must(c.UntagResource(ctx, &ecr.UntagResourceInput{ResourceArn: repo.RepositoryArn, TagKeys: []string{"team"}}))
	if tags := must(c.ListTagsForResource(ctx, &ecr.ListTagsForResourceInput{ResourceArn: repo.RepositoryArn})).Tags; len(tags) != 1 || *tags[0].Key != "env" {
		t.Fatalf("tags = %+v", tags)
	}

	must(c.DeleteRepository(ctx, &ecr.DeleteRepositoryInput{RepositoryName: name, Force: true}))
	if l := must(c.DescribeRepositories(ctx, &ecr.DescribeRepositoriesInput{})).Repositories; len(l) != 0 {
		t.Fatalf("repositories after delete = %+v", l)
	}
}

// The image API: the layer upload some tools use instead of the registry,
// PutImage, reads, tag immutability, scans, lifecycle policies and deletes.
func TestImages(t *testing.T) {
	hub, url := core.ServeTest(t, func(h *core.Hub) core.Service { return ecrsvc.New(h) })
	c := ecr.NewFromConfig(awstest.Config(t, url))
	name := aws.String("app")
	must(c.CreateRepository(ctx, &ecr.CreateRepositoryInput{RepositoryName: name, ImageScanningConfiguration: &types.ImageScanningConfiguration{ScanOnPush: true}}))

	upload := func(b []byte) {
		id := must(c.InitiateLayerUpload(ctx, &ecr.InitiateLayerUploadInput{RepositoryName: name})).UploadId
		for i := 0; i < len(b); i += 1000 {
			end := min(i+1000, len(b))
			must(c.UploadLayerPart(ctx, &ecr.UploadLayerPartInput{RepositoryName: name, UploadId: id, LayerPartBlob: b[i:end],
				PartFirstByte: aws.Int64(int64(i)), PartLastByte: aws.Int64(int64(end - 1))}))
		}
		must(c.CompleteLayerUpload(ctx, &ecr.CompleteLayerUploadInput{RepositoryName: name, UploadId: id, LayerDigests: []string{digest(b)}}))
	}
	layer, config, config2 := bytes.Repeat([]byte("x"), 2500), []byte(`{"os":"linux"}`), []byte(`{"os":"linux","v":2}`)
	upload(layer)
	upload(config)
	check := must(c.BatchCheckLayerAvailability(ctx, &ecr.BatchCheckLayerAvailabilityInput{RepositoryName: name, LayerDigests: []string{digest(layer), digest(config2)}}))
	if len(check.Layers) != 1 || *check.Layers[0].LayerSize != 2500 || len(check.Failures) != 1 || check.Failures[0].FailureCode != types.LayerFailureCodeMissingLayerDigest {
		t.Fatalf("layer check = %+v", check)
	}
	put := func(tag string, m []byte) error {
		_, err := c.PutImage(ctx, &ecr.PutImageInput{RepositoryName: name, ImageManifest: aws.String(string(m)), ImageTag: aws.String(tag)})
		return err
	}
	man := manifest(config, layer)
	if err := put("v1", man); err != nil {
		t.Fatal(err)
	}
	var exists *types.ImageAlreadyExistsException
	var missing *types.LayersNotFoundException
	if err := put("v1", man); !errors.As(err, &exists) {
		t.Fatalf("want ImageAlreadyExistsException, got %v", err)
	}
	if err := put("v2", manifest(config2, layer)); !errors.As(err, &missing) {
		t.Fatalf("want LayersNotFoundException, got %v", err)
	}

	d := must(c.DescribeImages(ctx, &ecr.DescribeImagesInput{RepositoryName: name})).ImageDetails
	if len(d) != 1 || *d[0].ImageDigest != digest(man) || *d[0].ImageSizeInBytes != 2500 || d[0].ImagePushedAt == nil || d[0].ImageTags[0] != "v1" ||
		*d[0].ImageManifestMediaType != oci || d[0].ImageScanStatus.Status != types.ScanStatusComplete { // scanOnPush
		t.Fatalf("images = %+v", d)
	}
	got := must(c.BatchGetImage(ctx, &ecr.BatchGetImageInput{RepositoryName: name, ImageIds: []types.ImageIdentifier{{ImageTag: aws.String("v1")}, {ImageTag: aws.String("v9")}}}))
	if len(got.Images) != 1 || *got.Images[0].ImageManifest != string(man) || *got.Images[0].ImageId.ImageDigest != digest(man) || len(got.Failures) != 1 {
		t.Fatalf("batch get = %+v", got)
	}
	u := must(c.GetDownloadUrlForLayer(ctx, &ecr.GetDownloadUrlForLayerInput{RepositoryName: name, LayerDigest: aws.String(digest(layer))})).DownloadUrl
	resp := must(http.Get(*u))
	if b, _ := io.ReadAll(resp.Body); !bytes.Equal(b, layer) {
		t.Fatalf("download %s = %d bytes", *u, len(b))
	}
	resp.Body.Close()

	// an immutable tag stays; a new tag is fine
	upload(config2)
	must(c.PutImageScanningConfiguration(ctx, &ecr.PutImageScanningConfigurationInput{RepositoryName: name, ImageScanningConfiguration: &types.ImageScanningConfiguration{}}))
	must(c.PutImageTagMutability(ctx, &ecr.PutImageTagMutabilityInput{RepositoryName: name, ImageTagMutability: types.ImageTagMutabilityImmutable}))
	man2 := manifest(config2, layer)
	var immutable *types.ImageTagAlreadyExistsException
	if err := put("v1", man2); !errors.As(err, &immutable) {
		t.Fatalf("want ImageTagAlreadyExistsException, got %v", err)
	}
	if err := put("v2", man2); err != nil {
		t.Fatal(err)
	}
	if ids := must(c.ListImages(ctx, &ecr.ListImagesInput{RepositoryName: name})).ImageIds; len(ids) != 2 {
		t.Fatalf("image ids = %+v", ids)
	}

	// scans: none until started, then complete and clean
	v2 := &types.ImageIdentifier{ImageTag: aws.String("v2")}
	var noScan *types.ScanNotFoundException
	if _, err := c.DescribeImageScanFindings(ctx, &ecr.DescribeImageScanFindingsInput{RepositoryName: name, ImageId: v2}); !errors.As(err, &noScan) {
		t.Fatalf("want ScanNotFoundException, got %v", err)
	}
	must(c.StartImageScan(ctx, &ecr.StartImageScanInput{RepositoryName: name, ImageId: v2}))
	f := must(c.DescribeImageScanFindings(ctx, &ecr.DescribeImageScanFindingsInput{RepositoryName: name, ImageId: v2}))
	if f.ImageScanStatus.Status != types.ScanStatusComplete || len(f.ImageScanFindings.Findings) != 0 {
		t.Fatalf("findings = %+v", f)
	}

	// a lifecycle policy that keeps one image expires the older
	must(c.PutLifecyclePolicy(ctx, &ecr.PutLifecyclePolicyInput{RepositoryName: name,
		LifecyclePolicyText: aws.String(`{"rules":[{"rulePriority":1,"selection":{"tagStatus":"any","countType":"imageCountMoreThan","countNumber":1},"action":{"type":"expire"}}]}`)}))
	roundTrip(t, hub)
	if ids := must(c.ListImages(ctx, &ecr.ListImagesInput{RepositoryName: name})).ImageIds; len(ids) != 1 || *ids[0].ImageTag != "v2" {
		t.Fatalf("image ids after the lifecycle policy = %+v", ids)
	}

	var notEmpty *types.RepositoryNotEmptyException
	if _, err := c.DeleteRepository(ctx, &ecr.DeleteRepositoryInput{RepositoryName: name}); !errors.As(err, &notEmpty) {
		t.Fatalf("want RepositoryNotEmptyException, got %v", err)
	}
	del := must(c.BatchDeleteImage(ctx, &ecr.BatchDeleteImageInput{RepositoryName: name, ImageIds: []types.ImageIdentifier{*v2, {ImageTag: aws.String("v9")}}}))
	if len(del.ImageIds) != 1 || *del.ImageIds[0].ImageDigest != digest(man2) || len(del.Failures) != 1 || del.Failures[0].FailureCode != types.ImageFailureCodeImageNotFound {
		t.Fatalf("batch delete = %+v", del)
	}
	must(c.DeleteRepository(ctx, &ecr.DeleteRepositoryInput{RepositoryName: name}))
}
