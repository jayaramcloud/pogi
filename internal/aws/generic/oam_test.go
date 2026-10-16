package generic_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/oam"
	"github.com/aws/aws-sdk-go-v2/service/oam/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestOAM(t *testing.T) {
	_, url := start(t, "oam")
	ctx := context.Background()
	c := oam.NewFromConfig(awstest.Config(t, url))

	s, err := c.CreateSink(ctx, &oam.CreateSinkInput{Name: aws.String("central"), Tags: map[string]string{"env": "dev"}})
	if err != nil {
		t.Fatal(err)
	}
	if aws.ToString(s.Name) != "central" || !strings.Contains(aws.ToString(s.Arn), ":sink/") || aws.ToString(s.Id) == "" {
		t.Fatalf("sink: %+v", s)
	}
	if _, err := c.CreateSink(ctx, &oam.CreateSinkInput{Name: aws.String("central")}); err == nil {
		t.Fatal("a second sink with the same name")
	}
	g, err := c.GetSink(ctx, &oam.GetSinkInput{Identifier: s.Arn})
	if err != nil || aws.ToString(g.Name) != "central" || g.Tags["env"] != "dev" {
		t.Fatalf("get sink: %v %+v", err, g)
	}
	if _, err := c.GetSink(ctx, &oam.GetSinkInput{Identifier: s.Id}); err != nil {
		t.Fatalf("get sink by id: %v", err)
	}
	if _, err := c.PutSinkPolicy(ctx, &oam.PutSinkPolicyInput{SinkIdentifier: s.Arn, Policy: aws.String(`{"Version":"2012-10-17"}`)}); err != nil {
		t.Fatal(err)
	}
	p, err := c.GetSinkPolicy(ctx, &oam.GetSinkPolicyInput{SinkIdentifier: s.Arn})
	if err != nil || aws.ToString(p.Policy) != `{"Version":"2012-10-17"}` {
		t.Fatalf("policy: %v %+v", err, p)
	}
	if _, err := c.CreateLink(ctx, &oam.CreateLinkInput{LabelTemplate: aws.String("x"), ResourceTypes: []types.ResourceType{types.ResourceTypeAwsLogsLoggroup}, SinkIdentifier: aws.String("arn:aws:oam:us-east-1:000000000000:sink/nope")}); err == nil {
		t.Fatal("a link to a sink that does not exist")
	}
	l, err := c.CreateLink(ctx, &oam.CreateLinkInput{LabelTemplate: aws.String("$AccountName"), ResourceTypes: []types.ResourceType{types.ResourceTypeAwsLogsLoggroup}, SinkIdentifier: s.Arn})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(aws.ToString(l.Arn), ":link/") || aws.ToString(l.Label) != "000000000000" {
		t.Fatalf("link: %+v", l)
	}
	gl, err := c.GetLink(ctx, &oam.GetLinkInput{Identifier: l.Arn})
	if err != nil || aws.ToString(gl.SinkArn) != aws.ToString(s.Arn) || aws.ToString(gl.LabelTemplate) != "$AccountName" {
		t.Fatalf("get link: %v %+v", err, gl)
	}
	ul, err := c.UpdateLink(ctx, &oam.UpdateLinkInput{Identifier: l.Arn, ResourceTypes: []types.ResourceType{types.ResourceTypeAwsLogsLoggroup, types.ResourceTypeAwsCloudwatchMetric}})
	if err != nil || len(ul.ResourceTypes) != 2 {
		t.Fatalf("update link: %v %+v", err, ul)
	}
	ls, err := c.ListSinks(ctx, &oam.ListSinksInput{})
	if err != nil || len(ls.Items) != 1 {
		t.Fatalf("list sinks: %v %+v", err, ls)
	}
	ll, err := c.ListLinks(ctx, &oam.ListLinksInput{})
	if err != nil || len(ll.Items) != 1 {
		t.Fatalf("list links: %v %+v", err, ll)
	}
	if _, err := c.TagResource(ctx, &oam.TagResourceInput{ResourceArn: s.Arn, Tags: map[string]string{"a": "b"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.UntagResource(ctx, &oam.UntagResourceInput{ResourceArn: s.Arn, TagKeys: []string{"env"}}); err != nil {
		t.Fatal(err)
	}
	tg, err := c.ListTagsForResource(ctx, &oam.ListTagsForResourceInput{ResourceArn: s.Arn})
	if err != nil || len(tg.Tags) != 1 || tg.Tags["a"] != "b" {
		t.Fatalf("tags: %v %+v", err, tg)
	}
	if _, err := c.DeleteLink(ctx, &oam.DeleteLinkInput{Identifier: l.Arn}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteSink(ctx, &oam.DeleteSinkInput{Identifier: s.Arn}); err != nil {
		t.Fatal(err)
	}
	var nf *types.ResourceNotFoundException
	if _, err = c.GetSink(ctx, &oam.GetSinkInput{Identifier: s.Arn}); !errors.As(err, &nf) {
		t.Fatalf("not found: %T %v", err, err)
	}
	if _, err = c.GetLink(ctx, &oam.GetLinkInput{Identifier: l.Arn}); !errors.As(err, &nf) {
		t.Fatalf("link not found: %T %v", err, err)
	}
}
