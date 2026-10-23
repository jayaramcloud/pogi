package generic_test

import (
	"context"
	"regexp"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/imagebuilder"
	"github.com/aws/aws-sdk-go-v2/service/imagebuilder/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestImageBuilder(t *testing.T) {
	_, url := start(t, "imagebuilder")
	ctx := context.Background()
	c := imagebuilder.NewFromConfig(awstest.Config(t, url))

	// failures first
	if _, err := c.CreateImageRecipe(ctx, &imagebuilder.CreateImageRecipeInput{Name: aws.String("r")}); err == nil {
		t.Fatal("recipe without a version succeeded")
	}
	_, err := c.GetImageRecipe(ctx, &imagebuilder.GetImageRecipeInput{ImageRecipeArn: aws.String("arn:aws:imagebuilder:us-east-1:000000000000:image-recipe/none/1.0.0")})
	kCode(t, err, "ResourceNotFoundException")

	comp, err := c.CreateComponent(ctx, &imagebuilder.CreateComponentInput{
		Name: aws.String("hello"), SemanticVersion: aws.String("1.0.0"), Platform: types.PlatformLinux,
		Data: aws.String("name: hello\nschemaVersion: 1.0\nphases: []\n"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`^arn:aws:imagebuilder:us-east-1:000000000000:component/hello/1\.0\.0/\d+$`).MatchString(aws.ToString(comp.ComponentBuildVersionArn)) {
		t.Fatalf("component arn %s", aws.ToString(comp.ComponentBuildVersionArn))
	}
	gc, err := c.GetComponent(ctx, &imagebuilder.GetComponentInput{ComponentBuildVersionArn: comp.ComponentBuildVersionArn})
	if err != nil || aws.ToString(gc.Component.Version) != "1.0.0" || gc.Component.Type != types.ComponentTypeBuild {
		t.Fatalf("component: %v %+v", err, gc.Component)
	}
	rec, err := c.CreateImageRecipe(ctx, &imagebuilder.CreateImageRecipeInput{
		Name: aws.String("r"), SemanticVersion: aws.String("1.0.0"),
		ParentImage: aws.String("arn:aws:imagebuilder:us-east-1:aws:image/amazon-linux-2023-x86/x.x.x"),
		Components:  []types.ComponentConfiguration{{ComponentArn: comp.ComponentBuildVersionArn}},
		Tags:        map[string]string{"env": "dev"},
	})
	if err != nil {
		t.Fatal(err)
	}
	rarn := aws.ToString(rec.ImageRecipeArn)
	if !regexp.MustCompile(`^arn:aws:imagebuilder:us-east-1:000000000000:image-recipe/r/1\.0\.0$`).MatchString(rarn) {
		t.Fatalf("recipe arn %s", rarn)
	}
	if _, err := c.CreateImageRecipe(ctx, &imagebuilder.CreateImageRecipeInput{Name: aws.String("r"), SemanticVersion: aws.String("1.0.0"), ParentImage: aws.String("x")}); err == nil {
		t.Fatal("duplicate recipe succeeded")
	}
	infra, err := c.CreateInfrastructureConfiguration(ctx, &imagebuilder.CreateInfrastructureConfigurationInput{
		Name: aws.String("infra"), InstanceProfileName: aws.String("p"), InstanceTypes: []string{"t3.micro"},
	})
	if err != nil {
		t.Fatal(err)
	}
	p, err := c.CreateImagePipeline(ctx, &imagebuilder.CreateImagePipelineInput{
		Name: aws.String("pipe"), ImageRecipeArn: rec.ImageRecipeArn, InfrastructureConfigurationArn: infra.InfrastructureConfigurationArn,
		Status: types.PipelineStatusDisabled,
	})
	if err != nil {
		t.Fatal(err)
	}
	g, err := c.GetImagePipeline(ctx, &imagebuilder.GetImagePipelineInput{ImagePipelineArn: p.ImagePipelineArn})
	if err != nil || aws.ToString(g.ImagePipeline.Name) != "pipe" || g.ImagePipeline.Status != types.PipelineStatusDisabled ||
		aws.ToString(g.ImagePipeline.ImageRecipeArn) != rarn {
		t.Fatalf("pipeline: %v %+v", err, g)
	}
	l, err := c.ListImagePipelines(ctx, &imagebuilder.ListImagePipelinesInput{})
	if err != nil || len(l.ImagePipelineList) != 1 {
		t.Fatalf("list: %v %+v", err, l)
	}
	tg, err := c.ListTagsForResource(ctx, &imagebuilder.ListTagsForResourceInput{ResourceArn: rec.ImageRecipeArn})
	if err != nil || tg.Tags["env"] != "dev" {
		t.Fatalf("tags: %v %+v", err, tg)
	}
	if _, err := c.DeleteImagePipeline(ctx, &imagebuilder.DeleteImagePipelineInput{ImagePipelineArn: p.ImagePipelineArn}); err != nil {
		t.Fatal(err)
	}
	_, err = c.GetImagePipeline(ctx, &imagebuilder.GetImagePipelineInput{ImagePipelineArn: p.ImagePipelineArn})
	kCode(t, err, "ResourceNotFoundException")
	if _, err := c.DeleteImageRecipe(ctx, &imagebuilder.DeleteImageRecipeInput{ImageRecipeArn: rec.ImageRecipeArn}); err != nil {
		t.Fatal(err)
	}
}
