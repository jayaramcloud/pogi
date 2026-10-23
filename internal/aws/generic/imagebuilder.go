package generic

import "strings"

// EC2 Image Builder: every resource is named by its ARN, which carries the name and the version
// (image-recipe/<name>/<version>, component/<name>/<version>/<build>). The engine would make up a
// random one, which Terraform refuses, so each create call builds the real shape. A pipeline never
// runs and no image is built.

func init() {
	byName := func(kind string) func(*Call) string { // kind/<Name>
		return func(c *Call) string { return kind + "/" + str(c.In["name"]) }
	}
	versioned := func(kind, tail string) func(*Call) string { // kind/<Name>/<SemanticVersion>[/tail]
		return func(c *Call) string {
			return kind + "/" + str(c.In["name"]) + "/" + str(c.In["semanticVersion"]) + tail
		}
	}
	for op, a := range map[string]struct {
		member string
		path   func(*Call) string
	}{
		"CreateComponent":                   {"componentBuildVersionArn", versioned("component", "/1")},
		"CreateImageRecipe":                 {"imageRecipeArn", versioned("image-recipe", "")},
		"CreateContainerRecipe":             {"containerRecipeArn", versioned("container-recipe", "")},
		"CreateInfrastructureConfiguration": {"infrastructureConfigurationArn", byName("infrastructure-configuration")},
		"CreateDistributionConfiguration":   {"distributionConfigurationArn", byName("distribution-configuration")},
		"CreateImagePipeline":               {"imagePipelineArn", byName("image-pipeline")},
		"CreateLifecyclePolicy":             {"lifecyclePolicyArn", byName("lifecycle-policy")},
		"CreateWorkflow": {"workflowBuildVersionArn", func(c *Call) string {
			return "workflow/" + strings.ToLower(str(c.In["type"])) + "/" + str(c.In["name"]) + "/" + str(c.In["semanticVersion"]) + "/1"
		}},
	} {
		Override("imagebuilder", op, func(c *Call) (map[string]any, error) {
			c.In[a.member] = c.arnOf(a.path(c), nil)
			return c.Default()
		})
	}
	// Reads return the version as `version` (it is sent as semanticVersion), a date, and a type for a component.
	for _, typ := range []string{"Component", "ImageRecipe", "ContainerRecipe", "Workflow"} {
		Derive("imagebuilder", typ, func(r *Resource) map[string]any {
			out := map[string]any{"version": r.Data["semanticVersion"], "dateCreated": r.Created.Format(timeFmt)}
			if str(r.Data["type"]) == "" && r.Type != "ImageRecipe" && r.Type != "ContainerRecipe" {
				out["type"] = "BUILD"
			}
			return out
		})
	}
	// An image is named after its recipe: image/<name>/<version>/<build>.
	Override("imagebuilder", "CreateImage", func(c *Call) (map[string]any, error) {
		for _, k := range []string{"imageRecipeArn", "containerRecipeArn"} {
			if _, rest, ok := strings.Cut(str(c.In[k]), ":"+strings.ReplaceAll(strings.TrimSuffix(k, "Arn"), "R", "-r")+"/"); ok {
				c.In["imageBuildVersionArn"] = c.arnOf("image/"+rest+"/1", nil)
			}
		}
		return c.Default()
	})
}
