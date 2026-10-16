package generic

import (
	"strconv"
	"strings"
	"time"
)

// Bedrock control plane: the foundation model catalog, and the members the engine cannot make up
// for guardrails, provisioned throughput, custom models and the invocation logging configuration.

// foundationModels is a small catalog: id|name|input|output|streaming|inference types|customizations.
var foundationModels = []string{
	"anthropic.claude-3-haiku-20240307-v1:0|Claude 3 Haiku|TEXT,IMAGE|TEXT|S|ON_DEMAND|",
	"anthropic.claude-3-sonnet-20240229-v1:0|Claude 3 Sonnet|TEXT,IMAGE|TEXT|S|ON_DEMAND|",
	"anthropic.claude-3-5-haiku-20241022-v1:0|Claude 3.5 Haiku|TEXT|TEXT|S|INFERENCE_PROFILE|",
	"anthropic.claude-3-5-sonnet-20240620-v1:0|Claude 3.5 Sonnet|TEXT,IMAGE|TEXT|S|ON_DEMAND|",
	"anthropic.claude-3-5-sonnet-20241022-v2:0|Claude 3.5 Sonnet v2|TEXT,IMAGE|TEXT|S|INFERENCE_PROFILE|",
	"anthropic.claude-3-7-sonnet-20250219-v1:0|Claude 3.7 Sonnet|TEXT,IMAGE|TEXT|S|INFERENCE_PROFILE|",
	"anthropic.claude-sonnet-4-20250514-v1:0|Claude Sonnet 4|TEXT,IMAGE|TEXT|S|INFERENCE_PROFILE|",
	"anthropic.claude-opus-4-20250514-v1:0|Claude Opus 4|TEXT,IMAGE|TEXT|S|INFERENCE_PROFILE|",
	"amazon.titan-text-lite-v1|Titan Text G1 - Lite|TEXT|TEXT|S|ON_DEMAND|FINE_TUNING,CONTINUED_PRE_TRAINING",
	"amazon.titan-text-express-v1|Titan Text G1 - Express|TEXT|TEXT|S|ON_DEMAND|FINE_TUNING,CONTINUED_PRE_TRAINING",
	"amazon.titan-text-premier-v1:0|Titan Text G1 - Premier|TEXT|TEXT|S|ON_DEMAND|FINE_TUNING",
	"amazon.titan-embed-text-v1|Titan Embeddings G1 - Text|TEXT|EMBEDDING||ON_DEMAND|",
	"amazon.titan-embed-text-v2:0|Titan Text Embeddings V2|TEXT|EMBEDDING||ON_DEMAND|",
	"amazon.titan-image-generator-v1|Titan Image Generator G1|TEXT,IMAGE|IMAGE||ON_DEMAND|FINE_TUNING",
	"amazon.nova-micro-v1:0|Nova Micro|TEXT|TEXT|S|ON_DEMAND,INFERENCE_PROFILE|",
	"amazon.nova-lite-v1:0|Nova Lite|TEXT,IMAGE,VIDEO|TEXT|S|ON_DEMAND,INFERENCE_PROFILE|",
	"amazon.nova-pro-v1:0|Nova Pro|TEXT,IMAGE,VIDEO|TEXT|S|ON_DEMAND,INFERENCE_PROFILE|",
	"meta.llama3-8b-instruct-v1:0|Llama 3 8B Instruct|TEXT|TEXT|S|ON_DEMAND|",
	"meta.llama3-70b-instruct-v1:0|Llama 3 70B Instruct|TEXT|TEXT|S|ON_DEMAND|",
	"meta.llama3-1-8b-instruct-v1:0|Llama 3.1 8B Instruct|TEXT|TEXT|S|INFERENCE_PROFILE|",
	"meta.llama3-1-70b-instruct-v1:0|Llama 3.1 70B Instruct|TEXT|TEXT|S|INFERENCE_PROFILE|",
	"mistral.mistral-7b-instruct-v0:2|Mistral 7B Instruct|TEXT|TEXT|S|ON_DEMAND|",
	"mistral.mixtral-8x7b-instruct-v0:1|Mixtral 8x7B Instruct|TEXT|TEXT|S|ON_DEMAND|",
	"mistral.mistral-large-2402-v1:0|Mistral Large (24.02)|TEXT|TEXT|S|ON_DEMAND|",
	"mistral.mistral-large-2407-v1:0|Mistral Large (24.07)|TEXT|TEXT|S|ON_DEMAND|",
	"cohere.command-text-v14|Command|TEXT|TEXT|S|ON_DEMAND|FINE_TUNING",
	"cohere.command-light-text-v14|Command Light|TEXT|TEXT|S|ON_DEMAND|FINE_TUNING",
	"cohere.command-r-v1:0|Command R|TEXT|TEXT|S|ON_DEMAND|",
	"cohere.command-r-plus-v1:0|Command R+|TEXT|TEXT|S|ON_DEMAND|",
	"cohere.embed-english-v3|Embed English|TEXT|EMBEDDING||ON_DEMAND|",
	"cohere.embed-multilingual-v3|Embed Multilingual|TEXT|EMBEDDING||ON_DEMAND|",
}

var providers = map[string]string{"anthropic": "Anthropic", "amazon": "Amazon", "meta": "Meta", "mistral": "Mistral AI", "cohere": "Cohere"}

func foundationModel(c *Call, line string) map[string]any {
	f := strings.Split(line, "|")
	list := func(s string) []any {
		out := []any{}
		for _, x := range strings.Split(s, ",") {
			if x != "" {
				out = append(out, x)
			}
		}
		return out
	}
	return map[string]any{
		"modelArn": aiFoundationModelARN(c, f[0]), "modelId": f[0], "modelName": f[1], "providerName": providers[strings.SplitN(f[0], ".", 2)[0]],
		"inputModalities": list(f[2]), "outputModalities": list(f[3]), "responseStreamingSupported": f[4] == "S",
		"inferenceTypesSupported": list(f[5]), "customizationsSupported": list(f[6]),
		"modelLifecycle": map[string]any{"status": "ACTIVE"},
	}
}

func hasAny(have any, want string) bool {
	for _, h := range asList(have) {
		if strings.EqualFold(str(h), want) {
			return true
		}
	}
	return false
}

func init() {
	Override("bedrock", "ListFoundationModels", func(c *Call) (map[string]any, error) {
		out := []any{}
		for _, line := range foundationModels {
			m := foundationModel(c, line)
			p, o, i, u := str(c.In["byProvider"]), str(c.In["byOutputModality"]), str(c.In["byInferenceType"]), str(c.In["byCustomizationType"])
			if p != "" && !strings.EqualFold(p, str(m["providerName"])) || o != "" && !hasAny(m["outputModalities"], o) ||
				i != "" && !hasAny(m["inferenceTypesSupported"], i) || u != "" && !hasAny(m["customizationsSupported"], u) {
				continue
			}
			out = append(out, m)
		}
		return map[string]any{"modelSummaries": out}, nil
	})
	Override("bedrock", "GetFoundationModel", func(c *Call) (map[string]any, error) {
		id := str(c.In["modelIdentifier"])
		if i := strings.LastIndex(id, "foundation-model/"); i >= 0 {
			id = id[i+len("foundation-model/"):]
		}
		for _, line := range foundationModels {
			if strings.HasPrefix(line, id+"|") {
				return map[string]any{"modelDetails": foundationModel(c, line)}, nil
			}
		}
		return nil, c.NotFound("Could not resolve the foundation model from the provided model identifier.")
	})

	// Guardrails start as the DRAFT; CreateGuardrailVersion freezes a numbered copy of it.
	aiSeed("bedrock", "CreateGuardrail", func(c *Call, r *Resource) { r.Data["version"] = "DRAFT" })
	Override("bedrock", "CreateGuardrailVersion", func(c *Call) (map[string]any, error) {
		r := aiFind(c, "Guardrail", "guardrailIdentifier")
		if r == nil {
			return nil, c.NotFound("Guardrail %s not found", str(c.In["guardrailIdentifier"]))
		}
		n, _ := strconv.Atoi(str(r.Data["latestVersion"]))
		r.Data["latestVersion"] = strconv.Itoa(n + 1)
		c.Put(r)
		return map[string]any{"guardrailId": r.ID, "version": strconv.Itoa(n + 1)}, nil
	})

	// Provisioned throughput is in service at once; the id is its ARN's tail, the name works too.
	aiSeed("bedrock", "CreateProvisionedModelThroughput", func(c *Call, r *Resource) {
		aiRekey(c, r, randHex(12))
		r.Name = str(c.In["provisionedModelName"])
		arn := aiFoundationModelARN(c, str(c.In["modelId"]))
		for k, v := range map[string]any{"provisionedModelArn": r.ARN, "modelArn": arn, "desiredModelArn": arn, "foundationModelArn": arn,
			"desiredModelUnits": c.In["modelUnits"], "status": "InService"} {
			r.Data[k] = v
		}
	})
	aiPatch("bedrock", "UpdateProvisionedModelThroughput", func(c *Call, r *Resource) {
		if n := str(c.In["desiredProvisionedModelName"]); n != "" {
			r.Data["provisionedModelName"], r.Name = n, n
		}
		if m := str(c.In["desiredModelId"]); m != "" {
			arn := aiFoundationModelARN(c, m)
			r.Data["modelArn"], r.Data["desiredModelArn"] = arn, arn
		}
	})

	// A customization job finishes at once and leaves a custom model behind.
	aiSeed("bedrock", "CreateModelCustomizationJob", func(c *Call, r *Resource) {
		base := str(c.In["baseModelIdentifier"])
		name := str(c.In["customModelName"])
		model := aiNewResource(c, "CustomModel", name, "", map[string]any{"modelName": name, "modelStatus": "Active", "jobArn": r.ARN, "jobName": r.Name,
			"baseModelArn": aiFoundationModelARN(c, base), "customizationType": aiFirstNonEmpty(str(c.In["customizationType"]), "FINE_TUNING"),
			"hyperParameters": c.In["hyperParameters"], "trainingDataConfig": c.In["trainingDataConfig"], "outputDataConfig": c.In["outputDataConfig"],
			"validationDataConfig": c.In["validationDataConfig"], "ownerAccountId": c.Account})
		model.ARN = c.arnOf("custom-model/"+base+"/"+model.ID, nil)
		model.Data["modelArn"] = model.ARN
		if tags := tagMap(c.In["customModelTags"]); len(tags) > 0 {
			model.Tags = tags
		}
		c.Put(model)
		now := time.Now().UTC().Format(time.RFC3339)
		for k, v := range map[string]any{"status": "Completed", "outputModelName": name, "outputModelArn": model.ARN, "baseModelArn": model.Data["baseModelArn"],
			"endTime": now, "lastModifiedTime": now} {
			r.Data[k] = v
		}
	})
	aiSeed("bedrock", "CreateCustomModel", func(c *Call, r *Resource) {
		r.Data["modelArn"], r.Data["modelName"], r.Data["modelStatus"] = r.ARN, r.Name, "Active"
	})
	aiSeed("bedrock", "CreateInferenceProfile", func(c *Call, r *Resource) {
		r.Data["status"], r.Data["type"], r.Data["inferenceProfileArn"] = "ACTIVE", "APPLICATION", r.ARN
		if from := str(aiAmap(c.In["modelSource"])["copyFrom"]); from != "" {
			r.Data["models"] = []any{map[string]any{"modelArn": from}}
		}
	})

	// The invocation logging configuration is one per account and region, and Get on none is empty.
	Override("bedrock", "PutModelInvocationLoggingConfiguration", func(c *Call) (map[string]any, error) {
		now := time.Now().UTC()
		c.Put(&Resource{Type: "ModelInvocationLoggingConfiguration", Key: "default", Account: c.Account, Region: c.Region,
			Data: map[string]any{"loggingConfig": c.In["loggingConfig"]}, Created: now, Updated: now})
		return map[string]any{}, nil
	})
	Override("bedrock", "GetModelInvocationLoggingConfiguration", func(c *Call) (map[string]any, error) {
		if r := c.Get("ModelInvocationLoggingConfiguration", "default"); r != nil {
			return map[string]any{"loggingConfig": r.Data["loggingConfig"]}, nil
		}
		return map[string]any{}, nil
	})
	Override("bedrock", "DeleteModelInvocationLoggingConfiguration", func(c *Call) (map[string]any, error) {
		c.Svc.Delete(c.Account, c.Region, "ModelInvocationLoggingConfiguration", "default")
		return map[string]any{}, nil
	})
}

func aiFirstNonEmpty(ss ...string) string {
	for _, s := range ss {
		if s != "" {
			return s
		}
	}
	return ""
}
