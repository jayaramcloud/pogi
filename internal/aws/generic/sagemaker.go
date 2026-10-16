package generic

import (
	"time"
)

// SageMaker: metadata only. Endpoints, notebook instances and domains take a moment to leave their
// first state, training jobs are completed at once, and nothing runs.

// aiSettle moves resources of a type to their next status (next, by current status) once they have waited for delay,
// whenever one of ops is called.
func aiSettle(service, typ, field string, next map[string]string, delay time.Duration, ops ...string) {
	for _, op := range ops {
		Override(service, op, func(c *Call) (map[string]any, error) {
			for _, r := range c.List(typ) {
				if to, ok := next[str(r.Data[field])]; ok && time.Since(r.Updated) >= delay {
					r.Data[field] = to
					c.Put(r)
				}
			}
			return c.Default()
		})
	}
}

const settleAfter = 500 * time.Millisecond

func init() {
	const svc = "sagemaker"

	// A configuration needs its models, and an endpoint its configuration; SageMaker says
	// ValidationException "Could not aiFind ..." for these.
	Override(svc, "CreateEndpointConfig", func(c *Call) (map[string]any, error) {
		for _, v := range asList(c.In["ProductionVariants"]) {
			if m := str(aiAmap(v)["ModelName"]); m != "" && !aiHasResource(c, "Model", m) {
				return nil, c.NotFound("Could not aiFind model \"%s\".", m)
			}
		}
		return c.Default()
	})
	aiSeed(svc, "CreateEndpoint", func(c *Call, r *Resource) {
		r.Data["EndpointStatus"], r.Data["ProductionVariants"] = "Creating", endpointVariants(c, str(c.In["EndpointConfigName"]))
	}, func(c *Call) error {
		if n := str(c.In["EndpointConfigName"]); !aiHasResource(c, "EndpointConfig", n) {
			return c.NotFound("Could not aiFind endpoint configuration \"%s\".", n)
		}
		return nil
	})
	aiPatch(svc, "UpdateEndpoint", func(c *Call, r *Resource) {
		r.Data["EndpointStatus"], r.Data["ProductionVariants"] = "Updating", endpointVariants(c, str(c.In["EndpointConfigName"]))
	})
	aiSettle(svc, "Endpoint", "EndpointStatus", map[string]string{"Creating": "InService", "Updating": "InService"}, settleAfter, "DescribeEndpoint", "ListEndpoints")

	aiSeed(svc, "CreateTrainingJob", func(c *Call, r *Resource) {
		out := str(aiAmap(c.In["OutputDataConfig"])["S3OutputPath"])
		now := r.Created.Format(time.RFC3339)
		for k, v := range map[string]any{"TrainingJobStatus": "Completed", "SecondaryStatus": "Completed", "TrainingStartTime": now, "TrainingEndTime": now,
			"TrainingTimeInSeconds": 1.0, "BillableTimeInSeconds": 1.0,
			"ModelArtifacts": map[string]any{"S3ModelArtifacts": out + "/" + r.Name + "/output/model.tar.gz"}} {
			r.Data[k] = v
		}
	})

	aiSeed(svc, "CreateNotebookInstance", func(c *Call, r *Resource) {
		r.Data["NotebookInstanceStatus"] = "Pending"
		r.Data["Url"] = r.Name + ".notebook." + c.Region + ".sagemaker.aws"
	})
	aiSettle(svc, "NotebookInstance", "NotebookInstanceStatus", map[string]string{"Pending": "InService"}, settleAfter, "DescribeNotebookInstance", "ListNotebookInstances")
	aiPatch(svc, "StopNotebookInstance", func(c *Call, r *Resource) { r.Data["NotebookInstanceStatus"] = "Stopped" })
	aiPatch(svc, "StartNotebookInstance", func(c *Call, r *Resource) { r.Data["NotebookInstanceStatus"] = "InService" })

	aiSeed(svc, "CreateDomain", func(c *Call, r *Resource) {
		aiRekey(c, r, "d-"+randHex(12))
		r.Data["DomainId"], r.Data["DomainArn"] = r.ID, r.ARN
		r.Data["Status"], r.Data["Url"] = "Pending", "https://"+r.ID+".studio."+c.Region+".sagemaker.aws"
	})
	aiSettle(svc, "Domain", "Status", map[string]string{"Pending": "InService"}, settleAfter, "DescribeDomain", "ListDomains")
}

func aiHasResource(c *Call, typ, name string) bool {
	for _, r := range c.List(typ) {
		if r.Name == name || r.ID == name || r.ARN == name {
			return true
		}
	}
	return false
}

// endpointVariants is what DescribeEndpoint says about the variants of an endpoint's configuration.
func endpointVariants(c *Call, config string) []any {
	var out []any
	if cfg := c.Get("EndpointConfig", config); cfg != nil {
		for _, v := range asList(cfg.Data["ProductionVariants"]) {
			vm := aiAmap(v)
			n := aiNum(vm["InitialInstanceCount"])
			w := 1.0
			if f, ok := vm["InitialVariantWeight"].(float64); ok {
				w = f
			}
			out = append(out, map[string]any{"VariantName": vm["VariantName"], "DeployedImages": []any{}, "CurrentWeight": w, "DesiredWeight": w,
				"CurrentInstanceCount": n, "DesiredInstanceCount": n})
		}
	}
	return out
}
