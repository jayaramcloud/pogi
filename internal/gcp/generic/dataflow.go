package generic

import (
	"fmt"
	"strings"

	"github.com/nrndev/pogi/internal/gcp/gapi"
)

// Dataflow starts a job from a template with templates.create (what google_dataflow_job calls). The
// job is kept at projects/p/locations/l/jobs/<id>, running at once; nothing runs behind it. A job
// update that asks for CANCELLED or DRAINED ends the job.
func init() {
	create := func(c *call) (any, error) {
		project := c.segs[1]
		loc := "us-central1"
		if len(c.segs) >= 4 && c.segs[2] == "locations" {
			loc = c.segs[3]
		} else if l, _ := c.body["location"].(string); l != "" {
			loc = l
		}
		name, _ := c.body["jobName"].(string)
		if name == "" || c.body["gcsPath"] == nil {
			return nil, gapi.Invalid("Missing required field %q in %s.", map[bool]string{true: "jobName", false: "gcsPath"}[name == ""], c.m.req)
		}
		if !strings.HasPrefix(fmt.Sprint(c.body["gcsPath"]), "gs://") {
			return nil, gapi.Invalid("The template path %q is not a Cloud Storage path (gs://...).", c.body["gcsPath"])
		}
		prefix := "projects/" + project + "/locations/" + loc + "/jobs/"
		for k, j := range c.a.objs {
			if strings.HasPrefix(k, prefix) && j["name"] == name && j["currentState"] == "JOB_STATE_RUNNING" {
				return nil, gapi.Exists("There is already an active job named %s.", name)
			}
		}
		id := now()[:10] + "_00_00_00-" + c.a.numericID()
		job := map[string]any{"id": id, "projectId": project, "name": name, "location": loc,
			"type": "JOB_TYPE_BATCH", "currentState": "JOB_STATE_RUNNING", "currentStateTime": now(), "createTime": now(),
			"environment": c.body["environment"]}
		if job["environment"] == nil {
			delete(job, "environment")
		}
		// the template's parameters belong to the run, not to the job: they are not kept
		c.a.objs[prefix+id] = job
		return clone(job), nil
	}
	apiHooks["dataflow"] = hooks{
		methods: map[string]hook{
			"dataflow.projects.locations.templates.create": create,
			"dataflow.projects.templates.create":           create,
		},
		check: map[string]func(*call, map[string]any) error{"Job": func(_ *call, job map[string]any) error {
			switch s, _ := job["requestedState"].(string); s {
			case "JOB_STATE_CANCELLED", "JOB_STATE_DRAINED":
				job["currentState"], job["currentStateTime"] = s, now()
			}
			return nil
		}},
	}
}
