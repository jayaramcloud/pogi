package generic

import (
	"slices"
	"strconv"

	"github.com/nrndev/pogi/internal/core"
)

// EMR Serverless: applications go CREATING, CREATED (STARTING, STARTED and
// STOPPING, STOPPED on start and stop); a job run goes SUBMITTED, RUNNING,
// SUCCESS. Metadata only, no job runs.
func init() {
	Derive("emr-serverless", "Application", func(r *Resource) map[string]any {
		var s string
		switch r.Data["_phase"] {
		case "start":
			s = Step(r, "app", "STARTING", "STARTED")
		case "stop":
			s = Step(r, "app", "STOPPING", "STOPPED")
		default:
			s = Step(r, "app", "CREATING", "CREATED")
		}
		return map[string]any{"state": s}
	})
	for _, op := range []string{"StartApplication", "StopApplication"} {
		Override("emr-serverless", op, func(c *Call) (map[string]any, error) {
			r := c.Get("Application", str(c.In["applicationId"]))
			if r == nil {
				return nil, c.NotFound("Application %s does not exist.", str(c.In["applicationId"]))
			}
			r.Data["_phase"] = map[string]string{"StartApplication": "start", "StopApplication": "stop"}[c.Op]
			Begin(r, "app")
			c.Put(r)
			return map[string]any{}, nil
		})
	}
	Override("emr-serverless", "DeleteApplication", func(c *Call) (map[string]any, error) {
		out, err := c.Default()
		if err != nil {
			return nil, err
		}
		for _, j := range c.List("JobRun") {
			if j.IDs["applicationId"] == c.Res.IDs["applicationId"] {
				c.Svc.Delete(c.Account, c.Region, "JobRun", j.Key)
			}
		}
		return out, nil
	})

	Override("emr-serverless", "StartJobRun", func(c *Call) (map[string]any, error) {
		app := c.Get("Application", str(c.In["applicationId"]))
		if app == nil {
			return nil, c.NotFound("Application %s does not exist.", str(c.In["applicationId"]))
		}
		now := c.now()
		id := randHex(17)
		data := map[string]any{}
		for k, v := range c.In {
			if k != "tags" {
				data[k] = v
			}
		}
		data["releaseLabel"] = app.Data["releaseLabel"]
		arn := c.arnOf("/applications/"+app.ID+"/jobruns/"+id, nil)
		tags := tagMap(c.In["tags"])
		if len(tags) == 0 {
			tags = nil
		}
		c.Put(&Resource{Type: "JobRun", Key: app.ID + "/" + id, IDs: map[string]string{"applicationId": app.ID, "jobRunId": id}, ID: id, ARN: arn,
			Account: c.Account, Region: c.Region, Data: data, Tags: tags, Created: now, Updated: now})
		return map[string]any{"applicationId": app.ID, "jobRunId": id, "arn": arn}, nil
	})
	Override("emr-serverless", "GetJobRun", func(c *Call) (map[string]any, error) {
		r := c.Get("JobRun", str(c.In["applicationId"]), str(c.In["jobRunId"]))
		if r == nil {
			return nil, c.NotFound("Job run %s does not exist.", str(c.In["jobRunId"]))
		}
		return map[string]any{"jobRun": esJobRun(r, false)}, nil
	})
	Override("emr-serverless", "ListJobRuns", func(c *Call) (map[string]any, error) {
		if c.Get("Application", str(c.In["applicationId"])) == nil {
			return nil, c.NotFound("Application %s does not exist.", str(c.In["applicationId"]))
		}
		states := asList(c.In["states"])
		var runs []any
		for _, r := range c.List("JobRun") {
			if r.IDs["applicationId"] != str(c.In["applicationId"]) {
				continue
			}
			jr := esJobRun(r, true)
			if len(states) == 0 || slices.Contains(states, jr["state"]) {
				runs = append(runs, jr)
			}
		}
		off, _ := strconv.Atoi(str(c.In["nextToken"]))
		off = min(max(off, 0), len(runs))
		page, next := runs[off:], ""
		if n, _ := strconv.Atoi(str(c.In["maxResults"])); n > 0 && off+n < len(runs) {
			page, next = runs[off:off+n], strconv.Itoa(off+n)
		}
		out := map[string]any{"jobRuns": orEmpty(page)}
		if next != "" {
			out["nextToken"] = next
		}
		return out, nil
	})
	Override("emr-serverless", "CancelJobRun", func(c *Call) (map[string]any, error) {
		r := c.Get("JobRun", str(c.In["applicationId"]), str(c.In["jobRunId"]))
		if r == nil {
			return nil, c.NotFound("Job run %s does not exist.", str(c.In["jobRunId"]))
		}
		if st := esJobState(r); st == "SUCCESS" || st == "FAILED" || st == "CANCELLED" {
			return nil, c.Invalid("Job run %s is already in state %s.", r.ID, st)
		}
		r.Data["_phase"] = "cancel"
		Begin(r, "job")
		c.Put(r)
		return map[string]any{"applicationId": r.IDs["applicationId"], "jobRunId": r.ID}, nil
	})
}

func esJobState(r *Resource) string {
	if r.Data["_phase"] == "cancel" {
		return Step(r, "job", "CANCELLING", "CANCELLED")
	}
	return Step(r, "job", "SUBMITTED", "RUNNING", "SUCCESS")
}

// esJobRun is a job run as GetJobRun shows it, or as ListJobRuns summarises it.
func esJobRun(r *Resource, summary bool) map[string]any {
	st := esJobState(r)
	out := map[string]any{"applicationId": r.IDs["applicationId"], "arn": r.ARN, "createdAt": r.Created.Format(timeFmt), "updatedAt": r.Updated.Format(timeFmt),
		"executionRole": r.Data["executionRoleArn"], "state": st, "stateDetails": "", "releaseLabel": r.Data["releaseLabel"], "createdBy": core.ARN("iam", "", r.Account, "root")}
	if n := r.Data["name"]; n != nil {
		out["name"] = n
	}
	if summary {
		out["id"] = r.ID
		return out
	}
	out["jobRunId"] = r.ID
	for _, k := range []string{"jobDriver", "configurationOverrides", "executionTimeoutMinutes"} {
		if v := r.Data[k]; v != nil {
			out[k] = v
		}
	}
	if len(r.Tags) > 0 {
		tags := map[string]any{}
		for k, v := range r.Tags {
			tags[k] = v
		}
		out["tags"] = tags
	}
	if st == "SUCCESS" {
		out["totalResourceUtilization"] = map[string]any{"vCPUHour": 0.01, "memoryGBHour": 0.04, "storageGBHour": 0.0}
		out["totalExecutionDurationSeconds"] = 1.0
	}
	return out
}
