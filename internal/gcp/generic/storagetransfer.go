package generic

import (
	"encoding/json"
	"maps"
	"slices"
	"strings"

	"github.com/nrndev/pogi/internal/gcp/gapi"
)

// Storage Transfer names a job transferJobs/<name>, with no projects/ in the path; the project is
// the projectId field or query parameter. Jobs are stored under that name.
func init() {
	apiHooks["storagetransfer"] = hooks{methods: map[string]hook{
		"storagetransfer.transferJobs.create": jobCreate,
		"storagetransfer.transferJobs.get":    jobGet,
		"storagetransfer.transferJobs.patch":  jobPatch,
		"storagetransfer.transferJobs.delete": jobDelete,
		"storagetransfer.transferJobs.list":   jobList,
	}}
}

func jobCreate(c *call) (any, error) {
	job := clone(c.a.writable("TransferJob", c.body))
	if p, _ := job["projectId"].(string); p == "" {
		return nil, gapi.Invalid("Missing required field %q in TransferJob.", "projectId")
	}
	name, _ := job["name"].(string)
	if name == "" {
		name = "transferJobs/" + c.a.numericID()
	} else if id, ok := strings.CutPrefix(name, "transferJobs/"); !ok || !idRE.MatchString(id) {
		return nil, gapi.Invalid("Invalid job name %q: want transferJobs/<name> with letters, digits, '-', '_' and '.'", name)
	}
	if c.a.objs[name] != nil {
		return nil, gapi.Exists("Resource '%s' already exists", name)
	}
	job["name"] = name
	if job["status"] == nil {
		job["status"] = "ENABLED"
	}
	job["creationTime"], job["lastModificationTime"] = now(), now()
	c.a.objs[name] = job
	return clone(job), nil
}

// job finds the job a request names; the project must be the job's own.
func (c *call) job(project string) (map[string]any, error) {
	if project == "" {
		return nil, gapi.Invalid("The query parameter projectId is required.")
	}
	job := c.a.objs[c.name()]
	if job == nil || job["projectId"] != project {
		return nil, c.notFound()
	}
	return job, nil
}

func jobGet(c *call) (any, error) {
	job, err := c.job(c.q.Get("projectId"))
	if err != nil {
		return nil, err
	}
	return clone(job), nil
}

func jobDelete(c *call) (any, error) {
	if _, err := c.job(c.q.Get("projectId")); err != nil {
		return nil, err
	}
	delete(c.a.objs, c.name())
	return map[string]any{}, nil
}

// jobPatch changes the fields of the transferJob that updateTransferJobFieldMask names (all the
// fields sent when it is empty). Status DELETED keeps the job, as the real API does.
func jobPatch(c *call) (any, error) {
	project, _ := c.body["projectId"].(string)
	job, err := c.job(project)
	if err != nil {
		return nil, err
	}
	in, _ := c.body["transferJob"].(map[string]any)
	if in == nil {
		return nil, gapi.Invalid("Missing required field %q in UpdateTransferJobRequest.", "transferJob")
	}
	var paths []string
	if mask, _ := c.body["updateTransferJobFieldMask"].(string); mask != "" {
		paths = strings.Split(mask, ",")
	} else {
		for k := range in {
			paths = append(paths, k)
		}
	}
	props := c.a.schema("TransferJob").Properties
	next := clone(job)
	for _, p := range paths {
		if props[p] == nil {
			return nil, gapi.Invalid("Invalid update mask: %q is not a field of TransferJob.", p)
		}
		if props[p].ReadOnly || p == "name" || p == "projectId" {
			continue
		}
		if v, ok := in[p]; ok {
			next[p] = v
		} else {
			delete(next, p)
		}
	}
	next["lastModificationTime"] = now()
	if next["status"] == "DELETED" {
		next["deletionTime"] = now()
	}
	c.a.objs[c.name()] = next
	return clone(next), nil
}

// jobList reads the JSON filter: projectId (required), jobNames and jobStatuses.
func jobList(c *call) (any, error) {
	var f struct {
		ProjectID             string
		JobNames, JobStatuses []string
	}
	if err := json.Unmarshal([]byte(c.q.Get("filter")), &f); err != nil || f.ProjectID == "" {
		return nil, gapi.Invalid(`The filter must be JSON with a projectId, like {"projectId":"p"}.`)
	}
	var items []map[string]any
	for _, k := range slices.Sorted(maps.Keys(c.a.objs)) {
		j := c.a.objs[k]
		status, _ := j["status"].(string)
		if !strings.HasPrefix(k, "transferJobs/") || j["projectId"] != f.ProjectID ||
			len(f.JobNames) > 0 && !slices.Contains(f.JobNames, k) ||
			len(f.JobStatuses) > 0 && !slices.Contains(f.JobStatuses, status) {
			continue
		}
		items = append(items, j)
	}
	return c.page(items)
}
