package generic

import (
	"bytes"
	"path"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/aws/s3io"
)

// DataSync: locations of every kind are stored (S3 is the one with a real bucket behind it); a task
// execution copies the objects between two S3 locations, with include and exclude filters, and reports
// its status and the files and bytes it moved.
//
// ponytail: an execution runs inside StartTaskExecution and has finished when it returns (SUCCESS or ERROR),
// so there is nothing to cancel and no QUEUED or TRANSFERRING to poll; NFS, SMB, EFS and the other
// locations are metadata only, an execution that touches one ends in ERROR; folders and object
// metadata (mtime, tags, permissions) are not copied.
func init() {
	for _, k := range []string{"S3", "Efs", "FsxLustre", "FsxOntap", "FsxOpenZfs", "FsxWindows", "Hdfs", "Nfs", "ObjectStorage", "Smb", "AzureBlob"} {
		Override("datasync", "CreateLocation"+k, func(c *Call) (map[string]any, error) {
			data := s2Clone(c.In)
			delete(data, "Tags")
			data["_kind"] = k
			id := "loc-" + randHex(17)
			arn := c.arnOf("location/"+id, nil)
			data["LocationUri"] = dsURI(c, k, data)
			c.Res = s2Store(c, "Location", arn, id, arn, data, "LocationArn")
			s2Tags(c, c.Res)
			return map[string]any{"LocationArn": arn}, nil
		})
		Override("datasync", "DescribeLocation"+k, func(c *Call) (map[string]any, error) {
			r, err := dsLocation(c, str(c.In["LocationArn"]), k)
			if err != nil {
				return nil, err
			}
			out := c.Fill(r)
			out["LocationArn"], out["LocationUri"], out["CreationTime"] = r.ARN, r.Data["LocationUri"], s2Time(r.Created)
			return out, nil
		})
		Override("datasync", "UpdateLocation"+k, func(c *Call) (map[string]any, error) {
			r, err := dsLocation(c, str(c.In["LocationArn"]), k)
			if err != nil {
				return nil, err
			}
			for m, v := range c.In {
				if m != "LocationArn" {
					r.Data[m] = v
				}
			}
			r.Data["LocationUri"] = dsURI(c, k, r.Data)
			r.Updated = time.Now().UTC()
			c.Put(r)
			return map[string]any{}, nil
		})
	}
	Override("datasync", "DeleteLocation", func(c *Call) (map[string]any, error) {
		r, err := dsLocation(c, str(c.In["LocationArn"]), "")
		if err != nil {
			return nil, err
		}
		c.Svc.Delete(c.Account, c.Region, "Location", r.Key)
		return map[string]any{}, nil
	})
	Override("datasync", "ListLocations", func(c *Call) (map[string]any, error) {
		var items []any
		for _, r := range c.List("Location") {
			items = append(items, map[string]any{"LocationArn": r.ARN, "LocationUri": r.Data["LocationUri"]})
		}
		page, next := s2Page(items, c.In["MaxResults"], str(c.In["NextToken"]))
		out := map[string]any{"Locations": orEmpty(page)}
		if next != "" {
			out["NextToken"] = next
		}
		return out, nil
	})

	// A task needs two locations that exist, and its ARN is task/task-<id>.
	Override("datasync", "CreateTask", func(c *Call) (map[string]any, error) {
		for _, m := range []string{"SourceLocationArn", "DestinationLocationArn"} {
			if _, err := dsLocation(c, str(c.In[m]), ""); err != nil {
				return nil, err
			}
		}
		data := s2Clone(c.In)
		delete(data, "Tags")
		id := "task-" + randHex(17)
		arn := c.arnOf("task/"+id, nil)
		data["TaskArn"] = arn
		data["Options"] = dsOptions(c.In["Options"])
		c.Res = s2Store(c, "Task", arn, str(c.In["Name"]), arn, data, "TaskArn")
		s2Tags(c, c.Res)
		return map[string]any{"TaskArn": arn}, nil
	})
	s2After("datasync", "UpdateTask", func(c *Call, out map[string]any) error {
		s2Set(c, nil, map[string]any{"Options": dsOptions(c.Res.Data["Options"])})
		return nil
	})
	Hook("datasync", "DeleteTask", func(c *Call) {
		for _, e := range c.List("TaskExecution") {
			if str(e.Data["_task"]) == c.Res.ARN {
				c.Svc.Delete(c.Account, c.Region, "TaskExecution", e.Key)
			}
		}
	})

	Override("datasync", "StartTaskExecution", func(c *Call) (map[string]any, error) {
		task := c.Get("Task", str(c.In["TaskArn"]))
		if task == nil {
			return nil, c.Invalid("The task %s does not exist", str(c.In["TaskArn"]))
		}
		src, _ := dsLocation(c, str(task.Data["SourceLocationArn"]), "")
		dst, _ := dsLocation(c, str(task.Data["DestinationLocationArn"]), "")
		if src == nil || dst == nil {
			return nil, c.Invalid("A location of the task no longer exists")
		}
		opts := s2Clone(s2Map(task.Data["Options"]))
		for k, v := range s2Map(c.In["OverrideOptions"]) {
			opts[k] = v
		}
		inc, exc := task.Data["Includes"], task.Data["Excludes"]
		if v := c.In["Includes"]; v != nil {
			inc = v
		}
		if v := c.In["Excludes"]; v != nil {
			exc = v
		}
		id := "exec-" + randHex(17)
		arn := task.ARN + "/execution/" + id
		start := time.Now().UTC()
		res := dsCopy(c, src, dst, opts, dsFilter(inc), dsFilter(exc))
		res["StartTime"], res["EndTime"] = s2Time(start), s2Time(time.Now())
		res["TaskExecutionArn"], res["Options"], res["_task"] = arn, opts, task.ARN
		res["Includes"], res["Excludes"] = inc, exc
		s2Store(c, "TaskExecution", arn, id, arn, res)
		return map[string]any{"TaskExecutionArn": arn}, nil
	})
	Override("datasync", "DescribeTaskExecution", func(c *Call) (map[string]any, error) {
		e := c.Get("TaskExecution", str(c.In["TaskExecutionArn"]))
		if e == nil {
			return nil, c.Invalid("The task execution %s does not exist", str(c.In["TaskExecutionArn"]))
		}
		return e.Data, nil
	})
	Override("datasync", "ListTaskExecutions", func(c *Call) (map[string]any, error) {
		var items []any
		for _, e := range c.List("TaskExecution") {
			if t := str(c.In["TaskArn"]); t == "" || t == str(e.Data["_task"]) {
				items = append(items, map[string]any{"TaskExecutionArn": e.ARN, "Status": e.Data["Status"]})
			}
		}
		page, next := s2Page(items, c.In["MaxResults"], str(c.In["NextToken"]))
		out := map[string]any{"TaskExecutions": orEmpty(page)}
		if next != "" {
			out["NextToken"] = next
		}
		return out, nil
	})
	for _, op := range []string{"CancelTaskExecution", "UpdateTaskExecution"} { // an execution is over by the time anyone can ask
		Override("datasync", op, func(c *Call) (map[string]any, error) {
			if c.Get("TaskExecution", str(c.In["TaskExecutionArn"])) == nil {
				return nil, c.Invalid("The task execution %s does not exist", str(c.In["TaskExecutionArn"]))
			}
			return map[string]any{}, nil
		})
	}
}

// dsOptions is a task's options: what DataSync sets when the call leaves one out, then what the call set.
func dsOptions(set any) map[string]any {
	opts := map[string]any{"Atime": "BEST_EFFORT", "BytesPerSecond": -1.0, "Gid": "INT_VALUE", "LogLevel": "OFF", "Mtime": "PRESERVE", "ObjectTags": "PRESERVE",
		"OverwriteMode": "ALWAYS", "PosixPermissions": "PRESERVE", "PreserveDeletedFiles": "PRESERVE", "PreserveDevices": "NONE", "SecurityDescriptorCopyFlags": "NONE",
		"TaskQueueing": "ENABLED", "TransferMode": "CHANGED", "Uid": "INT_VALUE", "VerifyMode": "POINT_IN_TIME_CONSISTENT"}
	for k, v := range s2Map(set) {
		opts[k] = v
	}
	return opts
}

func dsLocation(c *Call, arn, kind string) (*Resource, error) {
	r := c.Get("Location", arn)
	if r == nil || kind != "" && r.Data["_kind"] != kind {
		return nil, c.Invalid("The location %s does not exist", arn)
	}
	return r, nil
}

// dsURI is the URI of a location, as DescribeLocation shows it.
func dsURI(c *Call, kind string, d map[string]any) string {
	sub := strings.Trim(str(d["Subdirectory"]), "/")
	if sub != "" {
		sub += "/"
	}
	switch kind {
	case "S3":
		_, bucket, _ := strings.Cut(str(d["S3BucketArn"]), ":::")
		return "s3://" + bucket + "/" + sub
	case "Nfs":
		return "nfs://" + str(d["ServerHostname"]) + "/" + sub
	case "Smb":
		return "smb://" + str(d["ServerHostname"]) + "/" + sub
	case "Efs":
		return "efs://" + c.Region + "." + path.Base(str(d["EfsFilesystemArn"])) + "/" + sub
	}
	return strings.ToLower(kind) + "://" + c.Region + "/" + sub
}

// dsFilter reads the patterns of a list of FilterRules.
func dsFilter(v any) []string {
	var out []string
	for _, r := range asList(v) {
		for p := range strings.SplitSeq(str(s2Map(r)["Value"]), "|") {
			if p = strings.TrimSpace(p); p != "" {
				out = append(out, p)
			}
		}
	}
	return out
}

// dsMatch says whether a path (relative to the location, with a leading "/") or one of its folders matches a pattern.
func dsMatch(patterns []string, rel string) bool {
	for p := rel; p != "" && p != "/"; p = path.Dir(p) {
		for _, pat := range patterns {
			if dsGlob(pat, p) {
				return true
			}
		}
	}
	return false
}

// dsGlob matches a pattern whose only wildcard is "*" (any characters, "/" included).
func dsGlob(pat, s string) bool {
	head, rest, wild := strings.Cut(pat, "*")
	if !wild {
		return pat == s
	}
	rest0, ok := strings.CutPrefix(s, head)
	if !ok {
		return false
	}
	for i := 0; i <= len(rest0); i++ {
		if dsGlob(rest, rest0[i:]) {
			return true
		}
	}
	return false
}

// dsCopy copies the objects of an S3 location into another and returns the members of DescribeTaskExecution.
func dsCopy(c *Call, src, dst *Resource, opts map[string]any, include, exclude []string) map[string]any {
	res := map[string]any{"Status": "ERROR"}
	fail := func(code, detail string) map[string]any {
		res["Result"] = map[string]any{"ErrorCode": code, "ErrorDetail": detail, "PrepareStatus": "ERROR", "TransferStatus": "PENDING", "VerifyStatus": "PENDING"}
		return res
	}
	sb, sp, sok := s3io.Split(str(src.Data["LocationUri"]))
	db, dp, dok := s3io.Split(str(dst.Data["LocationUri"]))
	if !sok || !dok {
		return fail("NotSupported", "pogi copies between S3 locations only")
	}
	s3 := s3io.Client{Hub: c.Hub(), Account: c.Account, Region: c.Region}
	objs, _, err := s3.List(sb, sp, "")
	if err != nil {
		return fail("S3Error", err.Error())
	}
	var listed, skipped, transferred, failed, bytes_ int64
	wanted := map[string]bool{}
	for _, o := range objs {
		if strings.HasSuffix(o.Key, "/") { // a folder marker
			continue
		}
		rel := "/" + strings.TrimPrefix(o.Key, sp)
		if len(include) > 0 && !dsMatch(include, rel) || dsMatch(exclude, rel) {
			continue
		}
		listed++
		wanted[dp+rel[1:]] = true
		data, err := s3.Get(sb, o.Key, 0, -1)
		if err != nil {
			failed++
			continue
		}
		if cur, err := s3.Get(db, dp+rel[1:], 0, -1); err == nil && (str(opts["OverwriteMode"]) == "NEVER" ||
			str(opts["TransferMode"]) != "ALL" && bytes.Equal(cur, data)) {
			skipped++
			continue
		}
		if s3.Put(db, dp+rel[1:], data) != nil {
			failed++
			continue
		}
		transferred++
		bytes_ += int64(len(data))
	}
	var deleted int64
	if str(opts["PreserveDeletedFiles"]) == "REMOVE" { // the destination mirrors the source
		old, _, _ := s3.List(db, dp, "")
		for _, o := range old {
			if !strings.HasSuffix(o.Key, "/") && !wanted[o.Key] && s3.Delete(db, o.Key) == nil {
				deleted++
			}
		}
	}
	status := "SUCCESS"
	if failed > 0 {
		status = "ERROR"
	}
	res["Status"] = status
	res["EstimatedFilesToTransfer"], res["EstimatedBytesToTransfer"] = float64(listed-skipped), float64(bytes_)
	res["FilesTransferred"], res["BytesTransferred"], res["BytesWritten"], res["BytesCompressed"] = float64(transferred), float64(bytes_), float64(bytes_), float64(bytes_)
	res["FilesSkipped"], res["FilesPrepared"], res["FilesVerified"], res["FilesDeleted"] = float64(skipped), float64(listed), float64(transferred), float64(deleted)
	res["FilesListed"] = map[string]any{"AtSource": float64(listed)}
	res["FilesFailed"] = map[string]any{"Transfer": float64(failed)}
	res["Result"] = map[string]any{"PrepareStatus": "SUCCESS", "TransferStatus": status, "VerifyStatus": "SUCCESS", "TotalDuration": 0.0}
	if failed > 0 {
		res["Result"].(map[string]any)["ErrorCode"], res["Result"].(map[string]any)["ErrorDetail"] = "S3Error", "some objects could not be copied"
	}
	return res
}
