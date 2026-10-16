package generic

import (
	"slices"

	"github.com/nrndev/pogi/internal/core"
)

// CodeGuru Reviewer: repository associations go Associating, Associated and are
// removed on disassociate; code reviews are the engine's CRUD against an
// association. Nothing is reviewed.
func init() {
	Override("codeguru-reviewer", "AssociateRepository", func(c *Call) (map[string]any, error) {
		repo, _ := c.In["Repository"].(map[string]any)
		var provider, name, owner, conn string
		for _, p := range []string{"CodeCommit", "Bitbucket", "GitHubEnterpriseServer", "S3Bucket"} {
			if m, ok := repo[p].(map[string]any); ok {
				provider, name, owner, conn = p, str(m["Name"]), str(m["Owner"]), str(m["ConnectionArn"])
			}
		}
		if provider == "" || name == "" {
			return nil, c.Invalid("Repository must name a CodeCommit, Bitbucket, GitHubEnterpriseServer or S3Bucket repository.")
		}
		if owner == "" {
			owner = c.Account
		}
		for _, r := range c.List("RepositoryAssociation") {
			if r.Name == name && r.Data["ProviderType"] == provider && r.Data["Owner"] == owner {
				return nil, c.Exists("Repository %s is already associated.", name)
			}
		}
		id := core.UUID()
		now := c.now()
		data := map[string]any{"AssociationId": id, "Name": name, "Owner": owner, "ProviderType": provider}
		if conn != "" {
			data["ConnectionArn"] = conn
		}
		if k, ok := c.In["KMSKeyDetails"]; ok {
			data["KMSKeyDetails"] = k
		}
		if s3, ok := repo["S3Bucket"].(map[string]any); ok {
			data["S3RepositoryDetails"] = map[string]any{"BucketName": s3["Name"], "CodeArtifacts": map[string]any{"SourceCodeArtifactsObjectKey": "", "BuildArtifactsObjectKey": ""}}
		}
		r := &Resource{Type: "RepositoryAssociation", Key: id, IDs: map[string]string{"AssociationId": id}, ID: id, Name: name, ARN: c.arnOf("association:"+id, nil), Account: c.Account, Region: c.Region,
			Data: data, Tags: tagMap(c.In["Tags"]), Created: now, Updated: now}
		if len(r.Tags) == 0 {
			r.Tags = nil
		}
		Begin(r, "assoc")
		c.Put(r)
		return cgOut(r), nil
	})
	Override("codeguru-reviewer", "DescribeRepositoryAssociation", func(c *Call) (map[string]any, error) {
		r := c.Svc.byARN(c.Account, str(c.In["AssociationArn"]))
		if r == nil || r.Type != "RepositoryAssociation" {
			return nil, c.NotFound("Association %s not found.", str(c.In["AssociationArn"]))
		}
		return cgOut(r), nil
	})
	Override("codeguru-reviewer", "DisassociateRepository", func(c *Call) (map[string]any, error) {
		r := c.Svc.byARN(c.Account, str(c.In["AssociationArn"]))
		if r == nil || r.Type != "RepositoryAssociation" {
			return nil, c.NotFound("Association %s not found.", str(c.In["AssociationArn"]))
		}
		c.Svc.Delete(c.Account, c.Region, "RepositoryAssociation", r.Key)
		out := cgOut(r)
		out["RepositoryAssociation"].(map[string]any)["State"] = "Disassociating"
		return out, nil
	})
	Override("codeguru-reviewer", "ListRepositoryAssociations", func(c *Call) (map[string]any, error) {
		in := func(k string, v any) bool { l := asList(c.In[k]); return len(l) == 0 || slices.Contains(l, v) }
		var items []any
		for _, r := range c.List("RepositoryAssociation") {
			a := cgOut(r)["RepositoryAssociation"].(map[string]any)
			if in("ProviderTypes", a["ProviderType"]) && in("States", a["State"]) && in("Names", a["Name"]) && in("Owners", a["Owner"]) {
				delete(a, "StateReason")
				delete(a, "CreatedTimeStamp")
				delete(a, "KMSKeyDetails")
				delete(a, "S3RepositoryDetails")
				items = append(items, a)
			}
		}
		page, next, err := pageOf(c, items, "NextToken", "MaxResults")
		if err != nil {
			return nil, err
		}
		out := map[string]any{"RepositoryAssociationSummaries": page}
		if next != "" {
			out["NextToken"] = next
		}
		return out, nil
	})
	// A review needs an association that exists.
	Override("codeguru-reviewer", "CreateCodeReview", func(c *Call) (map[string]any, error) {
		a := c.Svc.byARN(c.Account, str(c.In["RepositoryAssociationArn"]))
		if a == nil {
			return nil, c.NotFound("Association %s not found.", str(c.In["RepositoryAssociationArn"]))
		}
		if _, err := c.Default(); err != nil {
			return nil, err
		}
		r := c.Res
		r.Data["RepositoryName"], r.Data["Owner"], r.Data["ProviderType"] = a.Name, a.Data["Owner"], a.Data["ProviderType"]
		Begin(r, "review")
		c.Put(r)
		return c.Fill(r), nil
	})
	Derive("codeguru-reviewer", "CodeReview", func(r *Resource) map[string]any {
		return map[string]any{"State": Step(r, "review", "Pending", "Completed")}
	})
}

// cgOut is an association as the calls show it, with its tags.
func cgOut(r *Resource) map[string]any {
	a := map[string]any{"AssociationId": r.ID, "AssociationArn": r.ARN, "Name": r.Name, "Owner": r.Data["Owner"], "ProviderType": r.Data["ProviderType"],
		"State": Step(r, "assoc", "Associating", "Associated"), "StateReason": "", "LastUpdatedTimeStamp": r.Updated.Format(timeFmt), "CreatedTimeStamp": r.Created.Format(timeFmt)}
	for _, k := range []string{"ConnectionArn", "KMSKeyDetails", "S3RepositoryDetails"} {
		if v := r.Data[k]; v != nil {
			a[k] = v
		}
	}
	tags := map[string]any{}
	for k, v := range r.Tags {
		tags[k] = v
	}
	return map[string]any{"RepositoryAssociation": a, "Tags": tags}
}
