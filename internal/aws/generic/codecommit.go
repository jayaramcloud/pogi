package generic

import "regexp"

var ccName = regexp.MustCompile(`^[\w.-]{1,100}$`)

// CodeCommit: a repository's ARN is just its name, and it has the clone URLs AWS gives it. There is no git
// server behind them. Deleting a repository that is already gone answers with no repository id, not an error.
func init() {
	arnTemplates["codecommit/Repository"] = "{repositoryName}"
	Override("codecommit", "CreateRepository", func(c *Call) (map[string]any, error) {
		if !ccName.MatchString(str(c.In["repositoryName"])) {
			return nil, c.Err("InvalidRepositoryNameException", "The repository name %q is not valid: use letters, digits, . _ and -.", str(c.In["repositoryName"]))
		}
		if _, err := c.Default(); err != nil {
			return nil, err
		}
		path := "git-codecommit." + c.Region + ".amazonaws.com/v1/repos/" + c.Res.Name
		s2Set(c, nil, map[string]any{"cloneUrlHttp": "https://" + path, "cloneUrlSsh": "ssh://" + path})
		return c.Fill(c.Res), nil
	})
	Override("codecommit", "DeleteRepository", func(c *Call) (map[string]any, error) {
		if c.Get("Repository", str(c.In["repositoryName"])) == nil {
			return map[string]any{}, nil
		}
		return c.Default()
	})
}

// Renaming moves the repository to a new name, and its ARN and clone URLs follow.
func init() {
	Override("codecommit", "UpdateRepositoryName", func(c *Call) (map[string]any, error) {
		oldName, name := str(c.In["oldName"]), str(c.In["newName"])
		r := c.Get("Repository", oldName)
		if r == nil {
			return nil, c.NotFound("Repository %s does not exist.", oldName)
		}
		if c.Get("Repository", name) != nil {
			return nil, c.Exists("Repository %s already exists.", name)
		}
		c.Svc.Delete(c.Account, c.Region, "Repository", oldName)
		path := "git-codecommit." + c.Region + ".amazonaws.com/v1/repos/" + name
		r.Key, r.Name, r.ARN = name, name, c.arnOf(name, nil)
		r.IDs = map[string]string{"repositoryName": name}
		r.Data["repositoryName"] = name
		r.Data["cloneUrlHttp"], r.Data["cloneUrlSsh"] = "https://"+path, "ssh://"+path
		r.Updated = c.now()
		c.Put(r)
		return map[string]any{}, nil
	})
}
