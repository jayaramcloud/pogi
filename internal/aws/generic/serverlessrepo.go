package generic

import (
	"net/url"
	"regexp"
	"strconv"
)

// Serverless Application Repository: an application is named by an ARN that ends in its name,
// so the id is built from the name (the engine would make up a random one).
//
// CreateCloudFormationChangeSet makes a real change set in pogi's own CloudFormation, from the
// template of the application version, and tags the stack the way AWS does
// (serverlessrepo:applicationId, serverlessrepo:semanticVersion). Executing it is the caller's job.

var srCFNID = regexp.MustCompile(`<(StackId|Id)>([^<]+)</`)

func init() {
	Override("serverlessrepo", "CreateApplication", func(c *Call) (map[string]any, error) {
		c.In["ApplicationId"] = c.arnOf("applications/"+str(c.In["Name"]), nil)
		return c.Default()
	})
	Override("serverlessrepo", "CreateCloudFormationChangeSet", func(c *Call) (map[string]any, error) {
		appID := str(c.In["ApplicationId"])
		app := c.Get("Application", appID)
		if app == nil {
			return nil, c.NotFound("Application %s not found", appID)
		}
		ver, body := str(c.In["SemanticVersion"]), str(app.Data["TemplateBody"])
		if ver == "" {
			ver = str(app.Data["SemanticVersion"])
		}
		for _, v := range c.List("ApplicationVersion") {
			if str(v.Data["ApplicationId"]) == appID && str(v.Data["SemanticVersion"]) == ver && str(v.Data["TemplateBody"]) != "" {
				body = str(v.Data["TemplateBody"])
			}
		}
		if body == "" {
			return nil, c.Invalid("Version %s of application %s has no template", ver, appID)
		}
		set := str(c.In["ChangeSetName"])
		if set == "" {
			set = str(c.In["StackName"]) + "-" + randHex(8)
		}
		f := url.Values{"Action": {"CreateChangeSet"}, "Version": {"2010-05-15"}, "StackName": {str(c.In["StackName"])},
			"ChangeSetName": {set}, "ChangeSetType": {"CREATE"}, "TemplateBody": {body}}
		for i, cp := range asList(c.In["Capabilities"]) {
			f.Set("Capabilities.member."+strconv.Itoa(i+1), str(cp))
		}
		for i, p := range asList(c.In["ParameterOverrides"]) {
			m, _ := p.(map[string]any)
			n := "Parameters.member." + strconv.Itoa(i+1)
			f.Set(n+".ParameterKey", str(m["Name"]))
			f.Set(n+".ParameterValue", str(m["Value"]))
		}
		tags := append(asList(c.In["Tags"]), map[string]any{"Key": "serverlessrepo:applicationId", "Value": appID},
			map[string]any{"Key": "serverlessrepo:semanticVersion", "Value": ver})
		for i, t := range tags {
			m, _ := t.(map[string]any)
			n := "Tags.member." + strconv.Itoa(i+1)
			f.Set(n+".Key", str(m["Key"]))
			f.Set(n+".Value", str(m["Value"]))
		}
		st, _, out := c.Hub().Do(c.Account, c.Region, "cloudformation", "POST", "/",
			map[string][]string{"Content-Type": {"application/x-www-form-urlencoded"}}, []byte(f.Encode()))
		if st != 200 {
			return nil, c.Invalid("CloudFormation refused the change set: %s", out)
		}
		ids := map[string]string{}
		for _, m := range srCFNID.FindAllSubmatch(out, -1) {
			ids[string(m[1])] = string(m[2])
		}
		return map[string]any{"ApplicationId": appID, "SemanticVersion": ver, "ChangeSetId": ids["Id"], "StackId": ids["StackId"]}, nil
	})
}
