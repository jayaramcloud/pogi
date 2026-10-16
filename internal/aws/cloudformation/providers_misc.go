package cloudformation

import (
	"net/url"
	"strings"
	"time"
)

// Pipes, Cognito groups and API Gateway authorizers.

func pipeState(c *pctx, name string) (state string, attrs map[string]any, err error) {
	var out struct {
		Arn, CurrentState, DesiredState, StateReason string
		CreationTime, LastModifiedTime               float64
	}
	if err = c.rest("pipes", "GET", "/v1/pipes/"+name, nil, &out); err != nil {
		return "", nil, err
	}
	return out.CurrentState, map[string]any{"Arn": out.Arn, "CurrentState": out.CurrentState, "DesiredState": out.DesiredState,
		"StateReason": out.StateReason, "CreationTime": out.CreationTime, "LastModifiedTime": out.LastModifiedTime}, nil
}

// pipeSettled waits until a pipe is not starting, stopping, creating or updating.
func pipeSettled(c *pctx, name string) (map[string]any, error) {
	var attrs map[string]any
	var err error
	waitFor(10*time.Second, func() bool {
		var st string
		st, attrs, err = pipeState(c, name)
		return err != nil || st == "RUNNING" || !strings.HasSuffix(st, "ING")
	})
	return attrs, err
}

func init() {
	pipeBody := func(p map[string]any, keys ...string) map[string]any {
		in := pick(p, keys...)
		if t, ok := p["Tags"]; ok {
			in["Tags"] = tagMap(t)
		}
		return in
	}
	pipeTags := func(c *pctx, arn string, old, now any) error {
		o, n := tagMap(old), tagMap(now)
		gone := url.Values{}
		for k := range o {
			if _, ok := n[k]; !ok {
				gone.Add("tagKeys", k)
			}
		}
		if len(gone) > 0 {
			if err := c.rest("pipes", "DELETE", "/tags/"+arn+"?"+gone.Encode(), nil, nil); err != nil {
				return err
			}
		}
		if len(n) > 0 && !equal(o, n) {
			return c.rest("pipes", "POST", "/tags/"+arn, map[string]any{"tags": n}, nil)
		}
		return nil
	}
	register("AWS::Pipes::Pipe", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := or(str(p["Name"]), c.name(64, false))
			if err := c.rest("pipes", "POST", "/v1/pipes/"+name, pipeBody(p, "Source", "Target", "RoleArn", "Description", "DesiredState", "Enrichment",
				"SourceParameters", "EnrichmentParameters", "TargetParameters", "LogConfiguration", "KmsKeyIdentifier"), nil); err != nil {
				return "", nil, err
			}
			attrs, err := pipeSettled(c, name)
			return name, attrs, err
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, "Description", "DesiredState", "Enrichment", "SourceParameters", "EnrichmentParameters", "TargetParameters", "LogConfiguration",
				"KmsKeyIdentifier", "RoleArn", "Target", "Tags") {
				return nil, true, nil
			}
			if _, err := pipeSettled(c, phys); err != nil {
				return nil, false, err
			}
			if differs(old, p, "Tags") {
				if err := c.rest("pipes", "PUT", "/v1/pipes/"+phys, pick(p, "Description", "DesiredState", "Enrichment", "SourceParameters", "EnrichmentParameters",
					"TargetParameters", "LogConfiguration", "KmsKeyIdentifier", "RoleArn", "Target"), nil); err != nil {
					return nil, false, err
				}
			}
			attrs, err := pipeSettled(c, phys)
			if err != nil {
				return nil, false, err
			}
			return attrs, false, pipeTags(c, str(attrs["Arn"]), old["Tags"], p["Tags"])
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			if _, err := pipeSettled(c, phys); err != nil {
				return err
			}
			if err := c.rest("pipes", "DELETE", "/v1/pipes/"+phys, nil, nil); err != nil {
				return err
			}
			waitFor(10*time.Second, func() bool { _, _, err := pipeState(c, phys); return err != nil })
			return nil
		},
	})

	const cognito = "AWSCognitoIdentityProviderService"
	register("AWS::Cognito::UserPoolGroup", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := pick(p, "UserPoolId", "Description", "RoleArn", "Precedence")
			in["GroupName"] = or(str(p["GroupName"]), c.name(128, false))
			return str(in["GroupName"]), nil, c.call(cognito, "CreateGroup", in, nil)
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, "Description", "RoleArn", "Precedence") {
				return nil, true, nil
			}
			in := pick(p, "UserPoolId", "Description", "RoleArn", "Precedence")
			in["GroupName"] = phys
			return nil, false, c.call(cognito, "UpdateGroup", in, nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call(cognito, "DeleteGroup", map[string]any{"UserPoolId": p["UserPoolId"], "GroupName": phys}, nil)
		},
	})

	// ---- API Gateway authorizers ----
	esc := strings.NewReplacer("~", "~0", "/", "~1") // JSON pointer escapes: an ARN holds slashes
	restAuth := func(p map[string]any) map[string]any {
		return pick(p, "Name=name", "Type=type", "AuthorizerUri=authorizerUri", "AuthorizerCredentials=authorizerCredentials", "AuthType=authType",
			"IdentitySource=identitySource", "IdentityValidationExpression=identityValidationExpression", "ProviderARNs=providerARNs",
			"AuthorizerResultTtlInSeconds=authorizerResultTtlInSeconds")
	}
	register("AWS::ApiGateway::Authorizer", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			var out struct {
				ID string `json:"id"`
			}
			err := c.rest("apigateway", "POST", "/restapis/"+str(p["RestApiId"])+"/authorizers", restAuth(p), &out)
			return out.ID, map[string]any{"AuthorizerId": out.ID}, err
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(old["RestApiId"]) != str(p["RestApiId"]) {
				return nil, true, nil
			}
			var ops []map[string]any
			for k, v := range restAuth(p) {
				switch {
				case k == "providerARNs":
					for _, a := range minus(list(old["ProviderARNs"]), list(p["ProviderARNs"])) {
						ops = append(ops, map[string]any{"op": "remove", "path": "/providerARNs/" + esc.Replace(a)})
					}
					for _, a := range minus(list(p["ProviderARNs"]), list(old["ProviderARNs"])) {
						ops = append(ops, map[string]any{"op": "add", "path": "/providerARNs/" + esc.Replace(a)})
					}
				case !equal(restAuth(old)[k], v):
					ops = append(ops, map[string]any{"op": "replace", "path": "/" + k, "value": str(v)})
				}
			}
			if len(ops) == 0 {
				return nil, false, nil
			}
			return nil, false, c.rest("apigateway", "PATCH", "/restapis/"+str(p["RestApiId"])+"/authorizers/"+phys, map[string]any{"patchOperations": ops}, nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("apigateway", "DELETE", "/restapis/"+str(p["RestApiId"])+"/authorizers/"+phys, nil, nil)
		},
	})
	httpAuth := func(p map[string]any) map[string]any {
		in := camel(p, false).(map[string]any)
		delete(in, "apiId")
		return in
	}
	register("AWS::ApiGatewayV2::Authorizer", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			var out struct {
				ID string `json:"authorizerId"`
			}
			err := c.rest("apigateway", "POST", "/v2/apis/"+str(p["ApiId"])+"/authorizers", httpAuth(p), &out)
			return out.ID, map[string]any{"AuthorizerId": out.ID}, err
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(old["ApiId"]) != str(p["ApiId"]) {
				return nil, true, nil
			}
			return nil, false, c.rest("apigateway", "PATCH", "/v2/apis/"+str(p["ApiId"])+"/authorizers/"+phys, httpAuth(p), nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("apigateway", "DELETE", "/v2/apis/"+str(p["ApiId"])+"/authorizers/"+phys, nil, nil)
		},
	})
}
