package cloudformation

import (
	"net/url"
	"strings"
)

// Auto Scaling, Application Auto Scaling, ECS capacity providers, listener certificates,
// Lambda invoke settings and API Gateway keys, usage plans and domain names.

func init() {
	// ---- Auto Scaling (Query protocol) ----
	const asg = "autoscaling"
	groupKey := func(phys string, p map[string]any, member string) map[string]any {
		return map[string]any{"AutoScalingGroupName": p["AutoScalingGroupName"], member: phys}
	}
	register("AWS::AutoScaling::ScalingPolicy", simple(spec{sign: asg, create: "PutScalingPolicy", update: "PutScalingPolicy", del: "DeletePolicy", name: "PolicyName", max: 255,
		inPlace: []string{"PolicyType", "AdjustmentType", "ScalingAdjustment", "Cooldown", "MinAdjustmentMagnitude", "MetricAggregationType", "EstimatedInstanceWarmup",
			"StepAdjustments", "TargetTrackingConfiguration", "Enabled"},
		id: func(_ string, out map[string]any) string { return dig(out, "PolicyARN") },
		delIn: func(phys string, p map[string]any) map[string]any {
			return groupKey(lastSegment(phys), p, "PolicyName")
		}}))
	register("AWS::AutoScaling::ScheduledAction", simple(spec{sign: asg, create: "PutScheduledUpdateGroupAction", update: "PutScheduledUpdateGroupAction", del: "DeleteScheduledAction",
		name: "ScheduledActionName", max: 255, inPlace: []string{"DesiredCapacity", "MinSize", "MaxSize", "Recurrence", "StartTime", "EndTime", "TimeZone"},
		delIn: func(phys string, p map[string]any) map[string]any { return groupKey(phys, p, "ScheduledActionName") }}))
	register("AWS::AutoScaling::LifecycleHook", simple(spec{sign: asg, create: "PutLifecycleHook", update: "PutLifecycleHook", del: "DeleteLifecycleHook", name: "LifecycleHookName", max: 255,
		inPlace: []string{"LifecycleTransition", "DefaultResult", "HeartbeatTimeout", "NotificationMetadata", "NotificationTargetARN", "RoleARN"},
		delIn:   func(phys string, p map[string]any) map[string]any { return groupKey(phys, p, "LifecycleHookName") }}))
	// the warm pool has no id of its own: it is the group's
	warm := func(c *pctx, p map[string]any) error {
		_, err := c.query(asg, "PutWarmPool", pick(p, "AutoScalingGroupName", "MaxGroupPreparedCapacity", "MinSize", "PoolState", "InstanceReusePolicy"))
		return err
	}
	register("AWS::AutoScaling::WarmPool", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			return str(p["AutoScalingGroupName"]), nil, warm(c, p)
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(old["AutoScalingGroupName"]) != str(p["AutoScalingGroupName"]) {
				return nil, true, nil
			}
			return nil, false, warm(c, p)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.query(asg, "DeleteWarmPool", map[string]any{"AutoScalingGroupName": phys, "ForceDelete": true})
			return err
		},
	})

	// ---- Application Auto Scaling ----
	// A scalable target is named by what it scales: ResourceId|ScalableDimension|ServiceNamespace.
	targetOf := func(p map[string]any) (ns, rid, dim string) {
		if id := str(p["ScalingTargetId"]); id != "" {
			parts := strings.Split(id, "|")
			if len(parts) == 3 {
				return parts[2], parts[0], parts[1]
			}
		}
		return str(p["ServiceNamespace"]), str(p["ResourceId"]), str(p["ScalableDimension"])
	}
	actions := func(c *pctx, p map[string]any, ns, rid, dim string) error {
		for _, a := range listMaps(p["ScheduledActions"]) {
			in := camel(a, false).(map[string]any)
			in["serviceNamespace"], in["resourceId"], in["scalableDimension"] = ns, rid, dim
			if err := c.call(appAsT, "PutScheduledAction", in, nil); err != nil {
				return err
			}
		}
		return nil
	}
	register("AWS::ApplicationAutoScaling::ScalableTarget", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := pick(p, "ServiceNamespace", "ResourceId", "ScalableDimension", "MinCapacity", "MaxCapacity", "RoleARN", "SuspendedState")
			if err := c.call(appAsT, "RegisterScalableTarget", in, nil); err != nil {
				return "", nil, err
			}
			ns, rid, dim := targetOf(p)
			if err := actions(c, p, ns, rid, dim); err != nil {
				return "", nil, err
			}
			return rid + "|" + dim + "|" + ns, nil, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, "MinCapacity", "MaxCapacity", "RoleARN", "SuspendedState", "ScheduledActions") {
				return nil, true, nil
			}
			ns, rid, dim := targetOf(p)
			if err := c.call(appAsT, "RegisterScalableTarget", pick(p, "ServiceNamespace", "ResourceId", "ScalableDimension", "MinCapacity", "MaxCapacity", "RoleARN", "SuspendedState"), nil); err != nil {
				return nil, false, err
			}
			keep := map[string]bool{}
			for _, a := range listMaps(p["ScheduledActions"]) {
				keep[str(a["ScheduledActionName"])] = true
			}
			for _, a := range listMaps(old["ScheduledActions"]) {
				if n := str(a["ScheduledActionName"]); !keep[n] {
					c.call(appAsT, "DeleteScheduledAction", map[string]any{"ServiceNamespace": ns, "ResourceId": rid, "ScalableDimension": dim, "ScheduledActionName": n}, nil)
				}
			}
			return nil, false, actions(c, p, ns, rid, dim)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			ns, rid, dim := targetOf(p)
			for _, a := range listMaps(p["ScheduledActions"]) {
				c.call(appAsT, "DeleteScheduledAction", map[string]any{"ServiceNamespace": ns, "ResourceId": rid, "ScalableDimension": dim, "ScheduledActionName": a["ScheduledActionName"]}, nil)
			}
			return c.call(appAsT, "DeregisterScalableTarget", map[string]any{"ServiceNamespace": ns, "ResourceId": rid, "ScalableDimension": dim}, nil)
		},
	})
	register("AWS::ApplicationAutoScaling::ScalingPolicy", simple(spec{target: appAsT, create: "PutScalingPolicy", update: "PutScalingPolicy", del: "DeleteScalingPolicy", name: "PolicyName", max: 255, key: "PolicyName",
		inPlace: []string{"PolicyType", "StepScalingPolicyConfiguration", "TargetTrackingScalingPolicyConfiguration"},
		build: func(c *pctx, p map[string]any, name string) map[string]any {
			in := pick(p, "PolicyType", "StepScalingPolicyConfiguration", "TargetTrackingScalingPolicyConfiguration")
			ns, rid, dim := targetOf(p)
			in["ServiceNamespace"], in["ResourceId"], in["ScalableDimension"] = ns, rid, dim
			if _, ok := in["PolicyType"]; !ok {
				in["PolicyType"] = "TargetTrackingScaling"
			}
			return in
		},
		id: func(_ string, out map[string]any) string { return dig(out, "PolicyARN") },
		delIn: func(phys string, p map[string]any) map[string]any {
			ns, rid, dim := targetOf(p)
			_, name, _ := strings.Cut(phys, ":policyName/")
			return map[string]any{"PolicyName": name, "ServiceNamespace": ns, "ResourceId": rid, "ScalableDimension": dim}
		}}))

	// ---- ECS ----
	register("AWS::ECS::CapacityProvider", simple(spec{target: ecsTarget, create: "CreateCapacityProvider", del: "DeleteCapacityProvider", name: "Name", max: 255, key: "name",
		build: func(c *pctx, p map[string]any, _ string) map[string]any {
			return camel(pick(p, "AutoScalingGroupProvider", "Tags"), true).(map[string]any)
		},
		delIn: func(phys string, _ map[string]any) map[string]any { return map[string]any{"capacityProvider": phys} }}))
	register("AWS::ECS::ClusterCapacityProviderAssociations", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			return str(p["Cluster"]), nil, putCapacity(c, p)
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(old["Cluster"]) != str(p["Cluster"]) {
				return nil, true, nil
			}
			return nil, false, putCapacity(c, p)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.ecs("PutClusterCapacityProviders", map[string]any{"cluster": phys, "capacityProviders": []any{}, "defaultCapacityProviderStrategy": []any{}}, nil)
		},
	})

	// ---- ELB v2 ----
	register("AWS::ElasticLoadBalancingV2::ListenerCertificate", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			_, err := c.elb("AddListenerCertificates", pick(p, "ListenerArn", "Certificates"))
			return c.name(64, false), nil, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := c.elb("RemoveListenerCertificates", pick(p, "ListenerArn", "Certificates"))
			return err
		},
	})

	// ---- Lambda ----
	invokePath := func(p map[string]any) string {
		path := "/2019-09-25/functions/" + url.PathEscape(str(p["FunctionName"])) + "/event-invoke-config"
		if q := str(p["Qualifier"]); q != "" && q != "$LATEST" {
			path += "?Qualifier=" + url.QueryEscape(q)
		}
		return path
	}
	register("AWS::Lambda::EventInvokeConfig", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			return str(p["FunctionName"]) + ":" + str(p["Qualifier"]), nil, c.rest("lambda", "PUT", invokePath(p), pick(p, "MaximumEventAgeInSeconds", "MaximumRetryAttempts", "DestinationConfig"), nil)
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, "MaximumEventAgeInSeconds", "MaximumRetryAttempts", "DestinationConfig") {
				return nil, true, nil
			}
			return nil, false, c.rest("lambda", "PUT", invokePath(p), pick(p, "MaximumEventAgeInSeconds", "MaximumRetryAttempts", "DestinationConfig"), nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("lambda", "DELETE", invokePath(p), nil, nil)
		},
	})

	// ---- API Gateway ----
	body := func(p map[string]any, rename map[string]string, drop ...string) map[string]any {
		in := camel(p, false).(map[string]any)
		for from, to := range rename {
			if v, ok := in[lowerFirst(from)]; ok {
				in[to] = v
				delete(in, lowerFirst(from))
			}
		}
		for _, k := range drop {
			delete(in, lowerFirst(k))
		}
		if t, ok := p["Tags"]; ok {
			in["tags"] = tagMap(t)
		}
		return in
	}
	idOf := func(m string) func(map[string]any) string {
		return func(out map[string]any) string { return dig(out, m) }
	}
	rest := func(c *pctx, method, path string, in any) (map[string]any, error) {
		out := map[string]any{}
		return out, c.rest("apigateway", method, path, in, &out)
	}
	register("AWS::ApiGateway::ApiKey", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := body(p, map[string]string{"CustomerId": "customerId"}, "StageKeys", "GenerateDistinctId")
			if in["name"] == nil {
				in["name"] = c.name(255, false)
			}
			out, err := rest(c, "POST", "/apikeys", in)
			id := idOf("id")(out)
			return id, map[string]any{"APIKeyId": id}, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("apigateway", "DELETE", "/apikeys/"+phys, nil, nil)
		},
	})
	register("AWS::ApiGateway::UsagePlan", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := body(p, map[string]string{"UsagePlanName": "name"})
			out, err := rest(c, "POST", "/usageplans", in)
			id := idOf("id")(out)
			return id, map[string]any{"Id": id}, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("apigateway", "DELETE", "/usageplans/"+phys, nil, nil)
		},
	})
	register("AWS::ApiGateway::UsagePlanKey", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			out, err := rest(c, "POST", "/usageplans/"+str(p["UsagePlanId"])+"/keys", map[string]any{"keyId": p["KeyId"], "keyType": or(str(p["KeyType"]), "API_KEY")})
			return idOf("id")(out), nil, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("apigateway", "DELETE", "/usageplans/"+str(p["UsagePlanId"])+"/keys/"+phys, nil, nil)
		},
	})
	register("AWS::ApiGateway::DomainName", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			out, err := rest(c, "POST", "/domainnames", body(p, nil))
			name := or(dig(out, "domainName"), str(p["DomainName"]))
			return name, map[string]any{"DistributionDomainName": dig(out, "distributionDomainName"), "DistributionHostedZoneId": dig(out, "distributionHostedZoneId"),
				"RegionalDomainName": dig(out, "regionalDomainName"), "RegionalHostedZoneId": dig(out, "regionalHostedZoneId"), "DomainNameId": dig(out, "domainNameId")}, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("apigateway", "DELETE", "/domainnames/"+url.PathEscape(phys), nil, nil)
		},
	})
	register("AWS::ApiGateway::BasePathMapping", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := body(p, nil, "DomainName")
			_, err := rest(c, "POST", "/domainnames/"+url.PathEscape(str(p["DomainName"]))+"/basepathmappings", in)
			return str(p["DomainName"]) + "/" + or(str(p["BasePath"]), "(none)"), nil, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("apigateway", "DELETE", "/domainnames/"+url.PathEscape(str(p["DomainName"]))+"/basepathmappings/"+url.PathEscape(or(str(p["BasePath"]), "(none)")), nil, nil)
		},
	})
	register("AWS::ApiGateway::Model", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := or(str(p["Name"]), c.name(64, false))
			in := map[string]any{"name": name, "description": p["Description"], "contentType": p["ContentType"]}
			if sc, ok := p["Schema"]; ok {
				in["schema"] = jsonString(sc)
			}
			_, err := rest(c, "POST", "/restapis/"+str(p["RestApiId"])+"/models", in)
			return name, nil, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("apigateway", "DELETE", "/restapis/"+str(p["RestApiId"])+"/models/"+url.PathEscape(phys), nil, nil)
		},
	})
	register("AWS::ApiGateway::RequestValidator", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := map[string]any{"name": or(str(p["Name"]), c.name(64, false)), "validateRequestBody": p["ValidateRequestBody"], "validateRequestParameters": p["ValidateRequestParameters"]}
			out, err := rest(c, "POST", "/restapis/"+str(p["RestApiId"])+"/requestvalidators", in)
			id := idOf("id")(out)
			return id, map[string]any{"RequestValidatorId": id}, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("apigateway", "DELETE", "/restapis/"+str(p["RestApiId"])+"/requestvalidators/"+phys, nil, nil)
		},
	})
	register("AWS::ApiGateway::GatewayResponse", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := map[string]any{"statusCode": p["StatusCode"], "responseParameters": p["ResponseParameters"], "responseTemplates": p["ResponseTemplates"]}
			_, err := rest(c, "PUT", "/restapis/"+str(p["RestApiId"])+"/gatewayresponses/"+str(p["ResponseType"]), in)
			return str(p["RestApiId"]) + "-" + str(p["ResponseType"]), nil, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("apigateway", "DELETE", "/restapis/"+str(p["RestApiId"])+"/gatewayresponses/"+str(p["ResponseType"]), nil, nil)
		},
	})
	register("AWS::ApiGatewayV2::DomainName", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			out, err := rest(c, "POST", "/v2/domainnames", body(p, nil))
			var rd, rz string
			if cfgs, _ := out["domainNameConfigurations"].([]any); len(cfgs) > 0 {
				m, _ := cfgs[0].(map[string]any)
				rd, rz = str(m["apiGatewayDomainName"]), str(m["hostedZoneId"])
			}
			return str(p["DomainName"]), map[string]any{"RegionalDomainName": rd, "RegionalHostedZoneId": rz}, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("apigateway", "DELETE", "/v2/domainnames/"+url.PathEscape(phys), nil, nil)
		},
	})
	register("AWS::ApiGatewayV2::ApiMapping", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			out, err := rest(c, "POST", "/v2/domainnames/"+url.PathEscape(str(p["DomainName"]))+"/apimappings", body(p, nil, "DomainName"))
			id := idOf("apiMappingId")(out)
			return id, map[string]any{"ApiMappingId": id}, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("apigateway", "DELETE", "/v2/domainnames/"+url.PathEscape(str(p["DomainName"]))+"/apimappings/"+phys, nil, nil)
		},
	})
	register("AWS::ApiGatewayV2::Deployment", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			out, err := rest(c, "POST", "/v2/apis/"+str(p["ApiId"])+"/deployments", pick(p, "Description=description", "StageName=stageName"))
			id := idOf("deploymentId")(out)
			return id, map[string]any{"DeploymentId": id}, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("apigateway", "DELETE", "/v2/apis/"+str(p["ApiId"])+"/deployments/"+phys, nil, nil)
		},
	})
}

func putCapacity(c *pctx, p map[string]any) error {
	return c.ecs("PutClusterCapacityProviders", map[string]any{"cluster": p["Cluster"], "capacityProviders": or2(p["CapacityProviders"], []any{}),
		"defaultCapacityProviderStrategy": or2(camel(p["DefaultCapacityProviderStrategy"], true), []any{})}, nil)
}
