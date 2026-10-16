package cloudformation

import (
	"encoding/base64"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// AppConfig, Amazon MQ, EKS, MSK, IoT and ECS task sets.

func init() {
	// ---- AppConfig ----
	appBody := func(p map[string]any, drop ...string) map[string]any {
		in := map[string]any{}
		for k, v := range p {
			in[k] = v
		}
		for _, k := range drop {
			delete(in, k)
		}
		if t, ok := p["Tags"]; ok {
			in["Tags"] = tagMap(t)
		}
		return in
	}
	apc := func(c *pctx, method, path string, body any) (map[string]any, error) {
		out := map[string]any{}
		return out, c.rest("appconfig", method, path, body, &out)
	}
	register("AWS::AppConfig::Application", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			out, err := apc(c, "POST", "/applications", appBody(p))
			return dig(out, "Id"), map[string]any{"ApplicationId": dig(out, "Id")}, err
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, "Name", "Description") {
				_, err := apc(c, "PATCH", "/applications/"+phys, pick(p, "Name", "Description"))
				return nil, false, err
			}
			return nil, true, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("appconfig", "DELETE", "/applications/"+phys, nil, nil)
		},
	})
	register("AWS::AppConfig::Environment", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			out, err := apc(c, "POST", "/applications/"+str(p["ApplicationId"])+"/environments", appBody(p, "ApplicationId"))
			return dig(out, "Id"), map[string]any{"EnvironmentId": dig(out, "Id")}, err
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(old["ApplicationId"]) != str(p["ApplicationId"]) || !equal(old["Tags"], p["Tags"]) {
				return nil, true, nil
			}
			_, err := apc(c, "PATCH", "/applications/"+str(p["ApplicationId"])+"/environments/"+phys, pick(p, "Name", "Description", "Monitors"))
			return nil, false, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("appconfig", "DELETE", "/applications/"+str(p["ApplicationId"])+"/environments/"+phys, nil, nil)
		},
	})
	register("AWS::AppConfig::ConfigurationProfile", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			out, err := apc(c, "POST", "/applications/"+str(p["ApplicationId"])+"/configurationprofiles", appBody(p, "ApplicationId"))
			return dig(out, "Id"), map[string]any{"ConfigurationProfileId": dig(out, "Id")}, err
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(pick(old, "ApplicationId", "LocationUri", "Type", "Tags"), pick(p, "ApplicationId", "LocationUri", "Type", "Tags")) {
				return nil, true, nil
			}
			_, err := apc(c, "PATCH", "/applications/"+str(p["ApplicationId"])+"/configurationprofiles/"+phys, pick(p, "Name", "Description", "RetrievalRoleArn", "Validators", "KmsKeyIdentifier"))
			return nil, false, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("appconfig", "DELETE", "/applications/"+str(p["ApplicationId"])+"/configurationprofiles/"+phys, nil, nil)
		},
	})
	register("AWS::AppConfig::DeploymentStrategy", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := appBody(p)
			if in["Name"] == nil {
				in["Name"] = c.name(64, false)
			}
			out, err := apc(c, "POST", "/deploymentstrategies", in)
			return dig(out, "Id"), map[string]any{"Id": dig(out, "Id")}, err
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(old["Name"]) != str(p["Name"]) || str(old["ReplicateTo"]) != str(p["ReplicateTo"]) || !equal(old["Tags"], p["Tags"]) {
				return nil, true, nil
			}
			_, err := apc(c, "PATCH", "/deploymentstrategies/"+phys, pick(p, "Description", "DeploymentDurationInMinutes", "FinalBakeTimeInMinutes", "GrowthFactor", "GrowthType"))
			return nil, false, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("appconfig", "DELETE", "/deploymentstrategies/"+phys, nil, nil)
		},
	})
	hosted := func(p map[string]any) string {
		return "/applications/" + str(p["ApplicationId"]) + "/configurationprofiles/" + str(p["ConfigurationProfileId"]) + "/hostedconfigurationversions"
	}
	register("AWS::AppConfig::HostedConfigurationVersion", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			h := http.Header{"Content-Type": {str(p["ContentType"])}}
			for k, v := range map[string]string{"Description": "Description", "VersionLabel": "VersionLabel", "LatestVersionNumber": "Latest-Version-Number"} {
				if s := str(p[k]); s != "" {
					h.Set(v, s)
				}
			}
			rh, _, err := c.restRaw("appconfig", "POST", hosted(p), h, []byte(str(p["Content"])))
			if err != nil {
				return "", nil, err
			}
			n := rh.Get("Version-Number")
			return str(p["ApplicationId"]) + "/" + str(p["ConfigurationProfileId"]) + "/" + n, map[string]any{"VersionNumber": n}, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("appconfig", "DELETE", hosted(p)+"/"+lastSegment(phys), nil, nil)
		},
	})
	register("AWS::AppConfig::Deployment", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			path := "/applications/" + str(p["ApplicationId"]) + "/environments/" + str(p["EnvironmentId"]) + "/deployments"
			in := appBody(p, "ApplicationId", "EnvironmentId")
			var out struct {
				DeploymentNumber int
				State            string
			}
			if err := c.rest("appconfig", "POST", path, in, &out); err != nil {
				return "", nil, err
			}
			n := strconv.Itoa(out.DeploymentNumber)
			// the deployment is done when it leaves DEPLOYING and BAKING
			waitFor(time.Minute, func() bool {
				return c.rest("appconfig", "GET", path+"/"+n, nil, &out) != nil || (out.State != "DEPLOYING" && out.State != "BAKING")
			})
			return n, map[string]any{"DeploymentNumber": n}, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error { return nil }, // AppConfig has no delete for a deployment
	})

	// ---- Amazon MQ ----
	brokerBody := func(p map[string]any) map[string]any {
		in := camel(pick(p, "EngineType", "EngineVersion", "HostInstanceType", "DeploymentMode", "Users", "PubliclyAccessible", "AutoMinorVersionUpgrade", "SubnetIds",
			"SecurityGroups", "Configuration", "Logs", "MaintenanceWindowStartTime", "EncryptionOptions", "AuthenticationStrategy", "LdapServerMetadata", "StorageType"), false).(map[string]any)
		in["tags"] = tagMap(p["Tags"])
		return in
	}
	register("AWS::AmazonMQ::Broker", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := brokerBody(p)
			in["brokerName"] = or(str(p["BrokerName"]), c.name(50, false))
			var out struct{ BrokerId, BrokerArn string }
			if err := c.rest("mq", "POST", "/v1/brokers", in, &out); err != nil {
				return "", nil, err
			}
			var d struct {
				BrokerState    string
				Configurations struct {
					Current struct {
						Id       string
						Revision int
					}
				}
				BrokerInstances []struct{ Endpoints []string }
			}
			waitFor(2*time.Minute, func() bool {
				return c.rest("mq", "GET", "/v1/brokers/"+out.BrokerId, nil, &d) != nil || d.BrokerState == "RUNNING" || d.BrokerState == "CREATION_FAILED"
			})
			attrs := map[string]any{"Arn": out.BrokerArn, "Id": out.BrokerId, "ConfigurationId": d.Configurations.Current.Id, "ConfigurationRevision": strconv.Itoa(d.Configurations.Current.Revision)}
			var eps []any
			for _, i := range d.BrokerInstances {
				for _, e := range i.Endpoints {
					eps = append(eps, e)
				}
			}
			attrs["AmqpEndpoints"], attrs["OpenWireEndpoints"], attrs["StompEndpoints"], attrs["MqttEndpoints"], attrs["WssEndpoints"] = endpointsOf(eps, "amqp"), endpointsOf(eps, "ssl"),
				endpointsOf(eps, "stomp"), endpointsOf(eps, "mqtt"), endpointsOf(eps, "wss")
			return out.BrokerId, attrs, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			inPlace := []string{"AutoMinorVersionUpgrade", "Configuration", "EngineVersion", "HostInstanceType", "Logs", "MaintenanceWindowStartTime", "SecurityGroups", "Tags"}
			if differs(old, p, inPlace...) {
				return nil, true, nil
			}
			in := camel(pick(p, inPlace[:len(inPlace)-1]...), false).(map[string]any)
			return nil, false, c.rest("mq", "PUT", "/v1/brokers/"+phys, in, nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("mq", "DELETE", "/v1/brokers/"+phys, nil, nil)
		},
	})

	// ---- EKS ----
	clusterBody := func(p map[string]any) map[string]any {
		in := camel(pick(p, "RoleArn", "ResourcesVpcConfig", "Version", "Logging", "EncryptionConfig", "KubernetesNetworkConfig", "AccessConfig"), false).(map[string]any)
		if t := tagMap(p["Tags"]); len(t) > 0 {
			in["tags"] = t
		}
		return in
	}
	register("AWS::EKS::Cluster", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := clusterBody(p)
			name := or(str(p["Name"]), c.name(100, false))
			in["name"] = name
			if err := c.rest("eks", "POST", "/clusters", in, nil); err != nil {
				return "", nil, err
			}
			var d struct {
				Cluster map[string]any
			}
			waitFor(3*time.Minute, func() bool {
				return c.rest("eks", "GET", "/clusters/"+name, nil, &d) != nil || str(d.Cluster["status"]) != "CREATING"
			})
			vpc, _ := d.Cluster["resourcesVpcConfig"].(map[string]any)
			ca, _ := d.Cluster["certificateAuthority"].(map[string]any)
			oidc, _ := d.Cluster["identity"].(map[string]any)
			iss := ""
			if o, ok := oidc["oidc"].(map[string]any); ok {
				iss = str(o["issuer"])
			}
			return name, map[string]any{"Arn": d.Cluster["arn"], "Endpoint": d.Cluster["endpoint"], "CertificateAuthorityData": ca["data"],
				"ClusterSecurityGroupId": vpc["clusterSecurityGroupId"], "OpenIdConnectIssuerUrl": iss}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(old, p, "Version", "Logging", "ResourcesVpcConfig") {
				return nil, true, nil
			}
			if !equal(old["Version"], p["Version"]) {
				if err := c.rest("eks", "POST", "/clusters/"+phys+"/updates", map[string]any{"version": p["Version"]}, nil); err != nil {
					return nil, false, err
				}
			}
			cfg := camel(pick(p, "Logging", "ResourcesVpcConfig"), false)
			if !equal(old["Logging"], p["Logging"]) || !equal(old["ResourcesVpcConfig"], p["ResourcesVpcConfig"]) {
				return nil, false, c.rest("eks", "POST", "/clusters/"+phys+"/update-config", cfg, nil)
			}
			return nil, false, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("eks", "DELETE", "/clusters/"+phys, nil, nil)
		},
	})

	// ---- MSK ----
	register("AWS::MSK::Cluster", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := camel(pick(p, "KafkaVersion", "NumberOfBrokerNodes", "BrokerNodeGroupInfo", "EncryptionInfo", "ClientAuthentication", "ConfigurationInfo", "EnhancedMonitoring",
				"LoggingInfo", "OpenMonitoring", "StorageMode"), false).(map[string]any)
			in["clusterName"] = or(str(p["ClusterName"]), c.name(64, false))
			if t, ok := p["Tags"].(map[string]any); ok {
				in["tags"] = t
			}
			var out struct{ ClusterArn string }
			if err := c.rest("kafka", "POST", "/v1/clusters", in, &out); err != nil {
				return "", nil, err
			}
			waitFor(2*time.Minute, func() bool {
				var d struct{ ClusterInfo struct{ State string } }
				return c.rest("kafka", "GET", "/v1/clusters/"+url.PathEscape(out.ClusterArn), nil, &d) != nil || d.ClusterInfo.State != "CREATING"
			})
			return out.ClusterArn, map[string]any{"Arn": out.ClusterArn}, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("kafka", "DELETE", "/v1/clusters/"+url.PathEscape(phys), nil, nil)
		},
	})
	register("AWS::MSK::Configuration", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := map[string]any{"name": or(str(p["Name"]), c.name(128, false)), "description": p["Description"], "kafkaVersions": p["KafkaVersionsList"],
				"serverProperties": base64.StdEncoding.EncodeToString([]byte(str(p["ServerProperties"])))}
			var out struct{ Arn string }
			err := c.rest("kafka", "POST", "/v1/configurations", in, &out)
			return out.Arn, map[string]any{"Arn": out.Arn}, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("kafka", "DELETE", "/v1/configurations/"+url.PathEscape(phys), nil, nil)
		},
	})

	// ---- IoT ----
	register("AWS::IoT::Thing", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := or(str(p["ThingName"]), c.name(128, false))
			in := map[string]any{}
			if ap, ok := p["AttributePayload"].(map[string]any); ok {
				in["attributePayload"] = map[string]any{"attributes": ap["Attributes"]}
			}
			var out struct{ ThingArn, ThingId string }
			if err := c.rest("iot", "POST", "/things/"+url.PathEscape(name), in, &out); err != nil {
				return "", nil, err
			}
			return name, map[string]any{"Arn": out.ThingArn, "Id": out.ThingId}, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("iot", "DELETE", "/things/"+url.PathEscape(phys), nil, nil)
		},
	})
	register("AWS::IoT::Policy", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := or(str(p["PolicyName"]), c.name(128, false))
			var out struct{ PolicyArn string }
			if err := c.rest("iot", "POST", "/policies/"+url.PathEscape(name), map[string]any{"policyDocument": jsonString(p["PolicyDocument"])}, &out); err != nil {
				return "", nil, err
			}
			return name, map[string]any{"Arn": out.PolicyArn, "Id": name}, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("iot", "DELETE", "/policies/"+url.PathEscape(phys), nil, nil)
		},
	})
	rule := func(p map[string]any) map[string]any {
		pl, _ := p["TopicRulePayload"].(map[string]any)
		in := camel(pl, false).(map[string]any)
		return in
	}
	register("AWS::IoT::TopicRule", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := or(str(p["RuleName"]), c.name(128, false))
			if err := c.rest("iot", "POST", "/rules/"+url.PathEscape(name), rule(p), nil); err != nil {
				return "", nil, err
			}
			return name, map[string]any{"Arn": c.arn("iot", "rule/"+name)}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(old["RuleName"]) != str(p["RuleName"]) {
				return nil, true, nil
			}
			return nil, false, c.rest("iot", "PATCH", "/rules/"+url.PathEscape(phys), rule(p), nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("iot", "DELETE", "/rules/"+url.PathEscape(phys), nil, nil)
		},
	})

	// ---- ECS task sets ----
	register("AWS::ECS::TaskSet", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := camel(p, true).(map[string]any)
			var out struct{ TaskSet struct{ Id string } }
			if err := c.ecs("CreateTaskSet", in, &out); err != nil {
				return "", nil, err
			}
			return out.TaskSet.Id, map[string]any{"Id": out.TaskSet.Id}, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.ecs("DeleteTaskSet", map[string]any{"cluster": p["Cluster"], "service": p["Service"], "taskSet": phys, "force": true}, nil)
		},
	})
	register("AWS::ECS::PrimaryTaskSet", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			return str(p["Service"]), nil, c.ecs("UpdateServicePrimaryTaskSet", map[string]any{"cluster": p["Cluster"], "service": p["Service"], "primaryTaskSet": p["TaskSetId"]}, nil)
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			return nil, false, c.ecs("UpdateServicePrimaryTaskSet", map[string]any{"cluster": p["Cluster"], "service": p["Service"], "primaryTaskSet": p["TaskSetId"]}, nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error { return nil },
	})
}

// endpointsOf lists the endpoints of a broker that start with a scheme.
func endpointsOf(all []any, scheme string) []any {
	out := []any{}
	for _, e := range all {
		if s := str(e); len(s) > len(scheme) && s[:len(scheme)] == scheme {
			out = append(out, s)
		}
	}
	return out
}
