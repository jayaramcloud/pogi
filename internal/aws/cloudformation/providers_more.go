package cloudformation

// More resource types, each over the API of the service that holds it: Logs,
// Secrets Manager, IAM, EventBridge, Step Functions, Cognito, Application Auto
// Scaling, CloudWatch and Kinesis.

const (
	logsT     = "Logs_20140328"
	cognitoT  = "AWSCognitoIdentityProviderService"
	identityT = "AWSCognitoIdentityService"
	eventsT   = "AWSEvents"
	sfnT      = "AWSStepFunctions"
	cwT       = "GraniteServiceVersion20100801"
	appAsT    = "AnyScaleFrontendService"
	kinesisT  = "Kinesis_20131202"
	secretsT  = "secretsmanager"
	iam       = "AWSIdentityManagementV20100508"
)

func init() {
	register("AWS::Logs::LogStream", withDel(simple(spec{target: logsT, create: "CreateLogStream", name: "LogStreamName", max: 512, del: "DeleteLogStream"}),
		logsT, "DeleteLogStream", func(phys string, p map[string]any) map[string]any {
			return map[string]any{"LogGroupName": p["LogGroupName"], "LogStreamName": phys}
		}))
	filter := func(put, del string) *provider {
		return withDel(simple(spec{target: logsT, create: put, update: put, name: "FilterName", max: 512, del: del, inPlace: []string{"FilterPattern", "MetricTransformations", "DestinationArn", "RoleArn", "Distribution", "ApplyOnTransformedLogs", "FilterName"}}),
			logsT, del, func(phys string, p map[string]any) map[string]any {
				return map[string]any{"LogGroupName": p["LogGroupName"], "FilterName": phys}
			})
	}
	register("AWS::Logs::MetricFilter", filter("PutMetricFilter", "DeleteMetricFilter"))
	register("AWS::Logs::SubscriptionFilter", filter("PutSubscriptionFilter", "DeleteSubscriptionFilter"))

	register("AWS::SecretsManager::ResourcePolicy", withDel(simple(spec{target: secretsT, create: "PutResourcePolicy", update: "PutResourcePolicy", del: "DeleteResourcePolicy",
		inPlace: []string{"ResourcePolicy", "BlockPublicPolicy"}, drop: []string{"BlockPublicPolicy"},
		build: func(c *pctx, p map[string]any, _ string) map[string]any {
			return map[string]any{"SecretId": p["SecretId"], "ResourcePolicy": jsonText(p["ResourcePolicy"])}
		}, id: func(_ string, out map[string]any) string { return dig(out, "ARN") }}),
		secretsT, "DeleteResourcePolicy", func(phys string, p map[string]any) map[string]any { return map[string]any{"SecretId": p["SecretId"]} }))

	// ---- IAM ----
	register("AWS::IAM::OIDCProvider", simple(spec{target: iam, create: "CreateOpenIDConnectProvider", del: "DeleteOpenIDConnectProvider", rename: map[string]string{"ClientIdList": "ClientIDList"},
		id: func(_ string, out map[string]any) string { return dig(out, "OpenIDConnectProviderArn") },
		delIn: func(phys string, _ map[string]any) map[string]any {
			return map[string]any{"OpenIDConnectProviderArn": phys}
		}}))
	register("AWS::IAM::ServiceLinkedRole", simple(spec{target: iam, create: "CreateServiceLinkedRole", del: "DeleteServiceLinkedRole",
		id:    func(_ string, out map[string]any) string { return dig(out, "Role", "RoleName") },
		delIn: func(phys string, _ map[string]any) map[string]any { return map[string]any{"RoleName": phys} }}))
	register("AWS::IAM::UserToGroupAddition", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			for _, u := range list(p["Users"]) {
				if err := c.call(iam, "AddUserToGroup", map[string]any{"GroupName": p["GroupName"], "UserName": u}, nil); err != nil {
					return "", nil, err
				}
			}
			return c.name(128, false), nil, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			for _, u := range list(p["Users"]) {
				if err := c.call(iam, "RemoveUserFromGroup", map[string]any{"GroupName": p["GroupName"], "UserName": u}, nil); err != nil && !notFound(err) {
					return err
				}
			}
			return nil
		},
	})

	// ---- EventBridge ----
	register("AWS::Events::EventBusPolicy", withDel(simple(spec{target: eventsT, create: "PutPermission", del: "RemovePermission", name: "StatementId", max: 64,
		build: func(c *pctx, p map[string]any, name string) map[string]any {
			in := pick(p, "EventBusName", "Action", "Principal", "Condition")
			if s, ok := p["Statement"]; ok {
				in["Policy"] = jsonString(map[string]any{"Version": "2012-10-17", "Statement": []any{withSid(s, name)}})
				delete(in, "Action")
				delete(in, "Principal")
			}
			return in
		}}), eventsT, "RemovePermission", func(phys string, p map[string]any) map[string]any {
		return map[string]any{"StatementId": phys, "EventBusName": p["EventBusName"]}
	}))
	register("AWS::Events::Archive", simple(spec{target: eventsT, create: "CreateArchive", update: "UpdateArchive", del: "DeleteArchive", name: "ArchiveName", max: 48, inPlace: []string{"Description", "EventPattern", "RetentionDays"},
		build: func(c *pctx, p map[string]any, _ string) map[string]any {
			in := pick(p, "Description", "RetentionDays", "SourceArn=EventSourceArn")
			if v, ok := p["EventPattern"]; ok {
				in["EventPattern"] = jsonText(v)
			}
			return in
		},
		attrs: func(c *pctx, phys string, _, out map[string]any) map[string]any {
			return map[string]any{"Arn": dig(out, "ArchiveArn"), "ArchiveName": phys}
		}}))
	register("AWS::Events::Connection", simple(spec{target: eventsT, create: "CreateConnection", update: "UpdateConnection", del: "DeleteConnection", name: "Name", max: 64,
		inPlace: []string{"Description", "AuthParameters", "AuthorizationType"},
		attrs: func(c *pctx, phys string, _, out map[string]any) map[string]any {
			return map[string]any{"Arn": dig(out, "ConnectionArn"), "SecretArn": dig(out, "SecretArn")}
		}}))
	register("AWS::Events::ApiDestination", simple(spec{target: eventsT, create: "CreateApiDestination", update: "UpdateApiDestination", del: "DeleteApiDestination", name: "Name", max: 64,
		inPlace: []string{"Description", "ConnectionArn", "InvocationEndpoint", "HttpMethod", "InvocationRateLimitPerSecond"},
		attrs: func(c *pctx, phys string, _, out map[string]any) map[string]any {
			return map[string]any{"Arn": dig(out, "ApiDestinationArn")}
		}}))

	// ---- Step Functions ----
	register("AWS::StepFunctions::Activity", simple(spec{target: sfnT, create: "CreateActivity", del: "DeleteActivity", name: "Name", max: 80,
		build: func(c *pctx, p map[string]any, _ string) map[string]any {
			return map[string]any{"Tags": tagList(p["Tags"])}
		},
		id:    func(_ string, out map[string]any) string { return dig(out, "activityArn") },
		delIn: func(phys string, _ map[string]any) map[string]any { return map[string]any{"activityArn": phys} },
		attrs: func(c *pctx, phys string, p, _ map[string]any) map[string]any {
			return map[string]any{"Name": lastSegment(phys), "Arn": phys}
		}}))
	register("AWS::StepFunctions::StateMachineVersion", simple(spec{target: sfnT, create: "PublishStateMachineVersion", del: "DeleteStateMachineVersion",
		build: func(c *pctx, p map[string]any, _ string) map[string]any {
			return map[string]any{"StateMachineArn": p["StateMachineArn"], "RevisionId": p["StateMachineRevisionId"], "Description": p["Description"]}
		},
		id: func(_ string, out map[string]any) string { return dig(out, "stateMachineVersionArn") },
		delIn: func(phys string, _ map[string]any) map[string]any {
			return map[string]any{"stateMachineVersionArn": phys}
		},
		attrs: func(c *pctx, phys string, p, _ map[string]any) map[string]any { return map[string]any{"Arn": phys} }}))
	aliasRoutes := func(p map[string]any) any {
		if r, ok := p["RoutingConfiguration"]; ok {
			return r
		}
		if d, _ := p["DeploymentPreference"].(map[string]any); d != nil {
			return []any{map[string]any{"StateMachineVersionArn": d["StateMachineVersionArn"], "Weight": 100}}
		}
		return nil
	}
	register("AWS::StepFunctions::StateMachineAlias", simple(spec{target: sfnT, create: "CreateStateMachineAlias", del: "DeleteStateMachineAlias", name: "Name", max: 80,
		update: "UpdateStateMachineAlias", updKey: "StateMachineAliasArn", inPlace: []string{"Description", "RoutingConfiguration", "DeploymentPreference"}, key: "-",
		build: func(c *pctx, p map[string]any, name string) map[string]any {
			return map[string]any{"Name": name, "Description": p["Description"], "RoutingConfiguration": aliasRoutes(p)}
		},
		id: func(_ string, out map[string]any) string { return dig(out, "stateMachineAliasArn") },
		delIn: func(phys string, _ map[string]any) map[string]any {
			return map[string]any{"StateMachineAliasArn": phys}
		},
		attrs: func(c *pctx, phys string, p, _ map[string]any) map[string]any { return map[string]any{"Arn": phys} }}))

	// ---- Cognito ----
	register("AWS::Cognito::UserPoolDomain", withDel(simple(spec{target: cognitoT, create: "CreateUserPoolDomain", del: "DeleteUserPoolDomain", name: "Domain", max: 63, lower: true}),
		cognitoT, "DeleteUserPoolDomain", func(phys string, p map[string]any) map[string]any {
			return map[string]any{"UserPoolId": p["UserPoolId"], "Domain": phys}
		}))
	register("AWS::Cognito::UserPoolResourceServer", withDel(simple(spec{target: cognitoT, create: "CreateResourceServer", update: "UpdateResourceServer", del: "DeleteResourceServer", name: "Identifier", max: 256,
		inPlace: []string{"Name", "Scopes"}}), cognitoT, "DeleteResourceServer", func(phys string, p map[string]any) map[string]any {
		return map[string]any{"UserPoolId": p["UserPoolId"], "Identifier": phys}
	}))
	register("AWS::Cognito::UserPoolIdentityProvider", withDel(simple(spec{target: cognitoT, create: "CreateIdentityProvider", update: "UpdateIdentityProvider", del: "DeleteIdentityProvider", name: "ProviderName", max: 32,
		inPlace: []string{"ProviderDetails", "AttributeMapping", "IdpIdentifiers"}}), cognitoT, "DeleteIdentityProvider", func(phys string, p map[string]any) map[string]any {
		return map[string]any{"UserPoolId": p["UserPoolId"], "ProviderName": phys}
	}))
	register("AWS::Cognito::UserPoolUser", withDel(simple(spec{target: cognitoT, create: "AdminCreateUser", del: "AdminDeleteUser", name: "Username", max: 128,
		drop: []string{"ValidationData", "ClientMetadata"}, build: func(c *pctx, p map[string]any, _ string) map[string]any {
			return pick(p, "UserPoolId", "UserAttributes", "ValidationData", "DesiredDeliveryMediums", "ForceAliasCreation", "MessageAction")
		}}), cognitoT, "AdminDeleteUser", func(phys string, p map[string]any) map[string]any {
		return map[string]any{"UserPoolId": p["UserPoolId"], "Username": phys}
	}))
	register("AWS::Cognito::UserPoolUserToGroupAttachment", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			return c.name(128, false), nil, c.call(cognitoT, "AdminAddUserToGroup", pick(p, "UserPoolId", "Username", "GroupName"), nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call(cognitoT, "AdminRemoveUserFromGroup", pick(p, "UserPoolId", "Username", "GroupName"), nil)
		},
	})
	register("AWS::Cognito::IdentityPool", simple(spec{target: identityT, create: "CreateIdentityPool", update: "UpdateIdentityPool", del: "DeleteIdentityPool", name: "IdentityPoolName", max: 128,
		updKey: "IdentityPoolId", inPlace: []string{"AllowUnauthenticatedIdentities", "AllowClassicFlow", "CognitoIdentityProviders", "DeveloperProviderName", "OpenIdConnectProviderARNs",
			"SamlProviderARNs", "SupportedLoginProviders", "IdentityPoolTags"},
		build: func(c *pctx, p map[string]any, name string) map[string]any {
			in := pick(p, "AllowUnauthenticatedIdentities", "AllowClassicFlow", "CognitoIdentityProviders", "DeveloperProviderName", "OpenIdConnectProviderARNs",
				"SamlProviderARNs", "SupportedLoginProviders")
			in["IdentityPoolName"] = or(or(name, str(p["IdentityPoolName"])), c.logical)
			if t := tagMap(p["IdentityPoolTags"]); len(t) > 0 {
				in["IdentityPoolTags"] = t
			}
			return in
		},
		id:    func(_ string, out map[string]any) string { return dig(out, "IdentityPoolId") },
		delIn: func(phys string, _ map[string]any) map[string]any { return map[string]any{"IdentityPoolId": phys} },
		attrs: func(c *pctx, phys string, p, out map[string]any) map[string]any {
			return map[string]any{"Id": phys, "Name": dig(out, "IdentityPoolName")}
		}}))
	register("AWS::Cognito::IdentityPoolRoleAttachment", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			return str(p["IdentityPoolId"]), nil, c.call(identityT, "SetIdentityPoolRoles", pick(p, "IdentityPoolId", "Roles", "RoleMappings"), nil)
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(old["IdentityPoolId"]) != str(p["IdentityPoolId"]) {
				return nil, true, nil
			}
			return nil, false, c.call(identityT, "SetIdentityPoolRoles", pick(p, "IdentityPoolId", "Roles", "RoleMappings"), nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call(identityT, "SetIdentityPoolRoles", map[string]any{"IdentityPoolId": phys, "Roles": map[string]any{}}, nil)
		},
	})

	// ---- CloudWatch ----
	register("AWS::CloudWatch::CompositeAlarm", simple(spec{target: cwT, create: "PutCompositeAlarm", update: "PutCompositeAlarm", del: "DeleteAlarms", name: "AlarmName", max: 255,
		inPlace: []string{"AlarmRule", "AlarmDescription", "ActionsEnabled", "AlarmActions", "OKActions", "InsufficientDataActions", "ActionsSuppressor"},
		delIn: func(phys string, _ map[string]any) map[string]any {
			return map[string]any{"AlarmNames": []string{phys}}
		},
		attrs: func(c *pctx, phys string, _, _ map[string]any) map[string]any {
			return map[string]any{"Arn": c.arn("cloudwatch", "alarm:"+phys)}
		}}))
	register("AWS::CloudWatch::Dashboard", simple(spec{target: cwT, create: "PutDashboard", update: "PutDashboard", del: "DeleteDashboards", name: "DashboardName", max: 255,
		inPlace: []string{"DashboardBody"},
		delIn: func(phys string, _ map[string]any) map[string]any {
			return map[string]any{"DashboardNames": []string{phys}}
		}}))

	// ---- Kinesis ----
	register("AWS::Kinesis::StreamConsumer", simple(spec{target: kinesisT, create: "RegisterStreamConsumer", del: "DeregisterStreamConsumer", name: "ConsumerName", max: 128,
		build: func(c *pctx, p map[string]any, _ string) map[string]any {
			return map[string]any{"StreamARN": p["StreamARN"]}
		},
		id:    func(_ string, out map[string]any) string { return dig(out, "Consumer", "ConsumerARN") },
		delIn: func(phys string, _ map[string]any) map[string]any { return map[string]any{"ConsumerARN": phys} },
		attrs: func(c *pctx, phys string, p, out map[string]any) map[string]any {
			return map[string]any{"ConsumerARN": phys, "ConsumerName": dig(out, "Consumer", "ConsumerName"), "ConsumerStatus": dig(out, "Consumer", "ConsumerStatus"),
				"StreamARN": str(p["StreamARN"])}
		}}))
}

// withDel swaps the delete call of a simple provider for one that needs more than the name.
func withDel(p *provider, target, op string, in func(phys string, p map[string]any) map[string]any) *provider {
	p.del = func(c *pctx, phys string, props map[string]any) error {
		return c.call(target, op, in(phys, props), nil)
	}
	return p
}

func withSid(statement any, sid string) any {
	if m, ok := statement.(map[string]any); ok {
		m["Sid"] = sid
	}
	return statement
}

// tablePITR turns point-in-time recovery on or off as a table's properties say.
func tablePITR(c *pctx, table string, p map[string]any) error {
	spec, ok := p["PointInTimeRecoverySpecification"].(map[string]any)
	if !ok {
		return nil
	}
	return c.call("DynamoDB_20120810", "UpdateContinuousBackups", map[string]any{"TableName": table,
		"PointInTimeRecoverySpecification": map[string]any{"PointInTimeRecoveryEnabled": spec["PointInTimeRecoveryEnabled"]}}, nil)
}

const ddbT = "DynamoDB_20120810"

func init() {
	// A global table is a table in this region with replicas in the others.
	replicaRegions := func(c *pctx, p map[string]any) []string {
		var out []string
		for _, r := range listMaps(p["Replicas"]) {
			if reg := str(r["Region"]); reg != c.region {
				out = append(out, reg)
			}
		}
		return out
	}
	tableProps := func(p map[string]any) map[string]any {
		in := pick(p, "TableName", "AttributeDefinitions", "KeySchema", "BillingMode", "SSESpecification", "StreamSpecification", "TimeToLiveSpecification", "Tags")
		if in["BillingMode"] == nil {
			in["BillingMode"] = "PAY_PER_REQUEST"
		}
		if t, ok := p["WriteProvisionedThroughputSettings"].(map[string]any); ok && in["BillingMode"] == "PROVISIONED" {
			if cap, ok := t["WriteCapacityAutoScalingSettings"].(map[string]any); ok {
				in["ProvisionedThroughput"] = map[string]any{"ReadCapacityUnits": cap["MinCapacity"], "WriteCapacityUnits": cap["MinCapacity"]}
			}
		}
		var gsis []any
		for _, g := range listMaps(p["GlobalSecondaryIndexes"]) {
			gsis = append(gsis, pick(g, "IndexName", "KeySchema", "Projection"))
		}
		if gsis != nil {
			in["GlobalSecondaryIndexes"] = gsis
		}
		if r := listMaps(p["Replicas"]); len(r) > 0 {
			if pit, ok := r[0]["PointInTimeRecoverySpecification"]; ok {
				in["PointInTimeRecoverySpecification"] = pit
			}
		}
		return in
	}
	register("AWS::DynamoDB::GlobalTable", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := tableProps(p)
			name, attrs, err := providers["AWS::DynamoDB::Table"].create(c, in)
			if err != nil {
				return "", nil, err
			}
			for _, reg := range replicaRegions(c, p) {
				if err := c.call(ddbT, "UpdateTable", map[string]any{"TableName": name, "ReplicaUpdates": []any{map[string]any{"Create": map[string]any{"RegionName": reg}}}}, nil); err != nil {
					c.call(ddbT, "DeleteTable", map[string]any{"TableName": name}, nil)
					return "", nil, err
				}
			}
			return name, attrs, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(old["TableName"]) != str(p["TableName"]) || !equal(old["KeySchema"], p["KeySchema"]) {
				return nil, true, nil
			}
			attrs, _, err := providers["AWS::DynamoDB::Table"].update(c, phys, tableProps(old), tableProps(p))
			if err != nil {
				return nil, false, err
			}
			oldR, newR := replicaRegions(c, old), replicaRegions(c, p)
			var ups []any
			for _, r := range minus(newR, oldR) {
				ups = append(ups, map[string]any{"Create": map[string]any{"RegionName": r}})
			}
			for _, r := range minus(oldR, newR) {
				ups = append(ups, map[string]any{"Delete": map[string]any{"RegionName": r}})
			}
			if len(ups) > 0 {
				err = c.call(ddbT, "UpdateTable", map[string]any{"TableName": phys, "ReplicaUpdates": ups}, nil)
			}
			return attrs, false, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call(ddbT, "DeleteTable", map[string]any{"TableName": phys}, nil)
		},
	})

	register("AWS::SNS::TopicInlinePolicy", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			return str(p["TopicArn"]), nil, setTopicPolicy(c, p["TopicArn"], jsonString(p["PolicyDocument"]))
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(old["TopicArn"]) != str(p["TopicArn"]) {
				return nil, true, nil
			}
			return nil, false, setTopicPolicy(c, p["TopicArn"], jsonString(p["PolicyDocument"]))
		},
		del: func(c *pctx, phys string, p map[string]any) error { return nil },
	})

	rotation := func(p map[string]any) map[string]any {
		in := map[string]any{"SecretId": p["SecretId"], "RotationLambdaARN": p["RotationLambdaARN"], "RotationRules": p["RotationRules"]}
		if v, ok := p["RotationLambdaArn"]; ok {
			in["RotationLambdaARN"] = v
		}
		if v, ok := p["RotateImmediatelyOnUpdate"]; ok {
			in["RotateImmediately"] = v
		}
		return in
	}
	register("AWS::SecretsManager::RotationSchedule", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			if p["HostedRotationLambda"] != nil {
				return "", nil, validation("pogi does not run hosted rotation functions yet: give RotationLambdaArn")
			}
			return str(p["SecretId"]), nil, c.call(secretsT, "RotateSecret", rotation(p), nil)
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(old["SecretId"]) != str(p["SecretId"]) {
				return nil, true, nil
			}
			return nil, false, c.call(secretsT, "RotateSecret", rotation(p), nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call(secretsT, "CancelRotateSecret", map[string]any{"SecretId": phys}, nil)
		},
	})
}

func setTopicPolicy(c *pctx, topic any, policy string) error {
	return c.call("AmazonSimpleNotificationService", "SetTopicAttributes", map[string]any{"TopicArn": topic, "AttributeName": "Policy", "AttributeValue": policy}, nil)
}
