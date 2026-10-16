package cloudformation

import (
	"fmt"
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// samStmt is one statement of a SAM policy template. Actions and resources are
// space separated; ${Name} in a resource is a parameter of the template.
type samStmt struct{ actions, resources string }

const (
	samSQS   = "arn:${AWS::Partition}:sqs:${AWS::Region}:${AWS::AccountId}:"
	samS3    = "arn:${AWS::Partition}:s3:::"
	samDDB   = "arn:${AWS::Partition}:dynamodb:${AWS::Region}:${AWS::AccountId}:table/"
	samKin   = "arn:${AWS::Partition}:kinesis:${AWS::Region}:${AWS::AccountId}:stream/"
	samRegAc = "${AWS::Region}:${AWS::AccountId}"
)

// samPolicyTemplates are the SAM policy templates pogi expands. Statements
// follow the ones AWS publishes for each template.
var samPolicyTemplates = map[string][]samStmt{
	"SQSPollerPolicy":      {{"sqs:ChangeMessageVisibility sqs:ChangeMessageVisibilityBatch sqs:DeleteMessage sqs:DeleteMessageBatch sqs:GetQueueAttributes sqs:ReceiveMessage", samSQS + "${QueueName}"}},
	"SQSSendMessagePolicy": {{"sqs:SendMessage*", samSQS + "${QueueName}"}},
	"S3ReadPolicy":         {{"s3:GetObject s3:GetObjectAcl s3:GetObjectVersion s3:GetLifecycleConfiguration s3:ListBucket", samS3 + "${BucketName} " + samS3 + "${BucketName}/*"}},
	"S3WritePolicy":        {{"s3:PutObject s3:PutObjectAcl s3:PutLifecycleConfiguration", samS3 + "${BucketName} " + samS3 + "${BucketName}/*"}},
	"S3CrudPolicy": {{"s3:GetObject s3:GetObjectAcl s3:GetObjectVersion s3:PutObject s3:PutObjectAcl s3:DeleteObject s3:DeleteObjectVersion s3:PutLifecycleConfiguration s3:ListBucket",
		samS3 + "${BucketName} " + samS3 + "${BucketName}/*"}},
	"DynamoDBCrudPolicy": {{"dynamodb:GetItem dynamodb:DeleteItem dynamodb:PutItem dynamodb:Scan dynamodb:Query dynamodb:UpdateItem dynamodb:BatchWriteItem dynamodb:BatchGetItem dynamodb:DescribeTable dynamodb:ConditionCheckItem",
		samDDB + "${TableName} " + samDDB + "${TableName}/index/*"}},
	"DynamoDBReadPolicy":  {{"dynamodb:GetItem dynamodb:Scan dynamodb:Query dynamodb:BatchGetItem dynamodb:DescribeTable", samDDB + "${TableName} " + samDDB + "${TableName}/index/*"}},
	"DynamoDBWritePolicy": {{"dynamodb:PutItem dynamodb:UpdateItem dynamodb:BatchWriteItem", samDDB + "${TableName} " + samDDB + "${TableName}/index/*"}},
	"DynamoDBStreamReadPolicy": {
		{"dynamodb:DescribeStream dynamodb:GetRecords dynamodb:GetShardIterator", samDDB + "${TableName}/stream/${StreamName}"},
		{"dynamodb:ListStreams", samDDB + "${TableName}/stream/*"},
	},
	"SNSPublishMessagePolicy":      {{"sns:Publish", "arn:${AWS::Partition}:sns:" + samRegAc + ":${TopicName}"}},
	"LambdaInvokePolicy":           {{"lambda:InvokeFunction", "arn:${AWS::Partition}:lambda:" + samRegAc + ":function:${FunctionName}"}},
	"StepFunctionsExecutionPolicy": {{"states:StartExecution", "arn:${AWS::Partition}:states:" + samRegAc + ":stateMachine:${StateMachineName}"}},
	"KinesisCrudPolicy": {{"kinesis:AddTagsToStream kinesis:CreateStream kinesis:DecreaseStreamRetentionPeriod kinesis:DeleteStream kinesis:DescribeStream kinesis:DescribeStreamSummary kinesis:GetShardIterator kinesis:IncreaseStreamRetentionPeriod kinesis:ListTagsForStream kinesis:MergeShards kinesis:PutRecord kinesis:PutRecords kinesis:SplitShard kinesis:RemoveTagsFromStream kinesis:GetRecords kinesis:ListShards",
		samKin + "${StreamName}"}},
	"KinesisStreamReadPolicy": {
		{"kinesis:ListStreams kinesis:DescribeLimits", "*"},
		{"kinesis:DescribeStream kinesis:DescribeStreamSummary kinesis:GetRecords kinesis:GetShardIterator", samKin + "${StreamName}"},
	},
	"SSMParameterReadPolicy": {
		{"ssm:DescribeParameters", "*"},
		{"ssm:GetParameters ssm:GetParameter ssm:GetParametersByPath ssm:GetParameterHistory", "arn:${AWS::Partition}:ssm:" + samRegAc + ":parameter/${ParameterName}"},
	},
	"AWSSecretsManagerGetSecretValuePolicy": {{"secretsmanager:GetSecretValue", "${SecretArn}"}},
	"KMSDecryptPolicy":                      {{"kms:Decrypt", "arn:${AWS::Partition}:kms:" + samRegAc + ":key/${KeyId}"}},
	"EventBridgePutEventsPolicy":            {{"events:PutEvents", "arn:${AWS::Partition}:events:" + samRegAc + ":event-bus/${EventBusName}"}},
	"CloudWatchPutMetricPolicy":             {{"cloudwatch:PutMetricData", "*"}},
	"VPCAccessPolicy":                       {{"ec2:CreateNetworkInterface ec2:DeleteNetworkInterface ec2:DescribeNetworkInterfaces ec2:DetachNetworkInterface", "*"}},
}

// samFill puts the template parameters into s. A parameter that is a plain
// string is written in place; anything else (a Ref, a GetAtt) goes through Fn::Sub.
func samFill(s string, params map[string]any) any {
	vars := map[string]any{}
	out := subVar.ReplaceAllStringFunc(s, func(m string) string {
		name := m[2 : len(m)-1]
		if strings.Contains(name, "::") {
			return m
		}
		if v, ok := params[name].(string); ok && !strings.Contains(v, "${") {
			return v
		}
		vars[name] = params[name]
		return m
	})
	switch {
	case len(vars) > 0:
		return map[string]any{"Fn::Sub": []any{out, vars}}
	case strings.Contains(out, "${"):
		return map[string]any{"Fn::Sub": out}
	}
	return out
}

// samPolicyDoc expands one policy template call, {Name: {Param: value}}.
func samPolicyDoc(name string, params any) (map[string]any, error) {
	stmts, ok := samPolicyTemplates[name]
	if !ok {
		return nil, validation("pogi does not support the SAM policy template %s yet", name)
	}
	p, _ := params.(map[string]any)
	var out []any
	for _, st := range stmts {
		for _, m := range subVar.FindAllStringSubmatch(st.resources, -1) {
			if !strings.Contains(m[1], "::") && p[m[1]] == nil {
				return nil, validation("Policy template %s needs the parameter %s", name, m[1])
			}
		}
		var res []any
		for _, r := range strings.Fields(st.resources) {
			res = append(res, samFill(r, p))
		}
		var acts []any
		for _, a := range strings.Fields(st.actions) {
			acts = append(acts, a)
		}
		s := map[string]any{"Effect": "Allow", "Action": acts, "Resource": res}
		if len(res) == 1 {
			s["Resource"] = res[0]
		}
		out = append(out, s)
	}
	return map[string]any{"Version": "2012-10-17", "Statement": out}, nil
}

// samPolicies sorts a Policies list into managed policy ARNs and inline
// policy documents. id is the logical id of the role; inline policies are
// named like SAM names them: <role id>Policy<position in the list>.
func samPolicies(id string, v any) (managed, inline []any, err error) {
	items, ok := v.([]any)
	if !ok && v != nil {
		items = []any{v}
	}
	for i, it := range items {
		switch p := it.(type) {
		case string:
			managed = append(managed, samManaged(p))
		case map[string]any:
			name := fmt.Sprintf("%sPolicy%d", id, i)
			switch {
			case samIntrinsic(p):
				managed = append(managed, p)
			case p["Statement"] != nil:
				doc := map[string]any{"Version": "2012-10-17"}
				for k, val := range p {
					doc[k] = val
				}
				inline = append(inline, map[string]any{"PolicyName": name, "PolicyDocument": doc})
			case len(p) == 1:
				for tname, params := range p {
					doc, err := samPolicyDoc(tname, params)
					if err != nil {
						return nil, nil, err
					}
					inline = append(inline, map[string]any{"PolicyName": name, "PolicyDocument": doc})
				}
			default:
				return nil, nil, validation("Invalid policy at position %d of %s: it is not a managed policy name, a policy document or a policy template", i, id)
			}
		default:
			return nil, nil, validation("Invalid policy at position %d of %s", i, id)
		}
	}
	return managed, inline, nil
}

// samManaged turns a managed policy name into its ARN.
func samManaged(name string) any {
	if strings.HasPrefix(name, "arn:") {
		return name
	}
	return map[string]any{"Fn::Sub": "arn:${AWS::Partition}:iam::aws:policy/" + name}
}

// samRole adds the execution role of a function or state machine and returns
// its ARN. defaults are managed policy names every such role gets.
func (x *samExp) samRole(src *samSrc, id, service string, props map[string]any, defaults []string, extra []any) any {
	managed, inline, err := samPolicies(id, props["Policies"])
	if err != nil {
		x.fail("%v", err)
		return nil
	}
	var all []any
	for _, d := range defaults {
		all = append(all, samManaged(d))
	}
	rp := map[string]any{
		"AssumeRolePolicyDocument": map[string]any{"Version": "2012-10-17", "Statement": []any{map[string]any{
			"Action": []any{"sts:AssumeRole"}, "Effect": "Allow", "Principal": map[string]any{"Service": []any{service}}}}},
		"ManagedPolicyArns": append(all, managed...),
		"Tags":              samTags(nil, service),
	}
	if inline = append(inline, extra...); len(inline) > 0 {
		rp["Policies"] = inline
	}
	if b := props["PermissionsBoundary"]; b != nil {
		rp["PermissionsBoundary"] = b
	}
	if p := props["RolePath"]; p != nil {
		rp["Path"] = p
	}
	x.add(src, id, "AWS::IAM::Role", rp)
	return samAtt(id, "Arn")
}

// samTags turns a SAM Tags map into a CloudFormation tag list and adds the
// "createdBy: SAM" tag SAM adds (lambda:createdBy, or stateMachine:createdBy).
func samTags(v any, kind string) any {
	if v != nil && samIntrinsic(v) {
		return v
	}
	key := "lambda:createdBy"
	if kind == "states.amazonaws.com" || kind == "stateMachine" {
		key = "stateMachine:createdBy"
	}
	m, _ := v.(map[string]any)
	out := []any{}
	for _, k := range core.SortedKeys(m) {
		out = append(out, map[string]any{"Key": k, "Value": m[k]})
	}
	return append(out, map[string]any{"Key": key, "Value": "SAM"})
}
