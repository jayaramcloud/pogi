package iam

import (
	"crypto/sha256"
	"encoding/base32"
	"encoding/json"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Policy is a managed policy. AttachmentCount and PermissionsBoundaryUsageCount
// are computed per account on output.
type Policy struct {
	PolicyName, PolicyId, Arn, Path, DefaultVersionId string
	AttachmentCount                                   int
	PermissionsBoundaryUsageCount                     int
	IsAttachable                                      bool
	Description                                       string
	CreateDate, UpdateDate                            core.Time
	Tags                                              []Tag
	Versions                                          []*Version `query:"-"`
	NextVersion                                       int        `query:"-"`
	PolicyVersionList                                 []Version  `json:"-" query:"PolicyVersionList"`
}

// Version is a managed policy version; Document is raw in state and
// URL-encoded in responses.
type Version struct {
	Document         string
	VersionId        string
	IsDefaultVersion bool
	CreateDate       core.Time
}

func (p *Policy) version(id string) (*Version, error) {
	for _, v := range p.Versions {
		if v.VersionId == id {
			return v, nil
		}
	}
	return nil, core.Errorf(404, "NoSuchEntity", "Policy %s version %s does not exist or is not attachable.", p.Arn, id)
}

func (p *Policy) setDefault(id string) {
	p.DefaultVersionId = id
	for _, v := range p.Versions {
		v.IsDefaultVersion = v.VersionId == id
	}
}

// document is the default version's document.
func (p *Policy) document() string {
	v, _ := p.version(p.DefaultVersionId)
	if v == nil {
		return ""
	}
	return v.Document
}

// enc URL-encodes a policy document the way IAM returns documents.
func enc(doc string) string { return strings.ReplaceAll(url.QueryEscape(doc), "+", "%20") }

// checkDoc validates a policy document like IAM does: JSON with a known
// Version, statements with Effect and Action; trust policies need a Principal,
// identity policies a Resource and no Principal.
func checkDoc(doc string, trust bool) error {
	bad := func(msg string) error { return core.Errorf(400, "MalformedPolicyDocument", "%s", msg) }
	var p map[string]any
	if json.Unmarshal([]byte(doc), &p) != nil || p == nil {
		return bad("Syntax errors in policy.")
	}
	if v, ok := p["Version"]; ok && v != "2012-10-17" && v != "2008-10-17" {
		return bad("The policy failed legacy parsing")
	}
	var stmts []any
	switch st := p["Statement"].(type) {
	case []any:
		stmts = st
	case map[string]any:
		stmts = []any{st}
	default:
		return bad("Syntax errors in policy.")
	}
	for _, x := range stmts {
		st, ok := x.(map[string]any)
		switch {
		case !ok:
			return bad("Syntax errors in policy.")
		case st["Effect"] != "Allow" && st["Effect"] != "Deny":
			return bad("Missing required field Effect or invalid value.")
		case st["Action"] == nil && st["NotAction"] == nil:
			return bad("Policy statement must contain actions.")
		case trust && st["Principal"] == nil && st["NotPrincipal"] == nil:
			return bad("Missing required field Principal.")
		case !trust && (st["Principal"] != nil || st["NotPrincipal"] != nil):
			return bad("Policy document should not specify a principal.")
		case !trust && st["Resource"] == nil && st["NotResource"] == nil:
			return bad("Policy statement must contain resources.")
		}
	}
	return nil
}

// awsManaged are the AWS managed policies pogi knows, by name with path.
// Documents are the real ones, trimmed where AWS lists hundreds of actions.
var awsManaged = map[string]string{
	"AdministratorAccess":    `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
	"PowerUserAccess":        `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","NotAction":["iam:*","organizations:*","account:*"],"Resource":"*"},{"Effect":"Allow","Action":["iam:CreateServiceLinkedRole","iam:DeleteServiceLinkedRole","iam:ListRoles","organizations:DescribeOrganization","account:ListRegions","account:GetAccountInformation"],"Resource":"*"}]}`,
	"ReadOnlyAccess":         `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["acm:Describe*","acm:Get*","acm:List*","apigateway:GET","cloudformation:Describe*","cloudformation:Get*","cloudformation:List*","cloudwatch:Describe*","cloudwatch:Get*","cloudwatch:List*","dynamodb:BatchGet*","dynamodb:Describe*","dynamodb:Get*","dynamodb:List*","dynamodb:Query","dynamodb:Scan","ec2:Describe*","ec2:Get*","ecr:BatchGet*","ecr:Describe*","ecr:Get*","ecr:List*","events:Describe*","events:List*","iam:Get*","iam:List*","kinesis:Describe*","kinesis:Get*","kinesis:List*","kms:Describe*","kms:Get*","kms:List*","lambda:Get*","lambda:List*","logs:Describe*","logs:FilterLogEvents","logs:Get*","logs:StartQuery","logs:StopQuery","s3:Get*","s3:List*","secretsmanager:Describe*","secretsmanager:GetResourcePolicy","secretsmanager:List*","sns:Get*","sns:List*","sqs:Get*","sqs:List*","sqs:Receive*","ssm:Describe*","ssm:Get*","ssm:List*","states:Describe*","states:Get*","states:List*","sts:GetCallerIdentity","tag:Get*"],"Resource":"*"}]}`,
	"AmazonS3FullAccess":     `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:*","s3-object-lambda:*"],"Resource":"*"}]}`,
	"AmazonS3ReadOnlyAccess": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:Get*","s3:List*","s3:Describe*","s3-object-lambda:Get*","s3-object-lambda:List*"],"Resource":"*"}]}`,
	"AmazonDynamoDBFullAccess": `{"Version":"2012-10-17","Statement":[{"Action":["dynamodb:*","dax:*","application-autoscaling:*","cloudwatch:DeleteAlarms","cloudwatch:DescribeAlarms","cloudwatch:GetMetricData","cloudwatch:PutMetricAlarm","kinesis:DescribeStream","kinesis:ListStreams","kms:DescribeKey","kms:ListAliases","lambda:CreateEventSourceMapping","lambda:DeleteEventSourceMapping","lambda:GetFunctionConfiguration","lambda:ListEventSourceMappings","lambda:ListFunctions","sns:CreateTopic","sns:DeleteTopic","sns:ListSubscriptionsByTopic","sns:ListTopics","sns:Subscribe","sns:Unsubscribe","tag:GetResources","ec2:DescribeVpcs","ec2:DescribeSubnets","ec2:DescribeSecurityGroups","iam:GetRole","iam:ListRoles"],"Effect":"Allow","Resource":"*"},` +
		`{"Action":"iam:PassRole","Effect":"Allow","Resource":"*","Condition":{"StringLike":{"iam:PassedToService":["application-autoscaling.amazonaws.com","dax.amazonaws.com"]}}}]}`,
	"AWSCloudFormationReadOnlyAccess": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["cloudformation:Describe*","cloudformation:EstimateTemplateCost","cloudformation:Get*","cloudformation:List*","cloudformation:ValidateTemplate","cloudformation:Detect*"],"Resource":"*"}]}`,
	"AmazonSQSFullAccess":             `{"Version":"2012-10-17","Statement":[{"Action":["sqs:*"],"Effect":"Allow","Resource":"*"}]}`,
	"AmazonSQSReadOnlyAccess":         `{"Version":"2012-10-17","Statement":[{"Action":["sqs:GetQueueAttributes","sqs:GetQueueUrl","sqs:ListDeadLetterSourceQueues","sqs:ListQueues","sqs:ListMessageMoveTasks","sqs:ListQueueTags"],"Effect":"Allow","Resource":"*"}]}`,
	"AmazonSNSFullAccess":             `{"Version":"2012-10-17","Statement":[{"Action":["sns:*"],"Effect":"Allow","Resource":"*"}]}`,
	"CloudWatchLogsFullAccess":        `{"Version":"2012-10-17","Statement":[{"Sid":"CloudWatchLogsFullAccess","Effect":"Allow","Action":["logs:*","cloudwatch:GenerateQuery"],"Resource":"*"}]}`,
	"AWSLambda_FullAccess": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["cloudformation:DescribeStacks","cloudformation:ListStackResources","cloudwatch:ListMetrics","cloudwatch:GetMetricData","ec2:DescribeSecurityGroups","ec2:DescribeSubnets","ec2:DescribeVpcs","kms:ListAliases","iam:GetPolicy","iam:GetPolicyVersion","iam:GetRole","iam:GetRolePolicy","iam:ListAttachedRolePolicies","iam:ListRolePolicies","iam:ListRoles","lambda:*","logs:DescribeLogGroups","states:DescribeStateMachine","states:ListStateMachines","tag:GetResources","xray:GetTraceSummaries","xray:BatchGetTraces"],"Resource":"*"},` +
		`{"Effect":"Allow","Action":"iam:PassRole","Resource":"*","Condition":{"StringEquals":{"iam:PassedToService":"lambda.amazonaws.com"}}},` +
		`{"Effect":"Allow","Action":["logs:DescribeLogStreams","logs:GetLogEvents","logs:FilterLogEvents","logs:StartLiveTail","logs:StopLiveTail"],"Resource":"arn:aws:logs:*:*:log-group:/aws/lambda/*"}]}`,
	"SecretsManagerReadWrite": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["secretsmanager:*","cloudformation:CreateChangeSet","cloudformation:DescribeChangeSet","cloudformation:DescribeStackResource","cloudformation:DescribeStacks","cloudformation:ExecuteChangeSet","ec2:DescribeSecurityGroups","ec2:DescribeSubnets","ec2:DescribeVpcs","kms:DescribeKey","kms:ListAliases","kms:ListKeys","lambda:ListFunctions","rds:DescribeDBClusters","rds:DescribeDBInstances","redshift:DescribeClusters","tag:GetResources"],"Resource":"*"},` +
		`{"Effect":"Allow","Action":["lambda:AddPermission","lambda:CreateFunction","lambda:GetFunction","lambda:InvokeFunction","lambda:UpdateFunctionConfiguration"],"Resource":"arn:aws:lambda:*:*:function:SecretsManager*"},` +
		`{"Effect":"Allow","Action":["serverlessrepo:CreateCloudFormationChangeSet","serverlessrepo:GetApplication"],"Resource":"arn:aws:serverlessrepo:*:*:applications/SecretsManager*"},` +
		`{"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::awsserverlessrepo-changesets*","arn:aws:s3:::secrets-manager-rotation-apps-*/*"]}]}`,
	"AmazonEC2ContainerRegistryReadOnly":            `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["ecr:GetAuthorizationToken","ecr:BatchCheckLayerAvailability","ecr:GetDownloadUrlForLayer","ecr:GetRepositoryPolicy","ecr:DescribeRepositories","ecr:ListImages","ecr:DescribeImages","ecr:BatchGetImage","ecr:GetLifecyclePolicy","ecr:GetLifecyclePolicyPreview","ecr:ListTagsForResource","ecr:DescribeImageScanFindings"],"Resource":"*"}]}`,
	"AWSXrayWriteOnlyAccess":                        `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["xray:PutTraceSegments","xray:PutTelemetryRecords","xray:GetSamplingRules","xray:GetSamplingTargets","xray:GetSamplingStatisticSummaries"],"Resource":["*"]}]}`,
	"AmazonSSMManagedInstanceCore":                  `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["ssm:DescribeAssociation","ssm:GetDeployablePatchSnapshotForInstance","ssm:GetDocument","ssm:DescribeDocument","ssm:GetManifest","ssm:GetParameter","ssm:GetParameters","ssm:ListAssociations","ssm:ListInstanceAssociations","ssm:PutInventory","ssm:PutComplianceItems","ssm:PutConfigurePackageResult","ssm:UpdateAssociationStatus","ssm:UpdateInstanceAssociationStatus","ssm:UpdateInstanceInformation"],"Resource":"*"},{"Effect":"Allow","Action":["ssmmessages:CreateControlChannel","ssmmessages:CreateDataChannel","ssmmessages:OpenControlChannel","ssmmessages:OpenDataChannel"],"Resource":"*"},{"Effect":"Allow","Action":["ec2messages:AcknowledgeMessage","ec2messages:DeleteMessage","ec2messages:FailMessage","ec2messages:GetEndpoint","ec2messages:GetMessages","ec2messages:SendReply"],"Resource":"*"}]}`,
	"service-role/AWSLambdaBasicExecutionRole":      `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["logs:CreateLogGroup","logs:CreateLogStream","logs:PutLogEvents"],"Resource":"*"}]}`,
	"service-role/AWSLambdaSQSQueueExecutionRole":   `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["sqs:ReceiveMessage","sqs:DeleteMessage","sqs:GetQueueAttributes","logs:CreateLogGroup","logs:CreateLogStream","logs:PutLogEvents"],"Resource":"*"}]}`,
	"service-role/AWSLambdaDynamoDBExecutionRole":   `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["dynamodb:DescribeStream","dynamodb:GetRecords","dynamodb:GetShardIterator","dynamodb:ListStreams","logs:CreateLogGroup","logs:CreateLogStream","logs:PutLogEvents"],"Resource":"*"}]}`,
	"service-role/AWSLambdaKinesisExecutionRole":    `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["kinesis:DescribeStream","kinesis:DescribeStreamSummary","kinesis:GetRecords","kinesis:GetShardIterator","kinesis:ListShards","kinesis:ListStreams","kinesis:SubscribeToShard","logs:CreateLogGroup","logs:CreateLogStream","logs:PutLogEvents"],"Resource":"*"}]}`,
	"service-role/AWSLambdaVPCAccessExecutionRole":  `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["logs:CreateLogGroup","logs:CreateLogStream","logs:PutLogEvents","ec2:CreateNetworkInterface","ec2:DescribeNetworkInterfaces","ec2:DescribeSubnets","ec2:DeleteNetworkInterface","ec2:AssignPrivateIpAddresses","ec2:UnassignPrivateIpAddresses"],"Resource":"*"}]}`,
	"service-role/AmazonECSTaskExecutionRolePolicy": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["ecr:GetAuthorizationToken","ecr:BatchCheckLayerAvailability","ecr:GetDownloadUrlForLayer","ecr:BatchGetImage","logs:CreateLogStream","logs:PutLogEvents"],"Resource":"*"}]}`,
}

// managed holds the AWS managed policies by ARN, in every partition.
// Read-only: per-account attachment counts are computed on output.
var managed = func() map[string]*Policy {
	m := map[string]*Policy{}
	for name, doc := range awsManaged {
		for _, partition := range []string{"aws", "aws-cn", "aws-us-gov"} {
			p := awsPolicy(partition, name, doc)
			m[p.Arn] = p
		}
	}
	return m
}()

// awsPolicy builds an AWS managed policy of a partition from its name (with
// path) and its document in the aws partition.
func awsPolicy(partition, name, doc string) *Policy {
	created := core.Time(time.Date(2015, 2, 6, 18, 39, 46, 0, time.UTC))
	i := strings.LastIndex(name, "/") + 1
	sum := sha256.Sum256([]byte(name))
	doc = strings.ReplaceAll(doc, `"arn:aws:`, `"arn:`+partition+`:`)
	return &Policy{PolicyName: name[i:], PolicyId: "ANPA" + base32.StdEncoding.EncodeToString(sum[:])[:17], Arn: "arn:" + partition + ":iam::aws:policy/" + name,
		Path: "/" + name[:i], DefaultVersionId: "v1", IsAttachable: true, CreateDate: created, UpdateDate: created,
		Versions: []*Version{{Document: doc, VersionId: "v1", IsDefaultVersion: true, CreateDate: created}}}
}

// policy finds a managed policy by ARN, AWS managed or customer managed in
// any account; lock held.
func (s *Service) policy(arn string) (*Policy, error) {
	if p := managed[arn]; p != nil {
		return p, nil
	}
	_, _, account, res, ok := core.ParseARN(arn)
	if name, found := strings.CutPrefix(res, "policy/"); ok && found && account == "aws" && name != "" {
		// ponytail: AWS has 1,000+ managed policies; unlisted ones exist with no statements
		return awsPolicy(strings.Split(arn, ":")[1], name, `{"Version":"2012-10-17","Statement":[]}`), nil
	}
	if ok && strings.HasPrefix(res, "policy/") {
		if a := s.st.Accounts[account]; a != nil {
			if p := a.Policies[lower(res[strings.LastIndex(res, "/")+1:])]; p != nil && strings.EqualFold(p.Arn, arn) {
				return p, nil
			}
		}
	}
	return nil, core.Errorf(404, "NoSuchEntity", "Policy %s does not exist or is not attachable.", arn)
}

// localPolicy finds a customer managed policy the caller may modify; lock held.
func (s *Service) localPolicy(c *core.Ctx, arn string) (*Account, *Policy, error) {
	p, err := s.policy(arn)
	if err != nil {
		return nil, nil, err
	}
	if _, _, owner, _, _ := core.ParseARN(p.Arn); owner != c.Account {
		return nil, nil, core.Errorf(403, "AccessDenied", "Policy %s is not owned by account %s and cannot be modified.", arn, c.Account)
	}
	return s.acct(c.Account), p, nil
}

// usage counts the attachments and boundary uses of a policy in an account.
func (a *Account) usage(arn string) (attached, boundary int) {
	for _, u := range a.Users {
		if slices.Contains(u.Attached, arn) {
			attached++
		}
		if u.PermissionsBoundary != nil && u.PermissionsBoundary.PermissionsBoundaryArn == arn {
			boundary++
		}
	}
	for _, g := range a.Groups {
		if slices.Contains(g.Attached, arn) {
			attached++
		}
	}
	for _, r := range a.Roles {
		if slices.Contains(r.Attached, arn) {
			attached++
		}
		if r.PermissionsBoundary != nil && r.PermissionsBoundary.PermissionsBoundaryArn == arn {
			boundary++
		}
	}
	return attached, boundary
}

// policyOut is a response copy of p with this account's counts; lock held.
func (a *Account) policyOut(p *Policy) Policy {
	o := *p
	o.AttachmentCount, o.PermissionsBoundaryUsageCount = a.usage(p.Arn)
	return o
}

func (s *Service) createPolicy(c *core.Ctx, in *struct {
	PolicyName, Path, PolicyDocument, Description string
	Tags                                          []Tag
}) (any, error) {
	path, err := checkPath(in.Path)
	if err != nil {
		return nil, err
	}
	if err := checkName("policyName", in.PolicyName, 128); err != nil {
		return nil, err
	}
	if err := checkDoc(in.PolicyDocument, false); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.acct(c.Account)
	if a.Policies[lower(in.PolicyName)] != nil {
		return nil, core.Errorf(409, "EntityAlreadyExists", "A policy called %s already exists. Duplicate names are not allowed.", in.PolicyName)
	}
	t := now()
	p := &Policy{PolicyName: in.PolicyName, PolicyId: newID("ANPA"), Arn: iamARN(c, "policy", path, in.PolicyName), Path: path,
		DefaultVersionId: "v1", IsAttachable: true, Description: in.Description, CreateDate: t, UpdateDate: t, Tags: in.Tags,
		Versions: []*Version{{Document: in.PolicyDocument, VersionId: "v1", IsDefaultVersion: true, CreateDate: t}}, NextVersion: 2}
	a.Policies[lower(p.PolicyName)] = p
	return map[string]any{"Policy": a.policyOut(p)}, nil
}

type policyArnInput struct {
	PolicyArn, VersionId, Marker string
	MaxItems                     int
}

func (s *Service) getPolicy(c *core.Ctx, in *policyArnInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.policy(in.PolicyArn)
	if err != nil {
		return nil, err
	}
	return map[string]any{"Policy": s.acct(c.Account).policyOut(p)}, nil
}

func (s *Service) listPolicies(c *core.Ctx, in *struct {
	Scope, PathPrefix, Marker string
	OnlyAttached              bool
	MaxItems                  int
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.acct(c.Account)
	var all []*Policy
	if in.Scope != "Local" {
		for _, k := range core.SortedKeys(managed) {
			if strings.HasPrefix(k, "arn:"+core.Partition(c.Region)+":") {
				all = append(all, managed[k])
			}
		}
	}
	if in.Scope != "AWS" {
		for _, k := range core.SortedKeys(a.Policies) {
			all = append(all, a.Policies[k])
		}
	}
	var out []Policy
	for _, p := range all {
		if o := a.policyOut(p); strings.HasPrefix(p.Path, in.PathPrefix) && (!in.OnlyAttached || o.AttachmentCount > 0) {
			out = append(out, o)
		}
	}
	return page("Policies", out, in.Marker, in.MaxItems), nil
}

func (s *Service) deletePolicy(c *core.Ctx, in *policyArnInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, p, err := s.localPolicy(c, in.PolicyArn)
	if err != nil {
		return nil, err
	}
	if n, b := a.usage(p.Arn); n+b > 0 {
		return nil, core.Errorf(409, "DeleteConflict", "Cannot delete a policy attached to entities.")
	}
	if len(p.Versions) > 1 {
		return nil, core.Errorf(409, "DeleteConflict", "This policy has more than one version. Before you delete a policy, you must delete the policy's versions. The default version is deleted with the policy.")
	}
	delete(a.Policies, lower(p.PolicyName))
	return nil, nil
}

func (s *Service) createPolicyVersion(c *core.Ctx, in *struct {
	PolicyArn, PolicyDocument string
	SetAsDefault              bool
}) (any, error) {
	if err := checkDoc(in.PolicyDocument, false); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, p, err := s.localPolicy(c, in.PolicyArn)
	if err != nil {
		return nil, err
	}
	if len(p.Versions) >= 5 {
		return nil, core.Errorf(409, "LimitExceeded", "A managed policy can have up to 5 versions. Before you create a new version, you must delete an existing version.")
	}
	v := &Version{Document: in.PolicyDocument, VersionId: "v" + strconv.Itoa(p.NextVersion), CreateDate: now()}
	p.NextVersion++
	p.Versions = append(p.Versions, v)
	p.UpdateDate = v.CreateDate
	if in.SetAsDefault {
		p.setDefault(v.VersionId)
	}
	o := *v
	o.Document = ""
	return map[string]any{"PolicyVersion": o}, nil
}

func (s *Service) getPolicyVersion(c *core.Ctx, in *policyArnInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.policy(in.PolicyArn)
	if err != nil {
		return nil, err
	}
	v, err := p.version(in.VersionId)
	if err != nil {
		return nil, err
	}
	o := *v
	o.Document = enc(o.Document)
	return map[string]any{"PolicyVersion": o}, nil
}

func (s *Service) listPolicyVersions(c *core.Ctx, in *policyArnInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.policy(in.PolicyArn)
	if err != nil {
		return nil, err
	}
	out := make([]Version, len(p.Versions))
	for i, v := range p.Versions {
		out[i] = *v
		out[i].Document = ""
	}
	return page("Versions", out, in.Marker, in.MaxItems), nil
}

func (s *Service) deletePolicyVersion(c *core.Ctx, in *policyArnInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, p, err := s.localPolicy(c, in.PolicyArn)
	if err != nil {
		return nil, err
	}
	v, err := p.version(in.VersionId)
	if err != nil {
		return nil, err
	}
	if v.IsDefaultVersion {
		return nil, core.Errorf(409, "DeleteConflict", "Cannot delete the default version of a policy.")
	}
	p.Versions = without(p.Versions, slices.Index(p.Versions, v))
	return nil, nil
}

func (s *Service) setDefaultPolicyVersion(c *core.Ctx, in *policyArnInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, p, err := s.localPolicy(c, in.PolicyArn)
	if err != nil {
		return nil, err
	}
	if _, err := p.version(in.VersionId); err != nil {
		return nil, err
	}
	p.setDefault(in.VersionId)
	return nil, nil
}

func (s *Service) listEntitiesForPolicy(c *core.Ctx, in *struct {
	PolicyArn, EntityFilter, PathPrefix string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.policy(in.PolicyArn); err != nil {
		return nil, err
	}
	a := s.acct(c.Account)
	want := func(kind, path string, attached []string) bool {
		return (in.EntityFilter == "" || in.EntityFilter == kind) && strings.HasPrefix(path, in.PathPrefix) && slices.Contains(attached, in.PolicyArn)
	}
	type user struct{ UserName, UserId string }
	type group struct{ GroupName, GroupId string }
	type role struct{ RoleName, RoleId string }
	var users []user
	var groups []group
	var roles []role
	for _, k := range core.SortedKeys(a.Users) {
		if u := a.Users[k]; want("User", u.Path, u.Attached) {
			users = append(users, user{u.UserName, u.UserId})
		}
	}
	for _, k := range core.SortedKeys(a.Groups) {
		if g := a.Groups[k]; want("Group", g.Path, g.Attached) {
			groups = append(groups, group{g.GroupName, g.GroupId})
		}
	}
	for _, k := range core.SortedKeys(a.Roles) {
		if r := a.Roles[k]; want("Role", r.Path, r.Attached) {
			roles = append(roles, role{r.RoleName, r.RoleId})
		}
	}
	return map[string]any{"PolicyUsers": users, "PolicyGroups": groups, "PolicyRoles": roles, "IsTruncated": false}, nil
}

// ---- attached and inline policies of users, groups and roles ----

type permInput struct {
	UserName, GroupName, RoleName         string
	PolicyArn, PolicyName, PolicyDocument string
	PathPrefix, Marker                    string
	MaxItems                              int
}

// holder resolves the user, group or role a policy operation targets and
// returns its policies, its name member and its name; lock held.
func (s *Service) holder(c *core.Ctx, in *permInput) (*perms, string, string, error) {
	a := s.acct(c.Account)
	switch {
	case in.UserName != "":
		u, err := get(a.Users, "user", in.UserName)
		if err != nil {
			return nil, "", "", err
		}
		return &u.perms, "UserName", u.UserName, nil
	case in.GroupName != "":
		g, err := get(a.Groups, "group", in.GroupName)
		if err != nil {
			return nil, "", "", err
		}
		return &g.perms, "GroupName", g.GroupName, nil
	case in.RoleName != "":
		r, err := get(a.Roles, "role", in.RoleName)
		if err != nil {
			return nil, "", "", err
		}
		return &r.perms, "RoleName", r.RoleName, nil
	}
	return nil, "", "", core.Errorf(400, "ValidationError", "A user, group or role name is required for %s.", c.Op)
}

func (s *Service) attach(c *core.Ctx, in *permInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, _, _, err := s.holder(c, in)
	if err != nil {
		return nil, err
	}
	p, err := s.policy(in.PolicyArn)
	if err != nil {
		return nil, err
	}
	if !slices.Contains(h.Attached, p.Arn) {
		h.Attached = append(h.Attached, p.Arn)
	}
	return nil, nil
}

func (s *Service) detach(c *core.Ctx, in *permInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, _, _, err := s.holder(c, in)
	if err != nil {
		return nil, err
	}
	i := slices.Index(h.Attached, in.PolicyArn)
	if i < 0 {
		return nil, core.Errorf(404, "NoSuchEntity", "Policy %s was not found.", in.PolicyArn)
	}
	h.Attached = without(h.Attached, i)
	return nil, nil
}

// attached lists managed policy references; lock held.
func (s *Service) attached(arns []string, pathPrefix string) []AttachedPolicy {
	var out []AttachedPolicy
	for _, arn := range arns {
		if p, err := s.policy(arn); err == nil && strings.HasPrefix(p.Path, pathPrefix) {
			out = append(out, AttachedPolicy{p.PolicyName, p.Arn})
		}
	}
	return out
}

func (s *Service) listAttached(c *core.Ctx, in *permInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, _, _, err := s.holder(c, in)
	if err != nil {
		return nil, err
	}
	return page("AttachedPolicies", s.attached(h.Attached, in.PathPrefix), in.Marker, in.MaxItems), nil
}

func (s *Service) putInline(c *core.Ctx, in *permInput) (any, error) {
	if err := checkName("policyName", in.PolicyName, 128); err != nil {
		return nil, err
	}
	if err := checkDoc(in.PolicyDocument, false); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	h, _, _, err := s.holder(c, in)
	if err != nil {
		return nil, err
	}
	if h.Inline == nil {
		h.Inline = map[string]string{}
	}
	h.Inline[in.PolicyName] = in.PolicyDocument
	return nil, nil
}

func (s *Service) getInline(c *core.Ctx, in *permInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, member, name, err := s.holder(c, in)
	if err != nil {
		return nil, err
	}
	doc, ok := h.Inline[in.PolicyName]
	if !ok {
		return nil, core.Errorf(404, "NoSuchEntity", "The %s policy with name %s cannot be found.", lower(strings.TrimSuffix(member, "Name")), in.PolicyName)
	}
	return map[string]any{member: name, "PolicyName": in.PolicyName, "PolicyDocument": enc(doc)}, nil
}

func (s *Service) deleteInline(c *core.Ctx, in *permInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, member, _, err := s.holder(c, in)
	if err != nil {
		return nil, err
	}
	if _, ok := h.Inline[in.PolicyName]; !ok {
		return nil, core.Errorf(404, "NoSuchEntity", "The %s policy with name %s cannot be found.", lower(strings.TrimSuffix(member, "Name")), in.PolicyName)
	}
	delete(h.Inline, in.PolicyName)
	return nil, nil
}

func (s *Service) listInline(c *core.Ctx, in *permInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, _, _, err := s.holder(c, in)
	if err != nil {
		return nil, err
	}
	return page("PolicyNames", core.SortedKeys(h.Inline), in.Marker, in.MaxItems), nil
}

// ---- account-wide views ----

func inlineDetails(m map[string]string) []PolicyDetail {
	var out []PolicyDetail
	for _, n := range core.SortedKeys(m) {
		out = append(out, PolicyDetail{n, enc(m[n])})
	}
	return out
}

func (s *Service) authorizationDetails(c *core.Ctx, in *struct{ Filter []string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.acct(c.Account)
	want := func(f string) bool { return len(in.Filter) == 0 || slices.Contains(in.Filter, f) }
	out := map[string]any{"IsTruncated": false}
	if want("User") {
		var users []User
		for _, k := range core.SortedKeys(a.Users) {
			u := *a.Users[k]
			u.UserPolicyList, u.GroupList, u.AttachedManagedPolicies = inlineDetails(u.Inline), slices.Clone(u.Groups), s.attached(u.Attached, "")
			users = append(users, u)
		}
		out["UserDetailList"] = users
	}
	if want("Group") {
		var groups []Group
		for _, k := range core.SortedKeys(a.Groups) {
			g := *a.Groups[k]
			g.GroupPolicyList, g.AttachedManagedPolicies = inlineDetails(g.Inline), s.attached(g.Attached, "")
			groups = append(groups, g)
		}
		out["GroupDetailList"] = groups
	}
	if want("Role") {
		var roles []Role
		for _, k := range core.SortedKeys(a.Roles) {
			r := a.Roles[k].out()
			r.RolePolicyList, r.AttachedManagedPolicies, r.RoleLastUsed = inlineDetails(r.Inline), s.attached(r.Attached, ""), &struct{}{}
			for _, ipk := range core.SortedKeys(a.InstanceProfiles) {
				if ip := a.InstanceProfiles[ipk]; slices.Contains(ip.RoleNames, r.RoleName) {
					r.InstanceProfileList = append(r.InstanceProfileList, a.profileOut(ip))
				}
			}
			roles = append(roles, r)
		}
		out["RoleDetailList"] = roles
	}
	var policies []Policy
	add := func(p *Policy) {
		o := a.policyOut(p)
		for _, v := range p.Versions {
			vv := *v
			vv.Document = enc(vv.Document)
			o.PolicyVersionList = append(o.PolicyVersionList, vv)
		}
		policies = append(policies, o)
	}
	if want("LocalManagedPolicy") {
		for _, k := range core.SortedKeys(a.Policies) {
			add(a.Policies[k])
		}
	}
	if want("AWSManagedPolicy") { // only the ones in use, like AWS
		for _, k := range core.SortedKeys(managed) {
			if n, b := a.usage(k); n+b > 0 {
				add(managed[k])
			}
		}
	}
	out["Policies"] = policies
	return out, nil
}

// simulate answers SimulatePrincipalPolicy and SimulateCustomPolicy.
// ponytail: no evaluation yet, every action is implicitDeny; the IAM
// enforcement phase evaluates PoliciesFor documents here.
// simulate serves SimulatePrincipalPolicy and SimulateCustomPolicy with the
// same evaluator that enforcement uses.
func (s *Service) simulate(c *core.Ctx, in *struct {
	PolicySourceArn           string
	PolicyInputList           []string
	ActionNames, ResourceArns []string
	ContextEntries            []struct {
		ContextKeyName   string
		ContextKeyValues []string
	}
}) (any, error) {
	docs := in.PolicyInputList
	keys := map[string]string{}
	if in.PolicySourceArn != "" {
		docs = append(s.PoliciesFor(in.PolicySourceArn), docs...)
		keys["aws:principalarn"] = in.PolicySourceArn
	}
	for _, e := range in.ContextEntries {
		if len(e.ContextKeyValues) > 0 {
			keys[strings.ToLower(e.ContextKeyName)] = e.ContextKeyValues[0]
		}
	}
	resources := in.ResourceArns
	if len(resources) == 0 {
		resources = []string{"*"}
	}
	type result struct{ EvalActionName, EvalResourceName, EvalDecision string }
	var out []result
	for _, action := range in.ActionNames {
		for _, r := range resources {
			decision, _ := decide(docs, action, r, keys)
			out = append(out, result{action, r, decision})
		}
	}
	return map[string]any{"EvaluationResults": out, "IsTruncated": false}, nil
}

// PoliciesFor returns every identity policy document that applies to a user
// or role ARN: inline policies, default versions of attached managed
// policies and, for users, those of their groups. Assumed-role session ARNs
// (arn:<partition>:sts::<account>:assumed-role/<role>/<session>) map to the role.
// Permissions boundaries are not included.
func (s *Service) PoliciesFor(principalArn string) []string {
	_, _, account, res, ok := core.ParseARN(principalArn)
	if !ok {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.st.Accounts[account]
	if a == nil {
		return nil
	}
	var holders []*perms
	kind, rest, _ := strings.Cut(res, "/")
	switch kind {
	case "user":
		if u := a.Users[lower(rest[strings.LastIndex(rest, "/")+1:])]; u != nil {
			holders = append(holders, &u.perms)
			for _, n := range u.Groups {
				if g := a.Groups[lower(n)]; g != nil {
					holders = append(holders, &g.perms)
				}
			}
		}
	case "role", "assumed-role":
		name := rest[strings.LastIndex(rest, "/")+1:]
		if kind == "assumed-role" {
			name, _, _ = strings.Cut(rest, "/")
		}
		if r := a.Roles[lower(name)]; r != nil {
			holders = append(holders, &r.perms)
		}
	}
	var docs []string
	for _, h := range holders {
		for _, n := range core.SortedKeys(h.Inline) {
			docs = append(docs, h.Inline[n])
		}
		for _, arn := range h.Attached {
			if p, err := s.policy(arn); err == nil {
				docs = append(docs, p.document())
			}
		}
	}
	return docs
}
