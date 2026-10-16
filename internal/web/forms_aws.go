package web

import "github.com/nrndev/pogi/internal/core"

// curatedForm is a create call of a hand-written AWS service.
type curatedForm struct {
	Target, Op, Title string // Target is the service's JSON target prefix (or smithy name), for hub.Call
	Fields            []core.Field
}

// curated are the 15 most common creates, by service. Member names are the API's own (the JSON ones).
var curated = map[string][]curatedForm{
	"sqs": {{"AmazonSQS", "CreateQueue", "Create queue", []core.Field{
		req(str("QueueName")), dict("Attributes"), dict("tags")}}},
	"sns": {{"AmazonSimpleNotificationService", "CreateTopic", "Create topic", []core.Field{
		req(str("Name")), dict("Attributes"), tags("Tags")}}},
	"dynamodb": {{"DynamoDB_20120810", "CreateTable", "Create table", []core.Field{
		req(str("TableName")),
		req(list("AttributeDefinitions", obj("", req(str("AttributeName")), req(enum("AttributeType", "S", "N", "B"))))),
		req(list("KeySchema", obj("", req(str("AttributeName")), req(enum("KeyType", "HASH", "RANGE"))))),
		enum("BillingMode", "PAY_PER_REQUEST", "PROVISIONED"),
		obj("ProvisionedThroughput", req(num("ReadCapacityUnits")), req(num("WriteCapacityUnits")))}}},
	"kinesis": {{"Kinesis_20131202", "CreateStream", "Create stream", []core.Field{
		req(str("StreamName")), num("ShardCount"), obj("StreamModeDetails", req(enum("StreamMode", "ON_DEMAND", "PROVISIONED")))}}},
	"secretsmanager": {{"secretsmanager", "CreateSecret", "Create secret", []core.Field{
		req(str("Name")), str("Description"), text("SecretString"), str("KmsKeyId"), tags("Tags")}}},
	"ssm": {{"AmazonSSM", "PutParameter", "Put parameter", []core.Field{
		req(str("Name")), req(str("Value")), req(enum("Type", "String", "StringList", "SecureString")), str("Description"),
		flag("Overwrite"), enum("Tier", "Standard", "Advanced", "Intelligent-Tiering")}}},
	"logs": {{"Logs_20140328", "CreateLogGroup", "Create log group", []core.Field{
		req(str("logGroupName")), str("kmsKeyId"), dict("tags")}}},
	"events": {
		{"AWSEvents", "CreateEventBus", "Create event bus", []core.Field{req(str("Name")), str("Description"), tags("Tags")}},
		{"AWSEvents", "PutRule", "Put rule", []core.Field{
			req(str("Name")), str("EventBusName"), text("EventPattern"), str("ScheduleExpression"), enum("State", "ENABLED", "DISABLED"),
			str("Description"), tags("Tags")}}},
	"stepfunctions": {{"AWSStepFunctions", "CreateStateMachine", "Create state machine", []core.Field{
		req(str("name")), req(text("definition")), req(str("roleArn")), enum("type", "STANDARD", "EXPRESS"),
		list("tags", obj("", req(str("key")), str("value")))}}},
	"kms": {{"TrentService", "CreateKey", "Create key", []core.Field{
		str("Description"), enum("KeyUsage", "ENCRYPT_DECRYPT", "SIGN_VERIFY", "GENERATE_VERIFY_MAC"),
		enum("KeySpec", "SYMMETRIC_DEFAULT", "RSA_2048", "RSA_3072", "RSA_4096", "ECC_NIST_P256", "ECC_NIST_P384", "HMAC_256"),
		list("Tags", obj("", req(str("TagKey")), req(str("TagValue"))))}}},
	"iam": {
		{"AWSIdentityManagementV20100508", "CreateUser", "Create user", []core.Field{req(str("UserName")), str("Path"), tags("Tags")}},
		{"AWSIdentityManagementV20100508", "CreateRole", "Create role", []core.Field{
			req(str("RoleName")), req(text("AssumeRolePolicyDocument")), str("Description"), str("Path"), num("MaxSessionDuration"), tags("Tags")}}},
	"ecr": {{"AmazonEC2ContainerRegistry_V20150921", "CreateRepository", "Create repository", []core.Field{
		req(str("repositoryName")), enum("imageTagMutability", "MUTABLE", "IMMUTABLE"), obj("imageScanningConfiguration", flag("scanOnPush"))}}},
	"ecs": {{"AmazonEC2ContainerServiceV20141113", "CreateCluster", "Create cluster", []core.Field{str("clusterName")}}},
}

func curatedOp(svc, op string) *curatedForm {
	for i, c := range curated[svc] {
		if c.Op == op {
			return &curated[svc][i]
		}
	}
	return nil
}

// A small vocabulary for writing the forms above.

func fld(name, kind string) core.Field {
	return core.Field{Name: name, Label: core.Label(name), Kind: kind}
}
func str(n string) core.Field     { return fld(n, "string") }
func text(n string) core.Field    { return fld(n, "text") }
func num(n string) core.Field     { return fld(n, "integer") }
func flag(n string) core.Field    { return fld(n, "bool") }
func req(f core.Field) core.Field { f.Required = true; return f }

func enum(n string, vals ...string) core.Field {
	f := fld(n, "enum")
	f.Enum = vals
	return f
}

func obj(n string, fields ...core.Field) core.Field {
	f := fld(n, "struct")
	f.Fields = fields
	return f
}

func list(n string, elem core.Field) core.Field {
	f := fld(n, "list")
	f.Elem = &elem
	return f
}

// dict is a map of strings to strings.
func dict(n string) core.Field {
	f := fld(n, "map")
	f.Elem = &core.Field{Kind: "string"}
	return f
}

// tags is a list of key and value structures.
func tags(n string) core.Field { return list(n, obj("", req(str("Key")), str("Value"))) }
