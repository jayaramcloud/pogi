const cdk = require("aws-cdk-lib");
const { Stack, RemovalPolicy, CfnOutput, Duration } = cdk;
const s3 = require("aws-cdk-lib/aws-s3");
const sqs = require("aws-cdk-lib/aws-sqs");
const sns = require("aws-cdk-lib/aws-sns");
const subs = require("aws-cdk-lib/aws-sns-subscriptions");
const ddb = require("aws-cdk-lib/aws-dynamodb");
const lambda = require("aws-cdk-lib/aws-lambda");
const { SqsEventSource } = require("aws-cdk-lib/aws-lambda-event-sources");

class Demo extends Stack {
  constructor(scope, id, props) {
    super(scope, id, props);
    const bucket = new s3.Bucket(this, "Uploads", { removalPolicy: RemovalPolicy.DESTROY, autoDeleteObjects: true });
    const queue = new sqs.Queue(this, "Jobs", { visibilityTimeout: Duration.seconds(60) });
    const topic = new sns.Topic(this, "Events");
    topic.addSubscription(new subs.SqsSubscription(queue));
    const table = new ddb.Table(this, "Items", {
      partitionKey: { name: "pk", type: ddb.AttributeType.STRING },
      removalPolicy: RemovalPolicy.DESTROY,
    });
    const fn = new lambda.Function(this, "Worker", {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: "index.handler",
      code: lambda.Code.fromInline(
        "import os, boto3\n" +
        "def handler(event, context):\n" +
        "    t = boto3.resource('dynamodb').Table(os.environ['TABLE'])\n" +
        "    for r in event.get('Records', []):\n" +
        "        t.put_item(Item={'pk': r['messageId'], 'body': r['body']})\n" +
        "    return len(event.get('Records', []))\n"),
      environment: { TABLE: table.tableName },
    });
    table.grantReadWriteData(fn);
    fn.addEventSource(new SqsEventSource(queue));
    new CfnOutput(this, "BucketName", { value: bucket.bucketName });
    new CfnOutput(this, "TopicArn", { value: topic.topicArn });
    new CfnOutput(this, "TableName", { value: table.tableName });
  }
}

const app = new cdk.App();
new Demo(app, "PogiDemo", { env: { account: "000000000000", region: "us-east-1" } });
