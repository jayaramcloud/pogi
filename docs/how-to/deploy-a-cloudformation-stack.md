---
title: Deploy a CloudFormation stack
description: Deploy, update and delete a CloudFormation or SAM template against pogi with the AWS CLI.
---

# Deploy a CloudFormation stack

pogi runs CloudFormation templates and creates each resource in its own service. You'll deploy a template with a queue and a topic, deploy a SAM template with a function, then update and delete a stack.

## Before you start

1. Start pogi in another terminal:

   ```bash
   pogi start
   ```

2. Load the settings that point your tools at pogi:

   ```bash
   eval "$(pogi env)"
   ```

You need the AWS CLI. The SAM section runs a Python function, so it needs `python3` on your machine (or Docker). You don't need the SAM CLI: `aws cloudformation deploy` runs SAM templates too.

## Deploy a template

1. Write a template with an SQS queue and an SNS topic. The queue's visibility timeout (how long a received message stays hidden) is a parameter, so you can change it later:

   ```bash
   cat > template.yaml <<'YAML'
   AWSTemplateFormatVersion: "2010-09-09"
   Parameters:
     VisibilityTimeout:
       Type: Number
       Default: 30
   Resources:
     Jobs:
       Type: AWS::SQS::Queue
       Properties:
         VisibilityTimeout: !Ref VisibilityTimeout
     Alerts:
       Type: AWS::SNS::Topic
   Outputs:
     QueueUrl:
       Value: !Ref Jobs
   YAML
   ```

2. Deploy it as a stack called `demo`:

   ```bash
   aws cloudformation deploy --template-file template.yaml --stack-name demo
   ```

   ```text title="Output"
   Waiting for changeset to be created..
   Waiting for stack create/update to complete
   Successfully created/updated stack - demo
   ```

3. List the stack's resources:

   ```bash
   aws cloudformation describe-stack-resources --stack-name demo \
     --query 'StackResources[].[LogicalResourceId,ResourceType,ResourceStatus]' --output text
   ```

   ```text title="Output"
   Alerts	AWS::SNS::Topic	CREATE_COMPLETE
   Jobs	AWS::SQS::Queue	CREATE_COMPLETE
   ```

4. See the queue in SQS. CloudFormation named it for you:

   ```bash
   aws sqs list-queues --query QueueUrls --output text
   ```

   ```text title="Output"
   http://localhost:7644/000000000000/demo-Jobs-ERK6F9QHM3FI
   ```

## Deploy a SAM template

A SAM (Serverless Application Model) template is a CloudFormation template with a `Transform` line. The transform turns one `AWS::Serverless::Function` into a function and its role.

1. Write a template with one function. The code is inline:

   ```bash
   cat > sam.yaml <<'YAML'
   AWSTemplateFormatVersion: "2010-09-09"
   Transform: AWS::Serverless-2016-10-31
   Resources:
     Hello:
       Type: AWS::Serverless::Function
       Properties:
         Runtime: python3.12
         Handler: index.handler
         InlineCode: |
           def handler(event, context):
               return {"hello": event.get("name", "world")}
   YAML
   ```

2. Deploy it. The two capabilities allow the transform and the role it creates:

   ```bash
   aws cloudformation deploy --template-file sam.yaml --stack-name sam-demo \
     --capabilities CAPABILITY_IAM CAPABILITY_AUTO_EXPAND
   ```

   ```text title="Output"
   Waiting for changeset to be created..
   Waiting for stack create/update to complete
   Successfully created/updated stack - sam-demo
   ```

3. List the resources. The function and its role are both there:

   ```bash
   aws cloudformation describe-stack-resources --stack-name sam-demo \
     --query 'StackResources[].[LogicalResourceId,ResourceType,ResourceStatus]' --output text
   ```

   ```text title="Output"
   Hello	AWS::Lambda::Function	CREATE_COMPLETE
   HelloRole	AWS::IAM::Role	CREATE_COMPLETE
   ```

4. Call the function:

   ```bash
   FUNCTION=$(aws lambda list-functions --query 'Functions[0].FunctionName' --output text)
   aws lambda invoke --function-name $FUNCTION --cli-binary-format raw-in-base64-out \
     --payload '{"name":"pogi"}' result.json --query StatusCode
   cat result.json; echo
   ```

   ```text title="Output"
   200
   {"hello": "pogi"}
   ```

## Update and delete a stack

1. Deploy `template.yaml` again with a new value for the parameter. CloudFormation updates the stack:

   ```bash
   aws cloudformation deploy --template-file template.yaml --stack-name demo \
     --parameter-overrides VisibilityTimeout=60
   ```

   ```text title="Output"
   Waiting for changeset to be created..
   Waiting for stack create/update to complete
   Successfully created/updated stack - demo
   ```

2. Check the queue:

   ```bash
   QUEUE=$(aws cloudformation describe-stacks --stack-name demo --query 'Stacks[0].Outputs[0].OutputValue' --output text)
   aws sqs get-queue-attributes --queue-url $QUEUE --attribute-names VisibilityTimeout --query Attributes
   ```

   ```text title="Output"
   {
       "VisibilityTimeout": "60"
   }
   ```

3. Delete both stacks and wait until they are gone. Deleting a stack deletes its resources:

   ```bash
   aws cloudformation delete-stack --stack-name demo
   aws cloudformation delete-stack --stack-name sam-demo
   aws cloudformation wait stack-delete-complete --stack-name demo
   aws cloudformation wait stack-delete-complete --stack-name sam-demo
   ```

4. The queue is gone. This prints nothing, because no queues are left:

   ```bash
   aws sqs list-queues --query QueueUrls --output text
   ```

## Limits

- A template that uses a resource type pogi can't create is refused with an error that names the type. The coverage page lists the types it can create.
- SAM connectors, applications and deployment preferences are not supported yet.
- StackSets and macros are not supported yet.
- Drift detection always reports a stack as in sync.

See the [CloudFormation row](../reference/coverage.md#aws) in the coverage page.

## See also

- [Connect your tools](connect-your-tools.md#cdk)
- [Use queues and topics](use-queues-and-topics.md)
- [Run functions and HTTP APIs](run-functions-and-http-apis.md)
- [Coverage](../reference/coverage.md)
