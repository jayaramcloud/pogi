# Serverless Application Repository: deploy an application as a CloudFormation stack.
# AWS applications are published by their authors, and Terraform has no resource for that, so the
# application is made here with one call to pogi's own API (conformance.sh exports AWS_ENDPOINT_URL).
data "aws_caller_identity" "sar" {}

locals {
  sar_app = "arn:aws:serverlessrepo:us-east-1:${data.aws_caller_identity.sar.account_id}:applications/k-queue"
}

resource "terraform_data" "sar_app" {
  provisioner "local-exec" {
    command = <<-EOT
      curl -fsS -X POST "$AWS_ENDPOINT_URL/applications" \
        -H 'Authorization: AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/serverlessrepo/aws4_request, SignedHeaders=host, Signature=x' \
        -H 'Content-Type: application/json' \
        -d '{"name":"k-queue","author":"acme","description":"A queue","semanticVersion":"1.0.0","templateBody":"{\"Parameters\":{\"Name\":{\"Type\":\"String\"}},\"Resources\":{\"Q\":{\"Type\":\"AWS::SQS::Queue\",\"Properties\":{\"QueueName\":{\"Ref\":\"Name\"}}}}}"}'
    EOT
  }
}

data "aws_serverlessapplicationrepository_application" "k" {
  application_id = local.sar_app
  depends_on     = [terraform_data.sar_app]
}

resource "aws_serverlessapplicationrepository_cloudformation_stack" "k" {
  name             = "k-sar-stack"
  application_id   = data.aws_serverlessapplicationrepository_application.k.application_id
  semantic_version = data.aws_serverlessapplicationrepository_application.k.semantic_version
  capabilities     = data.aws_serverlessapplicationrepository_application.k.required_capabilities
  parameters = {
    Name = "k-sar-queue"
  }
}
