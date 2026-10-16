# Audit and delivery: CloudTrail, Fault Injection Service, CodeDeploy and
# CodePipeline (metadata only here; their runs are in the Go tests).

locals {
  delivery_iam = "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}"
}

# CloudTrail: a multi-region trail of write events
resource "aws_s3_bucket" "trail_logs" {
  bucket        = "tf-trail-logs"
  force_destroy = true
}

resource "aws_cloudtrail" "main" {
  name                       = "tf-trail"
  s3_bucket_name             = aws_s3_bucket.trail_logs.id
  s3_key_prefix              = "audit"
  is_multi_region_trail      = true
  enable_log_file_validation = true
  event_selector {
    read_write_type           = "WriteOnly"
    include_management_events = true
  }
  tags = { team = "audit" }
}

# Fault Injection Service: throttle one role's DynamoDB writes for a minute
resource "aws_fis_experiment_template" "throttle" {
  description = "throttle DynamoDB writes"
  role_arn    = "${local.delivery_iam}:role/fis"
  stop_condition {
    source = "none"
  }
  action {
    name      = "throttle"
    action_id = "aws:fis:inject-api-throttle-error"
    parameter {
      key   = "service"
      value = "dynamodb"
    }
    parameter {
      key   = "operations"
      value = "PutItem,BatchWriteItem"
    }
    parameter {
      key   = "percentage"
      value = "100"
    }
    parameter {
      key   = "duration"
      value = "PT1M"
    }
    target {
      key   = "Roles"
      value = "chaos-roles"
    }
  }
  target {
    name           = "chaos-roles"
    resource_type  = "aws:iam:role"
    selection_mode = "ALL"
    resource_arns  = ["${local.delivery_iam}:role/chaos-target"]
  }
  tags = { Name = "throttle" }
}

# CodeDeploy: a Lambda application that shifts traffic by a custom canary
resource "aws_codedeploy_app" "app" {
  name             = "tf-app"
  compute_platform = "Lambda"
  tags             = { team = "web" }
}

resource "aws_codedeploy_deployment_config" "canary" {
  deployment_config_name = "tf-canary"
  compute_platform       = "Lambda"
  traffic_routing_config {
    type = "TimeBasedCanary"
    time_based_canary {
      interval   = 5
      percentage = 25
    }
  }
}

resource "aws_codedeploy_deployment_group" "group" {
  app_name               = aws_codedeploy_app.app.name
  deployment_group_name  = "tf-group"
  service_role_arn       = "${local.delivery_iam}:role/codedeploy"
  deployment_config_name = aws_codedeploy_deployment_config.canary.id
  deployment_style {
    deployment_option = "WITH_TRAFFIC_CONTROL"
    deployment_type   = "BLUE_GREEN"
  }
  auto_rollback_configuration {
    enabled = true
    events  = ["DEPLOYMENT_FAILURE"]
  }
}

# CodePipeline: an S3 source, then a manual approval
resource "aws_s3_bucket" "pipeline_artifacts" {
  bucket        = "tf-pipeline-artifacts"
  force_destroy = true
}

resource "aws_codepipeline" "release" {
  name     = "tf-release"
  role_arn = "${local.delivery_iam}:role/pipeline"
  artifact_store {
    location = aws_s3_bucket.pipeline_artifacts.bucket
    type     = "S3"
  }
  stage {
    name = "Source"
    action {
      name             = "Source"
      category         = "Source"
      owner            = "AWS"
      provider         = "S3"
      version          = "1"
      output_artifacts = ["source"]
      configuration = {
        S3Bucket             = aws_s3_bucket.pipeline_artifacts.bucket
        S3ObjectKey          = "source.zip"
        PollForSourceChanges = "false"
      }
    }
  }
  stage {
    name = "Approve"
    action {
      name     = "Approve"
      category = "Approval"
      owner    = "AWS"
      provider = "Manual"
      version  = "1"
    }
  }
  tags = { team = "web" }
}
