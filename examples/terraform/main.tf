terraform {
  required_providers {
    aws     = { source = "hashicorp/aws", version = "~> 6.0" }
    archive = { source = "hashicorp/archive", version = "~> 2.0" }
  }
}
# pogi_override.tf (from `pogi tf`) points this provider at pogi.
provider "aws" {}

data "aws_partition" "current" {} # aws, aws-cn or aws-us-gov, from the region

data "archive_file" "fn" {
  type        = "zip"
  source_file = "${path.module}/index.py"
  output_path = "${path.module}/.build/fn.zip"
}

resource "aws_s3_bucket" "data" {
  bucket        = "tf-data"
  force_destroy = true
}
resource "aws_s3_bucket_versioning" "data" {
  bucket = aws_s3_bucket.data.id
  versioning_configuration { status = "Enabled" }
}
resource "aws_s3_bucket_public_access_block" "data" {
  bucket                  = aws_s3_bucket.data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
resource "aws_s3_object" "hello" {
  bucket  = aws_s3_bucket.data.id
  key     = "hello.txt"
  content = "hello"
}
resource "aws_sqs_queue" "dlq" { name = "tf-dlq" }
resource "aws_sqs_queue" "jobs" {
  name                       = "tf-jobs"
  visibility_timeout_seconds = 60
  redrive_policy             = jsonencode({ deadLetterTargetArn = aws_sqs_queue.dlq.arn, maxReceiveCount = 3 })
  tags                       = { team = "core" }
}
resource "aws_sns_topic" "events" { name = "tf-events" }
resource "aws_sns_topic_subscription" "jobs" {
  topic_arn = aws_sns_topic.events.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.jobs.arn
}
resource "aws_s3_bucket_notification" "data" {
  bucket = aws_s3_bucket.data.id
  queue {
    queue_arn = aws_sqs_queue.jobs.arn
    events    = ["s3:ObjectCreated:*"]
  }
}
resource "aws_dynamodb_table" "items" {
  name             = "tf-items"
  billing_mode     = "PAY_PER_REQUEST"
  hash_key         = "pk"
  range_key        = "sk"
  stream_enabled   = true
  stream_view_type = "NEW_AND_OLD_IMAGES"
  attribute {
    name = "pk"
    type = "S"
  }
  attribute {
    name = "sk"
    type = "S"
  }
  attribute {
    name = "gsi"
    type = "S"
  }
  global_secondary_index {
    name            = "by-gsi"
    hash_key        = "gsi"
    projection_type = "ALL"
  }
  ttl {
    attribute_name = "expires"
    enabled        = true
  }
  point_in_time_recovery { enabled = true }
  tags = { team = "core" }
}
data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}
resource "aws_iam_role" "fn" {
  name               = "tf-fn"
  assume_role_policy = data.aws_iam_policy_document.assume.json
}
resource "aws_iam_role_policy_attachment" "basic" {
  role       = aws_iam_role.fn.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}
resource "aws_iam_role_policy" "fn" {
  role = aws_iam_role.fn.id
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = ["sqs:*", "dynamodb:*"], Resource = "*" }]
  })
}
resource "aws_cloudwatch_log_group" "fn" {
  name              = "/aws/lambda/tf-fn"
  retention_in_days = 7
}
resource "aws_lambda_function" "fn" {
  function_name    = "tf-fn"
  role             = aws_iam_role.fn.arn
  runtime          = "python3.12"
  handler          = "index.handler"
  filename         = data.archive_file.fn.output_path
  source_code_hash = data.archive_file.fn.output_base64sha256
  environment { variables = { TABLE = aws_dynamodb_table.items.name } }
  depends_on       = [aws_cloudwatch_log_group.fn]
}
resource "aws_lambda_event_source_mapping" "jobs" {
  event_source_arn = aws_sqs_queue.jobs.arn
  function_name    = aws_lambda_function.fn.arn
  batch_size       = 5
}
resource "aws_lambda_permission" "events" {
  statement_id  = "events"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.fn.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.tick.arn
}
resource "aws_cloudwatch_event_rule" "tick" {
  name                = "tf-tick"
  schedule_expression = "rate(1 hour)"
}
resource "aws_cloudwatch_event_target" "tick" {
  rule = aws_cloudwatch_event_rule.tick.name
  arn  = aws_lambda_function.fn.arn
}
resource "aws_kms_key" "app" {
  description         = "tf app key"
  enable_key_rotation = true
}
resource "aws_kms_alias" "app" {
  name          = "alias/tf-app"
  target_key_id = aws_kms_key.app.key_id
}
resource "aws_secretsmanager_secret" "db" {
  name       = "tf-db"
  kms_key_id = aws_kms_key.app.arn
}
resource "aws_secretsmanager_secret_version" "db" {
  secret_id     = aws_secretsmanager_secret.db.id
  secret_string = jsonencode({ user = "app", password = "s3cret" })
}
resource "aws_ssm_parameter" "url" {
  name  = "/tf/url"
  type  = "String"
  value = "https://example.com"
}
resource "aws_sfn_state_machine" "flow" {
  name     = "tf-flow"
  role_arn = aws_iam_role.fn.arn
  definition = jsonencode({
    StartAt = "Work"
    States  = { Work = { Type = "Task", Resource = aws_lambda_function.fn.arn, End = true } }
  })
}
resource "aws_apigatewayv2_api" "http" {
  name          = "tf-http"
  protocol_type = "HTTP"
  target        = aws_lambda_function.fn.arn
}
resource "aws_kinesis_stream" "clicks" {
  name        = "tf-clicks"
  shard_count = 1
}
resource "aws_ecr_repository" "app" { name = "tf-app" }
resource "aws_route53_zone" "internal" { name = "tf.internal" }
resource "aws_cloudwatch_metric_alarm" "dlq" {
  alarm_name          = "tf-dlq-depth"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  dimensions          = { QueueName = aws_sqs_queue.dlq.name }
  statistic           = "Sum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
  alarm_actions       = [aws_sns_topic.events.arn]
}
output "queue_url" { value = aws_sqs_queue.jobs.url }
output "api" { value = aws_apigatewayv2_api.http.api_endpoint }
locals { arns = [aws_sqs_queue.jobs.arn, aws_iam_role.fn.arn, aws_lambda_function.fn.arn] }
output "arns" {
  value = local.arns
  precondition {
    condition     = alltrue([for a in local.arns : startswith(a, "arn:${data.aws_partition.current.partition}:")])
    error_message = "ARNs must follow the region's partition."
  }
}
resource "aws_cognito_user_pool" "users" {
  name                     = "tf-users"
  auto_verified_attributes = ["email"]
  username_attributes      = ["email"]
  password_policy {
    minimum_length    = 8
    require_lowercase = true
    require_numbers   = true
    require_symbols   = false
    require_uppercase = true
  }
}
resource "aws_cognito_user_pool_client" "web" {
  name                = "web"
  user_pool_id        = aws_cognito_user_pool.users.id
  explicit_auth_flows = ["ALLOW_USER_PASSWORD_AUTH", "ALLOW_USER_SRP_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"]
}
