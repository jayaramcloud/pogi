# Operations and storage services (internal/aws/generic): CloudWatch OAM and RUM, AWS Config,
# Managed Prometheus, Transfer Family, DataSync and S3 Tables.
resource "aws_s3_bucket" "ops" {
  bucket        = "tf-ops-bucket"
  force_destroy = true
}

data "aws_iam_policy_document" "ops_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["config.amazonaws.com", "transfer.amazonaws.com", "datasync.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ops" {
  name               = "tf-ops"
  assume_role_policy = data.aws_iam_policy_document.ops_assume.json
}

# CloudWatch Observability Access Manager and RUM
resource "aws_oam_sink" "ops" {
  name = "tf-ops-sink"
  tags = { env = "dev" }
}

resource "aws_oam_link" "ops" {
  label_template  = "$AccountName"
  resource_types  = ["AWS::CloudWatch::Metric"]
  sink_identifier = aws_oam_sink.ops.arn
}

resource "aws_rum_app_monitor" "ops" {
  name   = "tf-ops-web"
  domain = "example.com"
  tags   = { env = "dev" }
}

# AWS Config
resource "aws_config_configuration_recorder" "ops" {
  name     = "tf-ops"
  role_arn = aws_iam_role.ops.arn
  recording_group {
    all_supported = true
  }
}

resource "aws_config_delivery_channel" "ops" {
  name           = "tf-ops"
  s3_bucket_name = aws_s3_bucket.ops.bucket
  depends_on     = [aws_config_configuration_recorder.ops]
}

resource "aws_config_configuration_recorder_status" "ops" {
  name       = aws_config_configuration_recorder.ops.name
  is_enabled = true
  depends_on = [aws_config_delivery_channel.ops]
}

resource "aws_config_config_rule" "ops" {
  name = "tf-ops-versioning"
  source {
    owner             = "AWS"
    source_identifier = "S3_BUCKET_VERSIONING_ENABLED"
  }
  depends_on = [aws_config_configuration_recorder.ops]
}

resource "aws_config_conformance_pack" "ops" {
  name          = "tf-ops-pack"
  template_body = <<-EOT
    Resources:
      S3Versioning:
        Type: AWS::Config::ConfigRule
        Properties:
          ConfigRuleName: pack-versioning
          Source:
            Owner: AWS
            SourceIdentifier: S3_BUCKET_VERSIONING_ENABLED
  EOT
  depends_on    = [aws_config_configuration_recorder.ops]
}

# Managed Prometheus
resource "aws_prometheus_workspace" "ops" {
  alias = "tf-ops"
  tags  = { env = "dev" }
}

resource "aws_prometheus_rule_group_namespace" "ops" {
  name         = "tf-ops-rules"
  workspace_id = aws_prometheus_workspace.ops.id
  data         = <<-EOT
    groups:
      - name: example
        rules:
          - record: job:up:sum
            expr: sum by (job) (up)
  EOT
}

resource "aws_prometheus_alert_manager_definition" "ops" {
  workspace_id = aws_prometheus_workspace.ops.id
  definition   = <<-EOT
    alertmanager_config: |
      route:
        receiver: default
      receivers:
        - name: default
  EOT
}

# Transfer Family: an SFTP server on the bucket
resource "aws_transfer_server" "ops" {
  protocols              = ["SFTP"]
  identity_provider_type = "SERVICE_MANAGED"
  tags                   = { env = "dev" }
}

resource "aws_transfer_user" "ops" {
  server_id      = aws_transfer_server.ops.id
  user_name      = "tf-user"
  role           = aws_iam_role.ops.arn
  home_directory = "/${aws_s3_bucket.ops.bucket}/home"
}

resource "aws_transfer_ssh_key" "ops" {
  server_id = aws_transfer_server.ops.id
  user_name = aws_transfer_user.ops.user_name
  body      = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAILpU9h03/w3gp9O3sK1WqlFtYic2ITpI/UKCVFRozood pogi-tf"
}

# DataSync: copy between two prefixes of the bucket
resource "aws_datasync_location_s3" "ops_source" {
  s3_bucket_arn = aws_s3_bucket.ops.arn
  subdirectory  = "/source"
  s3_config {
    bucket_access_role_arn = aws_iam_role.ops.arn
  }
}

resource "aws_datasync_location_s3" "ops_destination" {
  s3_bucket_arn = aws_s3_bucket.ops.arn
  subdirectory  = "/destination"
  s3_config {
    bucket_access_role_arn = aws_iam_role.ops.arn
  }
}

resource "aws_datasync_task" "ops" {
  name                     = "tf-ops-sync"
  source_location_arn      = aws_datasync_location_s3.ops_source.arn
  destination_location_arn = aws_datasync_location_s3.ops_destination.arn
}

# S3 Tables
resource "aws_s3tables_table_bucket" "ops" {
  name = "tf-ops-lake"
}

resource "aws_s3tables_namespace" "ops" {
  namespace        = "sales"
  table_bucket_arn = aws_s3tables_table_bucket.ops.arn
}

resource "aws_s3tables_table" "ops" {
  name             = "orders"
  namespace        = aws_s3tables_namespace.ops.namespace
  table_bucket_arn = aws_s3tables_namespace.ops.table_bucket_arn
  format           = "ICEBERG"
}
