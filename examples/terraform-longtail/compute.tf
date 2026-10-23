# Phase 15, compute, network and analytics: the long-tail services of this area,
# served from botocore models plus overrides (internal/aws/generic). Resources
# are named cp_* so that this file merges beside the other areas' files.
#
# Left out because the provider waits a long fixed time for them (or needs an
# API pogi does not serve): aws_elastic_beanstalk_environment, aws_emr_cluster,
# aws_mwaa_environment, aws_dms_replication_instance and
# aws_cloudcontrolapi_resource (it reads the type's schema from CloudFormation).

data "aws_vpc" "cp_default" { default = true }
data "aws_subnets" "cp_default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.cp_default.id]
  }
}

resource "aws_s3_bucket" "cp_bundles" {
  bucket        = "tf-compute-bundles"
  force_destroy = true
}
resource "aws_s3_object" "cp_bundle" {
  bucket  = aws_s3_bucket.cp_bundles.id
  key     = "app-v1.zip"
  content = "zip"
}

# Elastic Beanstalk: an application, a version from the S3 bundle, a template
resource "aws_elastic_beanstalk_application" "cp_app" {
  name        = "tf-compute-app"
  description = "demo"
  tags        = { env = "dev" }
}
resource "aws_elastic_beanstalk_application_version" "cp_v1" {
  name        = "v1"
  application = aws_elastic_beanstalk_application.cp_app.name
  bucket      = aws_s3_bucket.cp_bundles.id
  key         = aws_s3_object.cp_bundle.key
}
resource "aws_elastic_beanstalk_configuration_template" "cp_small" {
  name                = "small"
  application         = aws_elastic_beanstalk_application.cp_app.name
  solution_stack_name = "64bit Amazon Linux 2023 v4.3.0 running Python 3.11"
}

# Lightsail: a key pair, an instance and a static IP attached to it
resource "aws_lightsail_key_pair" "cp_key" { name = "tf-compute-key" }
resource "aws_lightsail_instance" "cp_web" {
  name              = "tf-compute-web"
  availability_zone = "us-east-1a"
  blueprint_id      = "ubuntu_22_04"
  bundle_id         = "nano_3_0"
  key_pair_name     = aws_lightsail_key_pair.cp_key.name
  tags              = { env = "dev" }
}
resource "aws_lightsail_static_ip" "cp_ip" { name = "tf-compute-ip" }
resource "aws_lightsail_static_ip_attachment" "cp_ip" {
  static_ip_name = aws_lightsail_static_ip.cp_ip.name
  instance_name  = aws_lightsail_instance.cp_web.name
}

# DMS: a subnet group and a source endpoint
resource "aws_dms_replication_subnet_group" "cp" {
  replication_subnet_group_id          = "tf-compute-dms"
  replication_subnet_group_description = "dms"
  subnet_ids                           = data.aws_subnets.cp_default.ids
}
resource "aws_dms_endpoint" "cp_source" {
  endpoint_id   = "tf-compute-source"
  endpoint_type = "source"
  engine_name   = "mysql"
  server_name   = "db.example.com"
  port          = 3306
  username      = "app"
  password      = "secret-pass"
  database_name = "app"
  tags          = { env = "dev" }
}

# Lake Formation: settings, a registered location and an LF-tag
resource "aws_lakeformation_data_lake_settings" "cp" {
  admins = ["arn:aws:iam::000000000000:user/admin"]
}
resource "aws_lakeformation_resource" "cp" {
  arn                     = aws_s3_bucket.cp_bundles.arn
  use_service_linked_role = true
}
resource "aws_lakeformation_lf_tag" "cp" {
  key    = "tf-compute-env"
  values = ["dev", "prod"]
}

# EMR and EMR Serverless
resource "aws_emr_security_configuration" "cp" {
  name = "tf-compute-emr"
  configuration = jsonencode({
    EncryptionConfiguration = { EnableInTransitEncryption = false, EnableAtRestEncryption = false }
  })
}
resource "aws_emrserverless_application" "cp" {
  name          = "tf-compute-spark"
  release_label = "emr-7.1.0"
  type          = "spark"
  tags          = { env = "dev" }
}

# Managed Service for Apache Flink
resource "aws_kinesisanalyticsv2_application" "cp" {
  name                   = "tf-compute-flink"
  runtime_environment    = "FLINK-1_18"
  service_execution_role = "arn:aws:iam::000000000000:role/flink"
  application_configuration {
    application_code_configuration {
      code_content_type = "PLAINTEXT"
      code_content { text_content = "print('hi')" }
    }
  }
  tags = { env = "dev" }
}

# Amazon Connect and App Integrations
resource "aws_connect_instance" "cp" {
  identity_management_type = "CONNECT_MANAGED"
  inbound_calls_enabled    = true
  outbound_calls_enabled   = true
  instance_alias           = "tf-compute-connect"
}
resource "aws_appintegrations_event_integration" "cp" {
  name            = "tf-compute-ei"
  description     = "events"
  eventbridge_bus = "default"
  event_filter { source = "aws.partner/examplepartner.com" }
  tags = { env = "dev" }
}
