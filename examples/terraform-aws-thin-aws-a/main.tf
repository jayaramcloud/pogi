# Group aws-a: Direct Connect, WorkSpaces, WorkSpaces Secure Browser, AppStream 2.0, Directory Service, Signer,
# Elastic Disaster Recovery, Cloud WAN (Network Manager), Device Farm and License Manager, served by the
# generated engine. Created, planned (no changes) and destroyed against pogi.
# pogi_override.tf (from `pogi tf`) points this provider at pogi.
terraform {
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 6.0" }
  }
}

provider "aws" {}

resource "aws_dx_gateway" "this" {
  name            = "tf-dx-gateway"
  amazon_side_asn = "64512"
}

resource "aws_dx_connection" "this" {
  name      = "tf-dx-connection"
  bandwidth = "1Gbps"
  location  = "EqDC2"
  tags      = { env = "dev" }
}

resource "aws_workspaces_ip_group" "this" {
  name        = "tf-office"
  description = "office network"
  rules {
    source      = "10.0.0.0/16"
    description = "lan"
  }
  tags = { env = "dev" }
}

resource "aws_workspacesweb_portal" "this" {
  display_name = "tf-browser"
  tags         = { env = "dev" }
}

resource "aws_appstream_stack" "this" {
  name         = "tf-stack"
  description  = "apps"
  display_name = "Apps"
  storage_connectors {
    connector_type = "HOMEFOLDERS"
  }
  tags = { env = "dev" }
}

resource "aws_vpc" "dir" {
  cidr_block = "10.40.0.0/16"
}

resource "aws_subnet" "dir_a" {
  vpc_id            = aws_vpc.dir.id
  cidr_block        = "10.40.1.0/24"
  availability_zone = "us-east-1a"
}

resource "aws_subnet" "dir_b" {
  vpc_id            = aws_vpc.dir.id
  cidr_block        = "10.40.2.0/24"
  availability_zone = "us-east-1b"
}

resource "aws_directory_service_directory" "this" {
  name     = "corp.example.com"
  password = "SuperSecretPassw0rd"
  size     = "Small"
  type     = "SimpleAD"
  vpc_settings {
    vpc_id     = aws_vpc.dir.id
    subnet_ids = [aws_subnet.dir_a.id, aws_subnet.dir_b.id]
  }
  tags = { env = "dev" }
}

resource "aws_signer_signing_profile" "this" {
  name        = "tf_lambda_profile"
  platform_id = "AWSLambda-SHA384-ECDSA"
  signature_validity_period {
    type  = "DAYS"
    value = 135
  }
  tags = { env = "dev" }
}

resource "aws_drs_replication_configuration_template" "this" {
  associate_default_security_group        = false
  bandwidth_throttling                    = 12
  create_public_ip                        = false
  data_plane_routing                      = "PRIVATE_IP"
  default_large_staging_disk_type         = "GP2"
  ebs_encryption                          = "DEFAULT"
  use_dedicated_replication_server        = false
  replication_server_instance_type        = "t3.small"
  replication_servers_security_groups_ids = [aws_security_group.drs.id]
  staging_area_subnet_id                  = aws_subnet.dir_a.id
  staging_area_tags                       = { role = "staging" }
  pit_policy {
    enabled            = true
    interval           = 10
    retention_duration = 60
    units              = "MINUTE"
    rule_id            = 1
  }
  pit_policy {
    enabled            = true
    interval           = 1
    retention_duration = 24
    units              = "HOUR"
    rule_id            = 2
  }
  pit_policy {
    enabled            = true
    interval           = 1
    retention_duration = 3
    units              = "DAY"
    rule_id            = 3
  }
  tags = { env = "dev" }
}

resource "aws_security_group" "drs" {
  name   = "tf-drs"
  vpc_id = aws_vpc.dir.id
}

resource "aws_networkmanager_global_network" "this" {
  description = "tf wan"
  tags        = { env = "dev" }
}

resource "aws_devicefarm_project" "this" {
  name = "tf-app-tests"
  tags = { env = "dev" }
}

resource "aws_licensemanager_license_configuration" "this" {
  name                     = "tf-cores"
  description              = "core licenses"
  license_count            = 10
  license_count_hard_limit = true
  license_counting_type    = "Core"
  tags                     = { env = "dev" }
}
