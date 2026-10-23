# Group aws-b: MediaLive, MediaPackage v2, GameLift, Kinesis Video Streams, DataZone, Data Exchange, Clean Rooms,
# Resilience Hub, Application Recovery Controller (recovery control config), Payment Cryptography and Private CA,
# all served by the generated engine. pogi_override.tf (from `pogi tf`) points this provider at pogi.
terraform {
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 6.0" }
  }
}

provider "aws" {}

# MediaLive: an input security group and an input that uses it.
resource "aws_medialive_input_security_group" "ml" {
  whitelist_rules {
    cidr = "10.0.0.0/16"
  }
  tags = { env = "dev" }
}

resource "aws_medialive_input" "ml" {
  name                  = "tf-input"
  input_security_groups = [aws_medialive_input_security_group.ml.id]
  type                  = "UDP_PUSH"
  tags                  = { env = "dev" }
}

# MediaPackage v2: a channel group.
resource "aws_media_packagev2_channel_group" "mp" {
  name        = "tf-group"
  description = "pogi example"
  tags        = { env = "dev" }
}

# Kinesis Video Streams
resource "aws_kinesis_video_stream" "kv" {
  name                    = "tf-camera"
  data_retention_in_hours = 24
  media_type              = "video/h264"
  tags                    = { env = "dev" }
}

# Data Exchange
resource "aws_dataexchange_data_set" "dx" {
  name        = "tf-dataset"
  asset_type  = "S3_SNAPSHOT"
  description = "pogi example"
  tags        = { env = "dev" }
}

resource "aws_dataexchange_revision" "dx" {
  data_set_id = aws_dataexchange_data_set.dx.id
  comment     = "first revision"
}

# Resilience Hub
resource "aws_resiliencehub_resiliency_policy" "rh" {
  name = "tf-policy"
  tier = "NonCritical"
  policy {
    az {
      rpo = "24h"
      rto = "24h"
    }
    hardware {
      rpo = "24h"
      rto = "24h"
    }
    software {
      rpo = "24h"
      rto = "24h"
    }
  }
  tags = { env = "dev" }
}

# Application Recovery Controller: a cluster, a control panel and a routing control.
resource "aws_route53recoverycontrolconfig_cluster" "arc" {
  name = "tf-cluster"
}

resource "aws_route53recoverycontrolconfig_control_panel" "arc" {
  name        = "tf-panel"
  cluster_arn = aws_route53recoverycontrolconfig_cluster.arc.arn
}

resource "aws_route53recoverycontrolconfig_routing_control" "arc" {
  name              = "tf-control"
  cluster_arn       = aws_route53recoverycontrolconfig_cluster.arc.arn
  control_panel_arn = aws_route53recoverycontrolconfig_control_panel.arc.arn
}

# Payment Cryptography: a key and an alias for it.
resource "aws_paymentcryptography_key" "pc" {
  exportable = true
  key_attributes {
    key_algorithm = "TDES_3KEY"
    key_class     = "SYMMETRIC_KEY"
    key_usage     = "TR31_P0_PIN_ENCRYPTION_KEY"
    key_modes_of_use {
      decrypt = true
      encrypt = true
      unwrap  = true
      wrap    = true
    }
  }
  tags = { env = "dev" }
}

resource "aws_paymentcryptography_key_alias" "pc" {
  alias_name = "alias/tf-pin"
  key_arn    = aws_paymentcryptography_key.pc.arn
}

# Private CA: a root authority (it stays PENDING_CERTIFICATE; pogi issues no certificates).
resource "aws_acmpca_certificate_authority" "ca" {
  type                            = "ROOT"
  permanent_deletion_time_in_days = 7
  certificate_authority_configuration {
    key_algorithm     = "RSA_2048"
    signing_algorithm = "SHA256WITHRSA"
    subject {
      common_name = "example.com"
    }
  }
  tags = { env = "dev" }
}

# GameLift: an alias that answers with a message, and a game session queue.
resource "aws_gamelift_alias" "gl" {
  name        = "tf-alias"
  description = "pogi example"
  routing_strategy {
    type    = "TERMINAL"
    message = "closed for maintenance"
  }
  tags = { env = "dev" }
}

resource "aws_gamelift_game_session_queue" "gl" {
  name               = "tf-queue"
  timeout_in_seconds = 60
  tags               = { env = "dev" }
}

# DataZone: a domain.
resource "aws_iam_role" "dz" {
  name = "tf-datazone"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = ["sts:AssumeRole", "sts:TagSession"], Principal = { Service = "datazone.amazonaws.com" } }]
  })
}

resource "aws_datazone_domain" "dz" {
  name                  = "tf-domain"
  domain_execution_role = aws_iam_role.dz.arn
  tags                  = { env = "dev" }
}

# Clean Rooms: a collaboration.
resource "aws_cleanrooms_collaboration" "cr" {
  name                     = "tf-collaboration"
  description              = "pogi example"
  creator_display_name     = "Creator"
  creator_member_abilities = ["CAN_QUERY", "CAN_RECEIVE_RESULTS"]
  query_log_status         = "DISABLED"
  data_encryption_metadata {
    allow_clear_text                            = true
    allow_duplicates                            = true
    allow_joins_on_columns_with_different_names = true
    preserve_nulls                              = false
  }
  member {
    account_id       = "111122223333"
    display_name     = "Partner"
    member_abilities = []
  }
  tags = { env = "dev" }
}
