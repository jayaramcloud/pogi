# AppFlow, MediaConvert, IVS, Security Lake, QuickSight, Managed Grafana and VPC Lattice.
# Each is stored and read back; nothing runs behind it.

# AppFlow: an S3 to S3 flow and a connector profile
resource "aws_s3_bucket" "flow_src" {
  bucket = "aws7-flow-src"
}

resource "aws_s3_bucket" "flow_dst" {
  bucket = "aws7-flow-dst"
}

resource "aws_appflow_flow" "flow" {
  name = "aws7-flow"
  source_flow_config {
    connector_type = "S3"
    source_connector_properties {
      s3 {
        bucket_name   = aws_s3_bucket.flow_src.bucket
        bucket_prefix = "in"
      }
    }
  }
  destination_flow_config {
    connector_type = "S3"
    destination_connector_properties {
      s3 {
        bucket_name = aws_s3_bucket.flow_dst.bucket
        s3_output_format_config {
          file_type = "JSON"
        }
      }
    }
  }
  task {
    source_fields     = ["id"]
    destination_field = "id"
    task_type         = "Map"
    connector_operator {
      s3 = "NO_OP"
    }
  }
  trigger_config {
    trigger_type = "OnDemand"
  }
}

resource "aws_appflow_connector_profile" "slack" {
  name                 = "aws7-slack"
  connection_mode      = "Public"
  connector_type       = "Slack"
  connector_profile_config {
    connector_profile_credentials {
      slack {
        client_id     = "cid"
        client_secret = "csecret"
        access_token  = "token"
      }
    }
    connector_profile_properties {
      slack {
        instance_url = "https://example.slack.com"
      }
    }
  }
}

# MediaConvert
resource "aws_media_convert_queue" "q" {
  name = "aws7-queue"
}

# IVS
resource "aws_s3_bucket" "ivs" {
  bucket = "aws7-ivs-recordings"
}

resource "aws_ivs_recording_configuration" "rec" {
  name = "aws7-recording"
  destination_configuration {
    s3 {
      bucket_name = aws_s3_bucket.ivs.id
    }
  }
}

resource "aws_ivs_channel" "ch" {
  name                        = "aws7-channel"
  recording_configuration_arn = aws_ivs_recording_configuration.rec.arn
}

# Security Lake
resource "aws_iam_role" "lake" {
  name = "aws7-lake-meta"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "sts:AssumeRole", Principal = { Service = "lambda.amazonaws.com" } }]
  })
}

resource "aws_securitylake_data_lake" "lake" {
  meta_store_manager_role_arn = aws_iam_role.lake.arn
  configuration {
    region = "us-east-1"
  }
}

# QuickSight
resource "aws_quicksight_group" "g" {
  group_name = "aws7-group"
}

resource "aws_quicksight_data_source" "s3" {
  data_source_id = "aws7-s3"
  name           = "aws7-s3"
  type           = "S3"
  parameters {
    s3 {
      manifest_file_location {
        bucket = aws_s3_bucket.flow_src.bucket
        key    = "manifest.json"
      }
    }
  }
}

# Managed Grafana
resource "aws_grafana_workspace" "g" {
  name                     = "aws7-grafana"
  account_access_type      = "CURRENT_ACCOUNT"
  authentication_providers = ["AWS_SSO"]
  permission_type          = "SERVICE_MANAGED"
}

# VPC Lattice
resource "aws_vpclattice_service_network" "sn" {
  name = "aws7-network"
}

resource "aws_vpclattice_service" "svc" {
  name = "aws7-service"
}
