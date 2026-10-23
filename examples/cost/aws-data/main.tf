# Data services: a provisioned DynamoDB table, a Kinesis stream, an MSK cluster, an OpenSearch domain,
# a key and a secret. Region us-east-1.
provider "aws" {
  region = "us-east-1"
}

resource "aws_dynamodb_table" "orders" {
  name           = "orders"
  hash_key       = "id"
  read_capacity  = 20
  write_capacity = 10
  global_secondary_index {
    name            = "by_user"
    hash_key        = "user"
    read_capacity   = 5
    write_capacity  = 5
    projection_type = "ALL"
  }
}

resource "aws_kinesis_stream" "events" {
  name             = "events"
  shard_count      = 2
  retention_period = 48
}

resource "aws_msk_cluster" "bus" {
  cluster_name           = "bus"
  kafka_version          = "3.6.0"
  number_of_broker_nodes = 3
  broker_node_group_info {
    instance_type = "kafka.m5.large"
    storage_info {
      ebs_storage_info {
        volume_size = 500
      }
    }
  }
}

resource "aws_opensearch_domain" "search" {
  domain_name = "search"
  cluster_config {
    instance_type  = "r6g.large.search"
    instance_count = 3
  }
  ebs_options {
    ebs_enabled = true
    volume_type = "gp3"
    volume_size = 100
  }
}

resource "aws_kms_key" "data" {
  description = "data key"
}

resource "aws_secretsmanager_secret" "db" {
  name = "db-password"
}
