# MSK: a configuration and a cluster that uses it. With Docker running, the
# cluster is a Redpanda broker in a container, at its bootstrap_brokers.
resource "aws_msk_configuration" "kafka" {
  name           = "tf-msk"
  kafka_versions = ["3.8.x"]

  server_properties = <<-PROPERTIES
    auto.create.topics.enable = true
    delete.topic.enable = true
  PROPERTIES
}

resource "aws_msk_cluster" "kafka" {
  cluster_name           = "tf-msk"
  kafka_version          = "3.8.x"
  number_of_broker_nodes = 2

  broker_node_group_info {
    instance_type   = "kafka.t3.small"
    client_subnets  = ["subnet-0a", "subnet-0b"]
    security_groups = ["sg-0a"]

    storage_info {
      ebs_storage_info {
        volume_size = 10
      }
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.kafka.arn
    revision = aws_msk_configuration.kafka.latest_revision
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS_PLAINTEXT"
    }
  }
}
