# Amazon MQ: a RabbitMQ broker running a configuration revision. With Docker
# running, the broker is a RabbitMQ container: its AMQP endpoint and the
# management console answer on 127.0.0.1.
resource "aws_mq_configuration" "rabbit" {
  name           = "tf-rabbit"
  engine_type    = "RabbitMQ"
  engine_version = "3.13"
  data           = "consumer_timeout = 3600000\n"
}

resource "aws_mq_broker" "rabbit" {
  broker_name                = "tf-rabbit"
  engine_type                = "RabbitMQ"
  engine_version             = "3.13"
  host_instance_type         = "mq.t3.micro"
  auto_minor_version_upgrade = true
  configuration {
    id       = aws_mq_configuration.rabbit.id
    revision = aws_mq_configuration.rabbit.latest_revision
  }
  user {
    username = "admin"
    password = "local-password-1"
  }
  tags = { team = "platform" }
}

output "mq_console" { value = aws_mq_broker.rabbit.instances[0].console_url }
