# An autoscaling group of 3 t3.small servers from a launch template, a Fargate service with 2 tasks,
# and a Lambda function. Region ap-southeast-1.
provider "aws" {
  region = "ap-southeast-1"
}

resource "aws_launch_template" "web" {
  name          = "web"
  instance_type = "t3.small"
  block_device_mappings {
    device_name = "/dev/xvda"
    ebs {
      volume_size = 20
      volume_type = "gp3"
    }
  }
}

resource "aws_autoscaling_group" "web" {
  name             = "web"
  min_size         = 1
  max_size         = 5
  desired_capacity = 3
  launch_template {
    id = aws_launch_template.web.id
  }
}

resource "aws_ecs_task_definition" "api" {
  family = "api"
  cpu    = "512"
  memory = "1024"
}

resource "aws_ecs_service" "api" {
  name            = "api"
  launch_type     = "FARGATE"
  task_definition = aws_ecs_task_definition.api.arn
  desired_count   = 2
}

resource "aws_lambda_function" "worker" {
  function_name = "worker"
  memory_size   = 256
}
