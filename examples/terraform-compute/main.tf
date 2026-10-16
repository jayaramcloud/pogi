# ECR + ECS behind an ALB + an Auto Scaling group. With Docker running, the
# service's two nginx tasks answer through the load balancer.
terraform {
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 6.0" }
  }
}
# pogi_override.tf (from `pogi tf`) points this provider at pogi.
provider "aws" {}

data "aws_vpc" "default" { default = true }
data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

resource "aws_ecr_repository" "app" {
  name         = "tf-app"
  force_delete = true
}

resource "aws_lb" "web" {
  name               = "tf-web"
  load_balancer_type = "application"
  subnets            = data.aws_subnets.default.ids
}
resource "aws_lb_target_group" "web" {
  name        = "tf-web"
  port        = 80
  protocol    = "HTTP"
  target_type = "ip"
  vpc_id      = data.aws_vpc.default.id
  health_check { path = "/" }
}
resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.web.arn
  port              = 80
  protocol          = "HTTP"
  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.web.arn
  }
}

resource "aws_ecs_cluster" "main" { name = "tf-main" }
resource "aws_ecs_task_definition" "web" {
  family                   = "tf-web"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "256"
  memory                   = "512"
  container_definitions = jsonencode([{
    name         = "web"
    image        = "nginx:alpine"
    essential    = true
    portMappings = [{ containerPort = 80, protocol = "tcp" }]
  }])
}
resource "aws_ecs_service" "web" {
  name            = "tf-web"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.web.arn
  desired_count   = 2
  launch_type     = "FARGATE"
  network_configuration {
    subnets          = data.aws_subnets.default.ids
    assign_public_ip = true
  }
  load_balancer {
    target_group_arn = aws_lb_target_group.web.arn
    container_name   = "web"
    container_port   = 80
  }
  wait_for_steady_state = true
  depends_on            = [aws_lb_listener.http]
}

resource "aws_launch_template" "worker" {
  name          = "tf-worker"
  image_id      = "ami-0c02fb55956c7d316"
  instance_type = "t3.micro"
}
resource "aws_autoscaling_group" "workers" {
  name                = "tf-workers"
  min_size            = 1
  max_size            = 2
  desired_capacity    = 1
  vpc_zone_identifier = data.aws_subnets.default.ids
  launch_template {
    id      = aws_launch_template.worker.id
    version = "$Latest"
  }
}

output "url" { value = "http://${aws_lb.web.dns_name}" }
output "repository" { value = aws_ecr_repository.app.repository_url }
