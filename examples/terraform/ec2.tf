# EC2 and Auto Scaling: a launch template, a group of one with scaling
# policies, and an instance with a role. With Docker running, instances are
# containers that run their user data and read role credentials from IMDS.
data "aws_availability_zones" "available" {}

data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]
  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }
}

resource "aws_security_group" "web" {
  name        = "tf-web"
  description = "web servers"
  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_launch_template" "web" {
  name                   = "tf-web"
  image_id               = data.aws_ami.al2023.id
  instance_type          = "t3.micro"
  vpc_security_group_ids = [aws_security_group.web.id]
  user_data              = base64encode("#!/bin/sh\necho hello > /tmp/hello\n")
  tag_specifications {
    resource_type = "instance"
    tags          = { Name = "tf-web" }
  }
}

resource "aws_autoscaling_group" "web" {
  name               = "tf-web"
  min_size           = 1
  max_size           = 3
  desired_capacity   = 1
  availability_zones = [data.aws_availability_zones.available.names[0]]
  launch_template {
    id      = aws_launch_template.web.id
    version = "$Latest"
  }
  tag {
    key                 = "team"
    value               = "web"
    propagate_at_launch = true
  }
}

resource "aws_autoscaling_policy" "out" {
  name                   = "tf-scale-out"
  autoscaling_group_name = aws_autoscaling_group.web.name
  adjustment_type        = "ChangeInCapacity"
  scaling_adjustment     = 1
  cooldown               = 120
}

resource "aws_autoscaling_policy" "cpu" {
  name                   = "tf-cpu"
  autoscaling_group_name = aws_autoscaling_group.web.name
  policy_type            = "TargetTrackingScaling"
  target_tracking_configuration {
    target_value = 60
    predefined_metric_specification {
      predefined_metric_type = "ASGAverageCPUUtilization"
    }
  }
}

resource "aws_iam_role" "app" {
  name = "tf-app"
  assume_role_policy = jsonencode({ Version = "2012-10-17", Statement = [{ Effect = "Allow", Action = "sts:AssumeRole",
  Principal = { Service = "ec2.amazonaws.com" } }] })
}

resource "aws_iam_instance_profile" "app" {
  name = "tf-app"
  role = aws_iam_role.app.name
}

resource "aws_instance" "app" {
  ami                    = data.aws_ami.al2023.id
  instance_type          = "t3.micro"
  iam_instance_profile   = aws_iam_instance_profile.app.name
  vpc_security_group_ids = [aws_security_group.web.id]
  tags                   = { Name = "tf-app" }
}
