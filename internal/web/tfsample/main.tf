# A small web stack on AWS: network, load balancer, two app servers, a database and a bucket.
provider "aws" {
  region = "us-east-1"
}

variable "app_count" {
  default = 2
}

module "net" {
  source = "./modules/net"
}

resource "aws_security_group" "web" {
  name   = "web"
  vpc_id = module.net.vpc_id
}

resource "aws_lb" "web" {
  name               = "web"
  load_balancer_type = "application"
  subnets            = module.net.public_subnets
  security_groups    = [aws_security_group.web.id]
}

resource "aws_lb_target_group" "web" {
  port     = 80
  protocol = "HTTP"
  vpc_id   = module.net.vpc_id
}

resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.web.arn
  port              = 80
  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.web.arn
  }
}

resource "aws_instance" "app" {
  count         = var.app_count
  ami           = "ami-0abcdef1234567890"
  instance_type = "m7g.large"
  subnet_id     = module.net.private_subnets[count.index]
  root_block_device {
    volume_size = 50
  }
}

resource "aws_lb_target_group_attachment" "app" {
  count            = var.app_count
  target_group_arn = aws_lb_target_group.web.arn
  target_id        = aws_instance.app[count.index].id
}

resource "aws_db_subnet_group" "db" {
  name       = "db"
  subnet_ids = module.net.private_subnets
}

resource "aws_db_instance" "db" {
  engine               = "postgres"
  instance_class       = "db.t4g.medium"
  allocated_storage    = 100
  multi_az             = true
  db_subnet_group_name = aws_db_subnet_group.db.name
}

resource "aws_s3_bucket" "assets" {
  bucket = "pogi-sample-assets"
}
