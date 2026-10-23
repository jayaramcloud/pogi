# A small web stack: two web servers behind a load balancer, a Multi-AZ Postgres database,
# a Redis cache and a NAT gateway. Region us-east-1.
provider "aws" {
  region = "us-east-1"
}

resource "aws_instance" "web_a" {
  ami                         = "ami-0123456789abcdef0"
  instance_type               = "m5.large"
  associate_public_ip_address = false
  root_block_device {
    volume_size = 30
    volume_type = "gp3"
  }
}

resource "aws_instance" "web_b" {
  ami                         = "ami-0123456789abcdef0"
  instance_type               = "m5.large"
  associate_public_ip_address = false
  root_block_device {
    volume_size = 30
    volume_type = "gp3"
  }
}

resource "aws_lb" "app" {
  name               = "app"
  load_balancer_type = "application"
}

resource "aws_db_instance" "db" {
  engine            = "postgres"
  instance_class    = "db.t3.medium"
  multi_az          = true
  allocated_storage = 100
  storage_type      = "gp3"
}

resource "aws_elasticache_replication_group" "cache" {
  replication_group_id = "cache"
  description          = "session cache"
  engine               = "redis"
  node_type            = "cache.t3.small"
  num_cache_clusters   = 2
}

resource "aws_eip" "nat" {
  domain = "vpc"
}

resource "aws_nat_gateway" "main" {
  allocation_id = aws_eip.nat.id
  subnet_id     = "subnet-0123456789abcdef0"
}
