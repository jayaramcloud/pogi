# A Kubernetes cluster: the control plane, 3 managed nodes, a Network Load Balancer and a NAT gateway.
provider "aws" {
  region = "eu-west-1"
}

variable "nodes" {
  default = 3
}

resource "aws_eks_cluster" "main" {
  name     = "main"
  role_arn = "arn:aws:iam::123456789012:role/eks"
  vpc_config {
    subnet_ids = ["subnet-a", "subnet-b"]
  }
}

resource "aws_eks_node_group" "workers" {
  cluster_name    = aws_eks_cluster.main.name
  node_group_name = "workers"
  node_role_arn   = "arn:aws:iam::123456789012:role/nodes"
  subnet_ids      = ["subnet-a", "subnet-b"]
  instance_types  = ["m5.large"]
  disk_size       = 50
  scaling_config {
    desired_size = var.nodes
    min_size     = 1
    max_size     = 6
  }
}

resource "aws_lb" "ingress" {
  name               = "ingress"
  load_balancer_type = "network"
}

resource "aws_nat_gateway" "main" {
  allocation_id = "eipalloc-0123456789abcdef0"
  subnet_id     = "subnet-a"
}
