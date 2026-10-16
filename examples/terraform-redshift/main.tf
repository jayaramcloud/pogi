terraform {
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 6.0" }
  }
}
# pogi_override.tf (from `pogi tf`) points this provider at pogi.
provider "aws" {}

resource "aws_redshift_parameter_group" "etl" {
  name   = "tf-redshift-etl"
  family = "redshift-1.0"
  parameter {
    name  = "require_ssl"
    value = "true"
  }
  parameter {
    name  = "query_group"
    value = "etl"
  }
  tags = { env = "dev" }
}

resource "aws_redshift_cluster" "main" {
  cluster_identifier           = "tf-redshift"
  node_type                    = "ra3.xlplus"
  database_name                = "analytics"
  master_username              = "awsuser"
  master_password              = "Secret123"
  cluster_parameter_group_name = aws_redshift_parameter_group.etl.name
  skip_final_snapshot          = true
  tags                         = { env = "dev" }
}

resource "aws_redshiftserverless_namespace" "main" {
  namespace_name      = "tf-namespace"
  admin_username      = "admin"
  admin_user_password = "Secret123"
  db_name             = "dev"
  tags                = { env = "dev" }
}

resource "aws_redshiftserverless_workgroup" "main" {
  namespace_name = aws_redshiftserverless_namespace.main.namespace_name
  workgroup_name = "tf-workgroup"
  base_capacity  = 32
  tags           = { env = "dev" }
}

output "endpoints" {
  value = {
    cluster   = aws_redshift_cluster.main.endpoint
    workgroup = "${aws_redshiftserverless_workgroup.main.endpoint[0].address}:${aws_redshiftserverless_workgroup.main.endpoint[0].port}"
  }
}
