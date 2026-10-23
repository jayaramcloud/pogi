# Group K: Firewall Manager, User Notifications, Compute Optimizer, Cost Optimization Hub, EC2 Image Builder,
# Storage Gateway, End User Messaging, Serverless Application Repository, Oracle Database@AWS and Aurora DSQL.
# pogi_override.tf (from `pogi tf`) points this provider at pogi.
terraform {
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 6.0" }
  }
}

provider "aws" {}
