# Long-tail services served by the generated engine, one file per area.
# pogi_override.tf (from `pogi tf`) points this provider at pogi.
terraform {
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 6.0" }
  }
}

provider "aws" {}

data "aws_caller_identity" "current" {} # shared by cost.tf and security.tf
