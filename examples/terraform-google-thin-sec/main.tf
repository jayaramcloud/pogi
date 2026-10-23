# Group A, security and identity (Thin, on the generic engine): the google provider against pogi.
# `pogi tf --cloud gcp > pogi_override.tf`, then terraform init && terraform apply. A plan afterwards
# shows no changes. The organization, customer and billing account ids are made up: pogi does not check them.
terraform {
  required_providers {
    google = { source = "hashicorp/google" }
  }
}

locals {
  project = "pogi"
  org     = "organizations/123456789012"
}
