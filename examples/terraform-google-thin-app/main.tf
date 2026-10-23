# Thin Google Cloud services, group "app" (Apigee to Firebase Data Connect). The google provider
# against pogi: `pogi tf --cloud gcp > pogi_override.tf`, then terraform init && terraform apply.
terraform {
  required_providers {
    google = { source = "hashicorp/google" }
  }
}

locals {
  project = "pogi"
  region  = "us-central1"
}
