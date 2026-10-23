# Thin network and infrastructure services on the generic Google engine: Network Intelligence Center, Cloud NGFW,
# Cloud Service Mesh, Cloud Domains, VMware Engine, Distributed Cloud, GKE Multi-Cloud, Backup for GKE, App Engine,
# Cloud Workstations, Cluster Director, VM Manager, Cloud Quotas and Infrastructure Manager.
# `pogi tf --cloud gcp` writes pogi_override.tf, which points the provider at pogi.
terraform {
  required_providers {
    google = { source = "hashicorp/google" }
  }
}

locals {
  project = "pogi"
  region  = "us-central1"
  # Cloud NGFW endpoints belong to an organization. pogi does not check the id.
  org = "organizations/123456789012"
}
