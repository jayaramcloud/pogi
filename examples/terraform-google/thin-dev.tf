# Thin Google services of group A on the generic engine (internal/gcp/generic): Cloud Build,
# Cloud Deploy, API Keys, Service Directory, Binary Authorization,
# Container Analysis, Essential Contacts, Organization Policy and Cloud Asset Inventory.
# Needs main.tf in the same folder (it sets the provider).

resource "google_cloudbuild_trigger" "thin_dev_build" {
  name     = "thin-dev-build"
  filename = "cloudbuild.yaml"
  trigger_template {
    repo_name   = "thin-dev-repo"
    branch_name = "main"
  }
}

resource "google_cloudbuild_worker_pool" "thin_dev_pool" {
  name     = "thin-dev-pool"
  location = "us-central1"
  worker_config {
    disk_size_gb   = 100
    machine_type   = "e2-standard-4"
    no_external_ip = false
  }
}

resource "google_clouddeploy_target" "thin_dev_target" {
  name     = "thin-dev-target"
  location = "us-central1"
  gke {
    cluster = "projects/pogi/locations/us-central1/clusters/thin-dev"
  }
}

resource "google_clouddeploy_delivery_pipeline" "thin_dev_pipeline" {
  name     = "thin-dev-pipeline"
  location = "us-central1"
  serial_pipeline {
    stages {
      target_id = google_clouddeploy_target.thin_dev_target.name
    }
  }
}

resource "google_apikeys_key" "thin_dev_key" {
  name         = "thin-dev-key"
  display_name = "thin dev key"
  restrictions {
    api_targets {
      service = "translate.googleapis.com"
    }
  }
}

resource "google_service_directory_namespace" "thin_dev_ns" {
  namespace_id = "thin-dev-ns"
  location     = "us-central1"
  labels       = { env = "dev" }
}

resource "google_service_directory_service" "thin_dev_svc" {
  service_id = "thin-dev-svc"
  namespace  = google_service_directory_namespace.thin_dev_ns.id
}

resource "google_service_directory_endpoint" "thin_dev_ep" {
  endpoint_id = "thin-dev-ep"
  service     = google_service_directory_service.thin_dev_svc.id
  address     = "10.0.0.5"
  port        = 8080
}

resource "google_container_analysis_note" "thin_dev_note" {
  name = "thin-dev-note"
  attestation_authority {
    hint {
      human_readable_name = "thin dev attestor"
    }
  }
}

resource "google_container_analysis_occurrence" "thin_dev_occurrence" {
  resource_uri = "https://gcr.io/pogi/thin-dev@sha256:0000000000000000000000000000000000000000000000000000000000000000"
  note_name    = google_container_analysis_note.thin_dev_note.id
  attestation {
    serialized_payload = base64encode("{}")
    signatures {
      public_key_id = "thin-dev-key"
      signature     = base64encode("signed")
    }
  }
}

resource "google_binary_authorization_attestor" "thin_dev_attestor" {
  name = "thin-dev-attestor"
  attestation_authority_note {
    note_reference = google_container_analysis_note.thin_dev_note.name
  }
}

resource "google_binary_authorization_policy" "thin_dev_policy" {
  default_admission_rule {
    evaluation_mode         = "REQUIRE_ATTESTATION"
    enforcement_mode        = "ENFORCED_BLOCK_AND_AUDIT_LOG"
    require_attestations_by = [google_binary_authorization_attestor.thin_dev_attestor.name]
  }
}

resource "google_essential_contacts_contact" "thin_dev_contact" {
  parent                              = "projects/${local.project}"
  email                               = "thin-dev@example.com"
  language_tag                        = "en"
  notification_category_subscriptions = ["ALL"]
}

resource "google_org_policy_policy" "thin_dev_policy" {
  name   = "projects/${local.project}/policies/iam.disableServiceAccountKeyCreation"
  parent = "projects/${local.project}"
  spec {
    rules {
      enforce = "TRUE"
    }
  }
}

resource "google_cloud_asset_project_feed" "thin_dev_feed" {
  project         = local.project
  feed_id         = "thin-dev-feed"
  content_type    = "RESOURCE"
  billing_project = local.project
  asset_types     = ["compute.googleapis.com/Instance"]
  feed_output_config {
    pubsub_destination {
      topic = "projects/${local.project}/topics/thin-dev-feed"
    }
  }
}
