# The google provider against pogi: `pogi tf --cloud gcp > pogi_override.tf`, then
# terraform init && terraform apply. Plan afterwards shows no changes.
terraform {
  required_providers {
    google = { source = "hashicorp/google" }
  }
}

locals {
  project = "pogi"
  region  = "us-central1"
}

# Cloud Storage
resource "google_storage_bucket" "assets" {
  name                        = "pogi-tf-assets"
  location                    = "US"
  uniform_bucket_level_access = true
  force_destroy               = true
  labels                      = { env = "dev" }
  versioning {
    enabled = true
  }
}

resource "google_storage_bucket_object" "hello" {
  name         = "hello.txt"
  bucket       = google_storage_bucket.assets.name
  content      = "hello from terraform"
  content_type = "text/plain"
}

# Pub/Sub
resource "google_pubsub_topic" "orders" {
  name = "orders"
}

resource "google_pubsub_subscription" "worker" {
  name                 = "worker"
  topic                = google_pubsub_topic.orders.id
  ack_deadline_seconds = 30
  retry_policy {
    minimum_backoff = "10s"
  }
}

# Secret Manager
resource "google_secret_manager_secret" "db" {
  secret_id = "db-password"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "db" {
  secret      = google_secret_manager_secret.db.id
  secret_data = "hunter2"
}

# Cloud Tasks and Cloud Scheduler
resource "google_cloud_tasks_queue" "jobs" {
  name     = "jobs"
  location = local.region
  rate_limits {
    max_dispatches_per_second = 5
  }
}

resource "google_cloud_scheduler_job" "nightly" {
  name      = "nightly"
  region    = local.region
  schedule  = "0 3 * * *"
  time_zone = "Etc/UTC"
  pubsub_target {
    topic_name = google_pubsub_topic.orders.id
    data       = base64encode("tick")
  }
}

# Cloud Run (metadata only without Docker) and an Eventarc trigger that delivers to it
resource "google_cloud_run_v2_service" "api" {
  name                = "api"
  location            = local.region
  deletion_protection = false
  template {
    containers {
      image = "us-docker.pkg.dev/cloudrun/container/hello"
    }
  }
}

resource "google_eventarc_trigger" "uploads" {
  name     = "uploads"
  location = local.region
  matching_criteria {
    attribute = "type"
    value     = "google.cloud.pubsub.topic.v1.messagePublished"
  }
  destination {
    cloud_run_service {
      service = google_cloud_run_v2_service.api.name
      region  = local.region
    }
  }
  transport {
    pubsub {
      topic = google_pubsub_topic.orders.id
    }
  }
}

# Managed Service for Apache Kafka
resource "google_managed_kafka_cluster" "events" {
  cluster_id = "events"
  location   = local.region
  capacity_config {
    vcpu_count   = 3
    memory_bytes = 3221225472
  }
  gcp_config {
    access_config {
      network_configs {
        subnet = "projects/${local.project}/regions/${local.region}/subnetworks/default"
      }
    }
  }
}

resource "google_managed_kafka_topic" "clicks" {
  topic_id           = "clicks"
  cluster            = google_managed_kafka_cluster.events.cluster_id
  location           = local.region
  partition_count    = 3
  replication_factor = 3
}

# IAM, Cloud KMS and the project's services
resource "google_service_account" "app" {
  account_id   = "pogi-app"
  display_name = "The app"
}

resource "google_project_iam_member" "app_secrets" {
  project = local.project
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.app.email}"
}

resource "google_project_service" "run" {
  project            = local.project
  service            = "run.googleapis.com"
  disable_on_destroy = false
}

resource "google_kms_key_ring" "keys" {
  name     = "keys"
  location = local.region
}

resource "google_kms_crypto_key" "data" {
  name     = "data"
  key_ring = google_kms_key_ring.keys.id
  purpose  = "ENCRYPT_DECRYPT"
}

# BigQuery, Cloud Logging and Cloud Monitoring
resource "google_bigquery_dataset" "analytics" {
  dataset_id = "analytics"
  location   = "US"
}

resource "google_bigquery_table" "events" {
  dataset_id          = google_bigquery_dataset.analytics.dataset_id
  table_id            = "events"
  deletion_protection = false
  schema = jsonencode([
    { name = "id", type = "STRING", mode = "REQUIRED" },
    { name = "at", type = "TIMESTAMP", mode = "NULLABLE" },
  ])
}

resource "google_logging_project_sink" "errors" {
  name                   = "errors"
  destination            = "storage.googleapis.com/${google_storage_bucket.assets.name}"
  filter                 = "severity>=ERROR"
  unique_writer_identity = true
}

resource "google_monitoring_alert_policy" "latency" {
  display_name = "High latency"
  combiner     = "OR"
  conditions {
    display_name = "Latency over 1s"
    condition_threshold {
      filter          = "resource.type=\"cloud_run_revision\" AND metric.type=\"run.googleapis.com/request_latencies\""
      comparison      = "COMPARISON_GT"
      threshold_value = 1000
      duration        = "60s"
    }
  }
}

# Cloud SQL (a Postgres container when Docker runs)
resource "google_sql_database_instance" "db" {
  name                = "pogi-db"
  region              = local.region
  database_version    = "POSTGRES_15"
  deletion_protection = false
  settings {
    tier = "db-f1-micro"
  }
}

resource "google_sql_database" "app" {
  name     = "app"
  instance = google_sql_database_instance.db.name
}

output "bucket" {
  value = google_storage_bucket.assets.url
}
