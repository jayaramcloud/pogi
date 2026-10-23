provider "google" {
  project = "demo"
  region  = "us-central1"
}

resource "google_cloud_run_v2_service" "api" {
  name     = "api"
  location = "us-central1"
  template {
    containers {
      image = "us-docker.pkg.dev/cloudrun/container/hello"
      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
      }
    }
  }
}

resource "google_storage_bucket" "assets" {
  name     = "demo-assets"
  location = "US"
}

resource "google_pubsub_topic" "events" {
  name = "events"
}

resource "google_bigquery_dataset" "logs" {
  dataset_id = "logs"
  location   = "US"
}
