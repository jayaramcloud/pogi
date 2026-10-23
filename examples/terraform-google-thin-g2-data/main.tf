# Thin Google data services, round 2: Analytics Hub, BigLake, Data Fusion, Dataform, Dataproc Metastore,
# Database Migration Service, Memorystore for Valkey, Oracle Database@Google Cloud, Backup and DR,
# NetApp Volumes, Parallelstore, Managed Lustre and the Transcoder API.
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

resource "google_bigquery_analytics_hub_data_exchange" "ah_demo" {
  location         = "US"
  data_exchange_id = "ah_demo"
  display_name     = "ah-demo"
  description      = "a data exchange"
}

resource "google_biglake_catalog" "bl_demo" {
  name     = "bl-demo"
  location = "US"
}

resource "google_data_fusion_instance" "df_demo" {
  name   = "df-demo"
  region = local.region
  type   = "BASIC"
  labels = { env = "dev" }
}

resource "google_dataform_repository" "dform_demo" {
  name   = "dform-demo"
  region = local.region
  labels = { env = "dev" }
}

resource "google_dataproc_metastore_service" "ms_demo" {
  service_id = "ms-demo"
  location   = local.region
  labels     = { env = "dev" }

  hive_metastore_config {
    version = "3.1.2"
  }
}

resource "google_database_migration_service_connection_profile" "dms_demo" {
  location              = local.region
  connection_profile_id = "dms-demo"
  display_name          = "dms-demo"
  labels                = { env = "dev" }

  mysql {
    host     = "10.0.0.5"
    port     = 3306
    username = "root"
    password = "not-a-real-password"
  }
}

resource "google_memorystore_instance" "valkey_demo" {
  instance_id                 = "valkey-demo"
  location                    = local.region
  shard_count                 = 1
  deletion_protection_enabled = false
  labels                      = { env = "dev" }

  desired_auto_created_endpoints {
    network    = "projects/${local.project}/global/networks/default"
    project_id = local.project
  }
}

resource "google_oracle_database_cloud_exadata_infrastructure" "ora_demo" {
  cloud_exadata_infrastructure_id = "ora-demo"
  display_name                    = "ora-demo"
  location                        = local.region
  deletion_protection             = false

  properties {
    shape         = "Exadata.X9M"
    compute_count = 2
    storage_count = 3
  }
}

resource "google_backup_dr_backup_vault" "bdr_demo" {
  location                                   = local.region
  backup_vault_id                            = "bdr-demo"
  backup_minimum_enforced_retention_duration = "100000s"
  ignore_inactive_datasources                = true
  labels                                     = { env = "dev" }
}

resource "google_netapp_storage_pool" "na_demo" {
  name          = "na-demo"
  location      = local.region
  service_level = "PREMIUM"
  capacity_gib  = "2048"
  network       = "projects/${local.project}/global/networks/default"
  labels        = { env = "dev" }
}

resource "google_parallelstore_instance" "ps_demo" {
  instance_id  = "ps-demo"
  location     = "us-central1-a"
  capacity_gib = 12000
  network      = "projects/${local.project}/global/networks/default"
  labels       = { env = "dev" }
}

resource "google_lustre_instance" "lu_demo" {
  instance_id                 = "lu-demo"
  location                    = "us-central1-a"
  filesystem                  = "lfs"
  capacity_gib                = 18000
  per_unit_storage_throughput = 500
  network                     = "projects/${local.project}/global/networks/default"
  labels                      = { env = "dev" }
}

resource "google_transcoder_job_template" "tc_demo" {
  job_template_id = "tc-demo"
  location        = local.region
  labels          = { env = "dev" }

  config {
    elementary_streams {
      key = "video-stream0"
      video_stream {
        h264 {
          height_pixels     = 360
          width_pixels      = 640
          bitrate_bps       = 550000
          frame_rate        = 60
          pixel_format      = "yuv420p"
          rate_control_mode = "vbr"
        }
      }
    }
    mux_streams {
      key                = "sd"
      container          = "mp4"
      elementary_streams = ["video-stream0"]
    }
  }
}
