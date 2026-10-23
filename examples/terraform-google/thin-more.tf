# Thin Google Cloud services on the generic engine (group "more"): Looker, Certificate Authority
# Service, Cloud Healthcare, Dataplex, BigQuery Data Transfer and
# Storage Transfer. Needs main.tf in the same folder (it sets the provider).

resource "google_looker_instance" "looker_main" {
  name             = "looker-main"
  region           = "us-central1"
  platform_edition = "LOOKER_CORE_STANDARD_ANNUAL"
  oauth_config {
    client_id     = "client-id"
    client_secret = "client-secret"
  }
}

resource "google_privateca_ca_pool" "privateca_pool" {
  name     = "privateca-pool"
  location = "us-central1"
  tier     = "ENTERPRISE"
  labels   = { env = "dev" }
}

resource "google_privateca_certificate_authority" "privateca_ca" {
  pool                                   = google_privateca_ca_pool.privateca_pool.name
  certificate_authority_id               = "privateca-ca"
  location                               = "us-central1"
  deletion_protection                    = false
  skip_grace_period                      = true
  ignore_active_certificates_on_deletion = true
  config {
    subject_config {
      subject {
        organization = "pogi"
        common_name  = "pogi-ca"
      }
    }
    x509_config {
      ca_options {
        is_ca = true
      }
      key_usage {
        base_key_usage {
          cert_sign = true
          crl_sign  = true
        }
        extended_key_usage {
          server_auth = true
        }
      }
    }
  }
  key_spec {
    algorithm = "RSA_PKCS1_4096_SHA256"
  }
}

resource "google_healthcare_dataset" "healthcare_ds" {
  name      = "healthcare-ds"
  location  = "us-central1"
  time_zone = "UTC"
}

resource "google_dataplex_lake" "dataplex_lake" {
  name         = "dataplex-lake"
  location     = "us-central1"
  description  = "a lake"
  display_name = "Lake"
  labels       = { env = "dev" }
}

resource "google_bigquery_dataset" "bqtransfer_ds" {
  dataset_id = "bqtransfer_ds"
  location   = "US"
}

resource "google_bigquery_data_transfer_config" "bqtransfer_query" {
  display_name           = "bqtransfer-query"
  location               = "US"
  data_source_id         = "scheduled_query"
  schedule               = "first sunday of quarter 00:00"
  destination_dataset_id = google_bigquery_dataset.bqtransfer_ds.dataset_id
  params = {
    destination_table_name_template = "my_table"
    write_disposition               = "WRITE_APPEND"
    query                           = "SELECT 1 AS a"
  }
}

resource "google_storage_bucket" "storagetransfer_src" {
  name          = "pogi-tf-transfer-src"
  location      = "US"
  force_destroy = true
}

resource "google_storage_bucket" "storagetransfer_dst" {
  name          = "pogi-tf-transfer-dst"
  location      = "US"
  force_destroy = true
}

resource "google_storage_transfer_job" "storagetransfer_job" {
  description = "copy"
  transfer_spec {
    gcs_data_source {
      bucket_name = google_storage_bucket.storagetransfer_src.name
    }
    gcs_data_sink {
      bucket_name = google_storage_bucket.storagetransfer_dst.name
    }
  }
  schedule {
    schedule_start_date {
      year  = 2030
      month = 1
      day   = 1
    }
  }
}
