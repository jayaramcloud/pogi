provider "google" {
  project = "demo"
  region  = "us-central1"
}

resource "google_compute_instance" "web" {
  name         = "web"
  zone         = "us-central1-a"
  machine_type = "n2-standard-2"
  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-12"
      size  = 50
      type  = "pd-balanced"
    }
  }
  network_interface {
    network = "default"
    access_config {}
  }
}

resource "google_sql_database_instance" "db" {
  name             = "db"
  region           = "us-central1"
  database_version = "POSTGRES_15"
  settings {
    tier      = "db-custom-2-7680"
    disk_size = 100
  }
}

resource "google_redis_instance" "cache" {
  name           = "cache"
  region         = "us-central1"
  tier           = "BASIC"
  memory_size_gb = 2
}
