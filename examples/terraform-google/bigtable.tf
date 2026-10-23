# Cloud Bigtable: instance admin by pogi, tables and data by the official emulator (bttest), in process.
# The provider reaches Bigtable through the Go client, which needs BIGTABLE_EMULATOR_HOST=<pogi's address>.
resource "google_bigtable_instance" "bigtable_main" {
  name                = "bigtable-tf"
  deletion_protection = false
  labels              = { env = "dev" }
  cluster {
    cluster_id   = "bigtable-tf-c1"
    zone         = "us-central1-b"
    num_nodes    = 1
    storage_type = "SSD"
  }
}

resource "google_bigtable_table" "bigtable_events" {
  name          = "events"
  instance_name = google_bigtable_instance.bigtable_main.name
  column_family {
    family = "cf"
  }
}
