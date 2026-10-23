# Cloud Spanner: Google's emulator in Docker behind pogi (needs Docker).
resource "google_spanner_instance" "spanner_main" {
  name         = "spanner-tf"
  config       = "regional-us-central1"
  display_name = "pogi spanner"
  num_nodes    = 1
  labels       = { env = "dev" }
}

resource "google_spanner_database" "spanner_app" {
  instance            = google_spanner_instance.spanner_main.name
  name                = "app"
  deletion_protection = false
  ddl = [
    "CREATE TABLE users (id INT64 NOT NULL, name STRING(100)) PRIMARY KEY (id)",
    "CREATE INDEX users_by_name ON users (name)",
  ]
}
