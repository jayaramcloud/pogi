provider "google" {
  project = "demo"
  region  = "us-central1"
}

variable "nodes" {
  default = 2
}

resource "google_container_cluster" "main" {
  name                     = "main"
  location                 = "us-central1-a"
  remove_default_node_pool = true
  initial_node_count       = 1
}

resource "google_container_node_pool" "workers" {
  name       = "workers"
  cluster    = google_container_cluster.main.name
  location   = "us-central1-a"
  node_count = var.nodes
  node_config {
    machine_type = "e2-standard-4"
    disk_size_gb = 50
    disk_type    = "pd-standard"
  }
}

resource "google_compute_address" "lb" {
  name = "lb"
}

resource "google_compute_forwarding_rule" "web" {
  name                  = "web"
  load_balancing_scheme = "EXTERNAL"
  ip_address            = google_compute_address.lb.address
  port_range            = "80"
}

resource "google_compute_router_nat" "nat" {
  name   = "nat"
  router = "r"
}
