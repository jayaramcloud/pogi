# Thin data and network services on the generic Google engine: Dataproc, Cloud Composer, Dataflow,
# Memorystore for Memcached, Serverless VPC Access, Network Connectivity Center, AlloyDB and Datastream.
# Needs main.tf in the same folder (it sets the provider).

resource "google_dataproc_cluster" "dataproc_demo" {
  name   = "dataproc-demo"
  region = "us-central1"
  labels = { env = "dev" }

  cluster_config {
    master_config {
      num_instances = 1
      machine_type  = "n2-standard-2"
    }
    worker_config {
      num_instances = 2
      machine_type  = "n2-standard-2"
    }
  }
}

resource "google_composer_environment" "composer_demo" {
  name   = "composer-demo"
  region = "us-central1"
  labels = { env = "dev" }

  config {
    software_config {
      image_version = "composer-2-airflow-2"
    }
  }
}

resource "google_dataflow_job" "dataflow_demo" {
  name              = "dataflow-demo"
  region            = "us-central1"
  template_gcs_path = "gs://dataflow-templates/latest/Word_Count"
  temp_gcs_location = "gs://pogi-tf-assets/tmp"
  parameters = {
    inputFile = "gs://dataflow-samples/shakespeare/kinglear.txt"
    output    = "gs://pogi-tf-assets/out"
  }
  on_delete = "cancel"
}

resource "google_memcache_instance" "memcache_demo" {
  name             = "memcache-demo"
  region           = "us-central1"
  node_count       = 1
  memcache_version = "MEMCACHE_1_5"
  labels           = { env = "dev" }

  node_config {
    cpu_count      = 1
    memory_size_mb = 1024
  }
}

resource "google_vpc_access_connector" "vpcaccess_demo" {
  name          = "vpcaccess-demo"
  region        = "us-central1"
  network       = "default"
  ip_cidr_range = "10.8.0.0/28"
}

resource "google_network_connectivity_hub" "ncc_demo" {
  name        = "ncc-demo"
  description = "demo hub"
  labels      = { env = "dev" }
}

resource "google_network_connectivity_spoke" "ncc_demo" {
  name     = "ncc-demo-spoke"
  location = "global"
  hub      = google_network_connectivity_hub.ncc_demo.id
  linked_vpc_network {
    uri = "projects/pogi/global/networks/default"
  }
}

resource "google_alloydb_cluster" "alloydb_demo" {
  cluster_id          = "alloydb-demo"
  location            = "us-central1"
  deletion_protection = false
  network_config {
    network = "projects/pogi/global/networks/default"
  }
  initial_user {
    password = "alloydb-demo-pw"
  }
}

resource "google_alloydb_instance" "alloydb_demo" {
  cluster       = google_alloydb_cluster.alloydb_demo.name
  instance_id   = "alloydb-demo-primary"
  instance_type = "PRIMARY"
  machine_config {
    cpu_count = 2
  }
}

resource "google_datastream_connection_profile" "datastream_demo" {
  display_name          = "datastream-demo"
  location              = "us-central1"
  connection_profile_id = "datastream-demo"
  labels                = { env = "dev" }

  gcs_profile {
    bucket    = "pogi-tf-assets"
    root_path = "/datastream"
  }
}
