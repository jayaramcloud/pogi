# Network Intelligence Center
resource "google_network_management_connectivity_test" "net_nic_test" {
  name = "net-nic-test"
  source {
    ip_address = "10.0.0.5"
    project_id = local.project
  }
  destination {
    ip_address = "10.0.1.7"
    project_id = local.project
  }
  protocol = "TCP"
  labels   = { env = "dev" }
}

# Cloud NGFW
resource "google_network_security_firewall_endpoint" "net_ngfw_endpoint" {
  name     = "net-ngfw-endpoint"
  parent   = local.org
  location = "${local.region}-a"
  labels   = { env = "dev" }
}

# Cloud Service Mesh
resource "google_network_services_mesh" "net_mesh" {
  name        = "net-mesh"
  description = "A mesh for the demo"
  labels      = { env = "dev" }
}

# Cloud Domains
resource "google_clouddomains_registration" "net_domain" {
  domain_name = "pogi-example.com"
  location    = "global"
  labels      = { env = "dev" }

  yearly_price {
    currency_code = "USD"
    units         = "12"
  }

  dns_settings {
    custom_dns {
      name_servers = ["ns1.example.com", "ns2.example.com"]
    }
  }

  contact_settings {
    privacy = "REDACTED_CONTACT_DATA"
    registrant_contact {
      phone_number = "+11234567890"
      email        = "registrant@example.com"
      postal_address {
        region_code         = "US"
        postal_code         = "94043"
        administrative_area = "CA"
        locality            = "Mountain View"
        address_lines       = ["1600 Amphitheatre Pkwy"]
        recipients          = ["Pogi Example"]
      }
    }
    admin_contact {
      phone_number = "+11234567890"
      email        = "admin@example.com"
      postal_address {
        region_code         = "US"
        postal_code         = "94043"
        administrative_area = "CA"
        locality            = "Mountain View"
        address_lines       = ["1600 Amphitheatre Pkwy"]
        recipients          = ["Pogi Example"]
      }
    }
    technical_contact {
      phone_number = "+11234567890"
      email        = "tech@example.com"
      postal_address {
        region_code         = "US"
        postal_code         = "94043"
        administrative_area = "CA"
        locality            = "Mountain View"
        address_lines       = ["1600 Amphitheatre Pkwy"]
        recipients          = ["Pogi Example"]
      }
    }
  }
}

# VMware Engine
resource "google_vmwareengine_network" "net_vmware" {
  name        = "net-vmware"
  location    = "global"
  type        = "STANDARD"
  description = "A VMware Engine network"
}

# Distributed Cloud
resource "google_edgecontainer_cluster" "net_edge" {
  name     = "net-edge"
  location = local.region
  labels   = { env = "dev" }

  authorization {
    admin_users {
      username = "admin@example.com"
    }
  }
  networking {
    cluster_ipv4_cidr_blocks  = ["10.200.0.0/16"]
    services_ipv4_cidr_blocks = ["10.225.0.0/16"]
  }
  fleet {
    project = "projects/${local.project}"
  }
}

# GKE Multi-Cloud
resource "google_container_azure_client" "net_azure_client" {
  name           = "net-azure-client"
  location       = local.region
  application_id = "12345678-1234-1234-1234-123456789111"
  tenant_id      = "12345678-1234-1234-1234-123456789111"
}

# Backup for GKE
resource "google_gke_backup_backup_plan" "net_backup_plan" {
  name     = "net-backup-plan"
  cluster  = "projects/${local.project}/locations/${local.region}/clusters/net-cluster"
  location = local.region
  labels   = { env = "dev" }
  backup_config {
    include_volume_data = true
    include_secrets     = true
    all_namespaces      = true
  }
}

# App Engine
resource "google_app_engine_application" "net_app" {
  project     = local.project
  location_id = local.region
}

# Cloud Workstations
resource "google_workstations_workstation_cluster" "net_ws_cluster" {
  workstation_cluster_id = "net-ws-cluster"
  location               = local.region
  network                = "projects/${local.project}/global/networks/default"
  subnetwork             = "projects/${local.project}/regions/${local.region}/subnetworks/default"
  labels                 = { env = "dev" }
}

resource "google_workstations_workstation_config" "net_ws_config" {
  workstation_config_id  = "net-ws-config"
  workstation_cluster_id = google_workstations_workstation_cluster.net_ws_cluster.workstation_cluster_id
  location               = local.region
  idle_timeout           = "600s"
  running_timeout        = "21600s"
  host {
    gce_instance {
      machine_type                = "e2-standard-4"
      boot_disk_size_gb           = 35
      disable_public_ip_addresses = true
    }
  }
}

# Cluster Director
resource "google_hypercomputecluster_cluster" "net_hpc" {
  cluster_id  = "net-hpc"
  location    = "us-central1"
  description = "A Cluster Director cluster"
  labels      = { env = "dev" }
  network_resources {
    id = "network1"
    config {
      new_network {
        network = "projects/${local.project}/global/networks/net-hpc"
      }
    }
  }
}

# VM Manager
resource "google_os_config_patch_deployment" "net_patch" {
  patch_deployment_id = "net-patch"
  instance_filter {
    all = true
  }
  one_time_schedule {
    execute_time = "2030-01-01T00:00:00Z"
  }
}

# Cloud Quotas
resource "google_cloud_quotas_quota_adjuster_settings" "net_quota_adjuster" {
  parent     = "projects/${local.project}"
  enablement = "ENABLED"
}

resource "google_cloud_quotas_quota_preference" "net_quota_preference" {
  parent        = "projects/${local.project}"
  name          = "net-quota-preference"
  dimensions    = { region = local.region }
  service       = "compute.googleapis.com"
  quota_id      = "CPUS-per-project-region"
  contact_email = "admin@example.com"
  quota_config {
    preferred_value = 200
  }
}

# Infrastructure Manager
resource "google_config_deployment" "net_deployment" {
  name            = "net-deployment"
  location        = local.region
  service_account = "projects/${local.project}/serviceAccounts/infra@${local.project}.iam.gserviceaccount.com"
  labels          = { env = "dev" }
  terraform_blueprint {
    git_source {
      repo      = "https://example.com/pogi/blueprint.git"
      directory = "."
      ref       = "main"
    }
  }
}
