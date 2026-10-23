# Azure storage, data and health services of pogi's generic Azure engine (NetApp Files, Managed Lustre, Elastic
# SAN, Storage Mover, File Sync, Data Share, Database Migration Service, Analysis Services, HDInsight, Power BI
# Embedded, Fabric, Managed Redis, Oracle Database@Azure, Health Data Services, Digital Twins, IoT Hub Device
# Provisioning). Resources are stored and read back; nothing runs behind them.
# `pogi tf --cloud azure` writes pogi_override.tf, which points the provider at pogi.
terraform {
  required_providers {
    azurerm = { source = "hashicorp/azurerm" }
  }
}

resource "azurerm_resource_group" "rg" {
  name     = "pogi-tf-a2-data"
  location = "eastus"
}

resource "azurerm_virtual_network" "a2d" {
  name                = "a2d-vnet"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  address_space       = ["10.30.0.0/16"]
}

resource "azurerm_subnet" "a2d_lustre" {
  name                 = "a2d-lustre"
  resource_group_name  = azurerm_resource_group.rg.name
  virtual_network_name = azurerm_virtual_network.a2d.name
  address_prefixes     = ["10.30.2.0/24"]
}

# NetApp Files
resource "azurerm_netapp_account" "a2d" {
  name                = "a2d-netapp"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
}

resource "azurerm_netapp_pool" "a2d" {
  name                = "a2d-pool"
  account_name        = azurerm_netapp_account.a2d.name
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  service_level       = "Standard"
  size_in_tb          = 4
}

# The provider refuses to destroy a NetApp volume unless features { netapp { prevent_volume_destruction =
# false } } is set, and the generated pogi_override.tf owns the provider block: volumes are covered by the SDK test.

# Managed Lustre
resource "azurerm_managed_lustre_file_system" "a2d" {
  name                   = "a2d-lustre"
  resource_group_name    = azurerm_resource_group.rg.name
  location               = azurerm_resource_group.rg.location
  sku_name               = "AMLFS-Durable-Premium-125"
  subnet_id              = azurerm_subnet.a2d_lustre.id
  storage_capacity_in_tb = 16
  zones                  = ["1"]
  maintenance_window {
    day_of_week        = "Friday"
    time_of_day_in_utc = "22:00"
  }
}

# Elastic SAN
resource "azurerm_elastic_san" "a2d" {
  name                = "a2d-san"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  base_size_in_tib    = 1
  sku {
    name = "Premium_LRS"
  }
}

resource "azurerm_elastic_san_volume_group" "a2d" {
  name           = "a2d-vg"
  elastic_san_id = azurerm_elastic_san.a2d.id
}

resource "azurerm_elastic_san_volume" "a2d" {
  name            = "a2d-vol"
  volume_group_id = azurerm_elastic_san_volume_group.a2d.id
  size_in_gib     = 1
}

# Storage Mover
resource "azurerm_storage_mover" "a2d" {
  name                = "a2d-mover"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
}

resource "azurerm_storage_mover_project" "a2d" {
  name             = "a2d-project"
  storage_mover_id = azurerm_storage_mover.a2d.id
  description      = "pogi project"
}

resource "azurerm_storage_mover_source_endpoint" "a2d" {
  name             = "a2d-source"
  storage_mover_id = azurerm_storage_mover.a2d.id
  host             = "192.168.0.1"
  export           = "/export"
}

# File Sync
resource "azurerm_storage_sync" "a2d" {
  name                = "a2d-sync"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
}

resource "azurerm_storage_sync_group" "a2d" {
  name            = "a2d-group"
  storage_sync_id = azurerm_storage_sync.a2d.id
}

# Data Share
resource "azurerm_data_share_account" "a2d" {
  name                = "a2d-share-account"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_data_share" "a2d" {
  name        = "a2d-share"
  account_id  = azurerm_data_share_account.a2d.id
  kind        = "CopyBased"
  description = "pogi share"
  terms       = "terms"
  snapshot_schedule {
    name       = "a2d-schedule"
    recurrence = "Day"
    start_time = "2030-01-01T09:00:00Z"
  }
}

# Analysis Services
resource "azurerm_analysis_services_server" "a2d" {
  name                = "a2danalysis"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "B1"
}

# HDInsight
resource "azurerm_storage_account" "a2d" {
  name                     = "a2dhdistore"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

resource "azurerm_storage_container" "a2d" {
  name               = "a2dhdi"
  storage_account_id = azurerm_storage_account.a2d.id
}

resource "azurerm_hdinsight_hadoop_cluster" "a2d" {
  name                = "a2d-hdi"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  cluster_version     = "5.1"
  tier                = "Standard"
  tls_min_version     = "1.2"
  component_version {
    hadoop = "3.1"
  }
  gateway {
    username = "a2dgateway"
    password = "PogiTest123!"
  }
  storage_account {
    storage_account_id    = azurerm_storage_account.a2d.id
    storage_container_url = "${azurerm_storage_account.a2d.primary_blob_endpoint}${azurerm_storage_container.a2d.name}"
    storage_account_key   = azurerm_storage_account.a2d.primary_access_key
    is_default            = true
  }
  roles {
    head_node {
      vm_size  = "Standard_D3_V2"
      username = "a2dadmin"
      password = "PogiTest123!"
    }
    worker_node {
      vm_size               = "Standard_D3_V2"
      username              = "a2dadmin"
      password              = "PogiTest123!"
      target_instance_count = 2
    }
    zookeeper_node {
      vm_size  = "Standard_D3_V2"
      username = "a2dadmin"
      password = "PogiTest123!"
    }
  }
}

# Power BI Embedded
resource "azurerm_powerbi_embedded" "a2d" {
  name                = "a2dpowerbi"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku_name            = "A1"
  administrators      = ["admin@pogi.test"]
}

# Fabric capacity
resource "azurerm_fabric_capacity" "a2d" {
  name                   = "a2dfabric"
  resource_group_name    = azurerm_resource_group.rg.name
  location               = azurerm_resource_group.rg.location
  administration_members = ["admin@pogi.test"]
  sku {
    name = "F2"
    tier = "Fabric"
  }
}

# Managed Redis
resource "azurerm_managed_redis" "a2d" {
  name                = "a2d-redis"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku_name            = "Balanced_B0"
  default_database {
    access_keys_authentication_enabled = true
  }
}

# Oracle Database@Azure
resource "azurerm_oracle_exadata_infrastructure" "a2d" {
  name                = "a2d-exadata"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  display_name        = "a2d-exadata"
  shape               = "Exadata.X9M"
  zones               = ["1"]
  compute_count       = 2
  storage_count       = 3
}

# Health Data Services
resource "azurerm_healthcare_workspace" "a2d" {
  name                = "a2dhealth"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
}

resource "azurerm_healthcare_fhir_service" "a2d" {
  name                = "a2dfhir"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  workspace_id        = azurerm_healthcare_workspace.a2d.id
  kind                = "fhir-R4"
  authentication {
    authority = "https://login.microsoftonline.com/${data.azurerm_client_config.current.tenant_id}"
    audience  = "https://a2dhealth-a2dfhir.fhir.azurehealthcareapis.com"
  }
}

resource "azurerm_healthcare_dicom_service" "a2d" {
  name         = "a2ddicom"
  location     = azurerm_resource_group.rg.location
  workspace_id = azurerm_healthcare_workspace.a2d.id
}

data "azurerm_client_config" "current" {}

# Digital Twins
resource "azurerm_digital_twins_instance" "a2d" {
  name                = "a2d-twins"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
}

# IoT Hub Device Provisioning
resource "azurerm_iothub_dps" "a2d" {
  name                = "a2d-dps"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku {
    name     = "S1"
    capacity = 1
  }
}
