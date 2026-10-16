# The azurerm provider against pogi: `pogi tf --cloud azure > pogi_override.tf`, then run
# Terraform in a container that trusts pogi's CA (the comment in the generated file has the
# docker command). terraform apply, then plan shows no changes, then destroy.
terraform {
  required_providers {
    azurerm = { source = "hashicorp/azurerm" }
  }
}

data "azurerm_client_config" "current" {}

resource "azurerm_resource_group" "rg" {
  name     = "pogi-tf"
  location = "eastus"
  tags     = { env = "dev" }
}

# Storage
resource "azurerm_storage_account" "sa" {
  name                     = "pogitfstore"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

resource "azurerm_storage_container" "photos" {
  name                  = "photos"
  storage_account_id    = azurerm_storage_account.sa.id
  container_access_type = "private"
}

resource "azurerm_storage_queue" "jobs" {
  name               = "jobs"
  storage_account_id = azurerm_storage_account.sa.id
}

# Key Vault
resource "azurerm_key_vault" "kv" {
  name                = "pogi-tf-kv"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = "standard"

  rbac_authorization_enabled = false
  access_policy {
    tenant_id          = data.azurerm_client_config.current.tenant_id
    object_id          = data.azurerm_client_config.current.object_id
    secret_permissions = ["Get", "List", "Set", "Delete", "Purge", "Recover"]
  }
}

resource "azurerm_key_vault_secret" "db" {
  name         = "db-password"
  value        = "hunter2"
  key_vault_id = azurerm_key_vault.kv.id
}

# Cosmos DB
resource "azurerm_cosmosdb_account" "db" {
  name                = "pogi-tf-cosmos"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  offer_type          = "Standard"
  kind                = "GlobalDocumentDB"
  consistency_policy {
    consistency_level = "Session"
  }
  geo_location {
    location          = azurerm_resource_group.rg.location
    failover_priority = 0
  }
}

resource "azurerm_cosmosdb_sql_database" "app" {
  name                = "app"
  resource_group_name = azurerm_resource_group.rg.name
  account_name        = azurerm_cosmosdb_account.db.name
  throughput          = 400
}

resource "azurerm_cosmosdb_sql_container" "items" {
  name                  = "items"
  resource_group_name   = azurerm_resource_group.rg.name
  account_name          = azurerm_cosmosdb_account.db.name
  database_name         = azurerm_cosmosdb_sql_database.app.name
  partition_key_paths   = ["/pk"]
  partition_key_version = 2
}

# Service Bus
resource "azurerm_servicebus_namespace" "sb" {
  name                = "pogi-tf-bus"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "Standard"
}

resource "azurerm_servicebus_queue" "orders" {
  name         = "orders"
  namespace_id = azurerm_servicebus_namespace.sb.id
}

resource "azurerm_servicebus_topic" "events" {
  name         = "events"
  namespace_id = azurerm_servicebus_namespace.sb.id
}

resource "azurerm_servicebus_subscription" "audit" {
  name               = "audit"
  topic_id           = azurerm_servicebus_topic.events.id
  max_delivery_count = 5
}

# Event Hubs
resource "azurerm_eventhub_namespace" "eh" {
  name                = "pogi-tf-hubs"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "Standard"
}

resource "azurerm_eventhub" "telemetry" {
  name              = "telemetry"
  namespace_id      = azurerm_eventhub_namespace.eh.id
  partition_count   = 2
  message_retention = 1
}

# Managed identity, App Configuration, Event Grid
resource "azurerm_user_assigned_identity" "id" {
  name                = "pogi-tf-id"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
}

resource "azurerm_app_configuration" "conf" {
  name                = "pogi-tf-conf"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "standard"
}

resource "azurerm_eventgrid_topic" "orders" {
  name                = "pogi-tf-orders"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
}

output "storage_endpoint" {
  value = azurerm_storage_account.sa.primary_blob_endpoint
}
