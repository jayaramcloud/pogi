# A Linux App Service plan, a function app, a Service Bus namespace and a Cosmos DB account.
provider "azurerm" {
  features {}
}

variable "location" {
  default = "eastus2"
}

resource "azurerm_service_plan" "web" {
  name         = "web-plan"
  location     = var.location
  os_type      = "Linux"
  sku_name     = "P1v3"
  worker_count = 2
}

resource "azurerm_linux_web_app" "site" {
  name     = "site"
  location = var.location
}

resource "azurerm_linux_function_app" "jobs" {
  name     = "jobs"
  location = var.location
}

resource "azurerm_servicebus_namespace" "bus" {
  name     = "bus"
  location = var.location
  sku      = "Premium"
  capacity = 1
}

resource "azurerm_mssql_database" "small" {
  name     = "small"
  sku_name = "S1"
}
