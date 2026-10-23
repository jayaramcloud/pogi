# A web VM with a public IP, a PostgreSQL server and a Redis cache.
provider "azurerm" {
  features {}
}

variable "location" {
  default = "eastus"
}

resource "azurerm_resource_group" "main" {
  name     = "web-db"
  location = var.location
}

resource "azurerm_virtual_network" "main" {
  name                = "web-db-net"
  location            = var.location
  resource_group_name = azurerm_resource_group.main.name
  address_space       = ["10.0.0.0/16"]
}

resource "azurerm_public_ip" "web" {
  name                = "web-ip"
  location            = var.location
  resource_group_name = azurerm_resource_group.main.name
  allocation_method   = "Static"
  sku                 = "Standard"
}

resource "azurerm_linux_virtual_machine" "web" {
  name                = "web"
  location            = var.location
  resource_group_name = azurerm_resource_group.main.name
  size                = "Standard_D2s_v5"
  admin_username      = "azureuser"

  os_disk {
    storage_account_type = "Premium_LRS"
    disk_size_gb         = 64
  }
}

resource "azurerm_postgresql_flexible_server" "db" {
  name                = "web-db-pg"
  location            = var.location
  resource_group_name = azurerm_resource_group.main.name
  sku_name            = "GP_Standard_D2ds_v5"
  storage_mb          = 65536
}

resource "azurerm_redis_cache" "cache" {
  name                = "web-db-cache"
  location            = var.location
  resource_group_name = azurerm_resource_group.main.name
  capacity            = 1
  family              = "C"
  sku_name            = "Standard"
}
