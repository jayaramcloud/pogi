# A Windows VM with a data disk, an Azure SQL database and a storage account.
provider "azurerm" {
  features {}
}

variable "location" {
  default = "uksouth"
}

resource "azurerm_windows_virtual_machine" "app" {
  name           = "app"
  location       = var.location
  size           = "Standard_D4s_v5"
  admin_username = "azureuser"

  os_disk {
    storage_account_type = "StandardSSD_LRS"
    disk_size_gb         = 128
  }
}

resource "azurerm_managed_disk" "data" {
  name                 = "app-data"
  location             = var.location
  storage_account_type = "Premium_LRS"
  create_option        = "Empty"
  disk_size_gb         = 512
}

resource "azurerm_mssql_server" "main" {
  name                         = "app-sql"
  resource_group_name          = "app-rg"
  location                     = var.location
  version                      = "12.0"
  administrator_login          = "sqladmin"
  administrator_login_password = "change-me-Example1"
}

resource "azurerm_mssql_database" "main" {
  name        = "appdb"
  server_id   = azurerm_mssql_server.main.id
  sku_name    = "GP_Gen5_2"
  max_size_gb = 64
}

resource "azurerm_storage_account" "logs" {
  name                     = "applogs"
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "GRS"
}
