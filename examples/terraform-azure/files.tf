# Azure Files and Data Lake Gen2: a share, a file system on an account with a hierarchical namespace, and a directory.
resource "azurerm_storage_account" "files_lake" {
  name                     = "pogitflake"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true
}

resource "azurerm_storage_share" "files_docs" {
  name               = "docs"
  storage_account_id = azurerm_storage_account.files_lake.id
  quota              = 50
}

resource "azurerm_storage_data_lake_gen2_filesystem" "files_lake" {
  name               = "lake"
  storage_account_id = azurerm_storage_account.files_lake.id
}

resource "azurerm_storage_data_lake_gen2_path" "files_raw" {
  path               = "raw"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.files_lake.name
  storage_account_id = azurerm_storage_account.files_lake.id
  resource           = "directory"
}
