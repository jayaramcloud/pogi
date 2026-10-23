# Thin data services on the resource-manager engine: resources are stored and read back, nothing runs behind them.

# Data Factory
resource "azurerm_data_factory" "adf" {
  name                = "pogi-tf-adf"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
}

resource "azurerm_data_factory_linked_service_web" "adf" {
  name                = "adfweb"
  data_factory_id     = azurerm_data_factory.adf.id
  authentication_type = "Anonymous"
  url                 = "https://example.com"
}

resource "azurerm_data_factory_dataset_http" "adf" {
  name                = "adfhttp"
  data_factory_id     = azurerm_data_factory.adf.id
  linked_service_name = azurerm_data_factory_linked_service_web.adf.name
  relative_url        = "data.csv"
  request_method      = "GET"
}

resource "azurerm_data_factory_pipeline" "adf" {
  name            = "adfpipeline"
  data_factory_id = azurerm_data_factory.adf.id
}

# Batch
resource "azurerm_batch_account" "batch" {
  name                 = "pogitfbatch"
  resource_group_name  = azurerm_resource_group.rg.name
  location             = azurerm_resource_group.rg.location
  pool_allocation_mode = "BatchService"
}

resource "azurerm_batch_pool" "batch" {
  name                = "pogipool"
  resource_group_name = azurerm_resource_group.rg.name
  account_name        = azurerm_batch_account.batch.name
  node_agent_sku_id   = "batch.node.ubuntu 22.04"
  vm_size             = "Standard_A1"
  fixed_scale {
    target_dedicated_nodes = 0
  }
  storage_image_reference {
    publisher = "Canonical"
    offer     = "0001-com-ubuntu-server-jammy"
    sku       = "22_04-lts"
    version   = "latest"
  }
}

# Managed Grafana
resource "azurerm_dashboard_grafana" "grafana" {
  name                  = "pogi-tf-grafana"
  resource_group_name   = azurerm_resource_group.rg.name
  location              = azurerm_resource_group.rg.location
  grafana_major_version = 12
  identity {
    type = "SystemAssigned"
  }
}

# Cognitive Services
resource "azurerm_cognitive_account" "cog" {
  name                = "pogi-tf-cognitive"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  kind                = "TextAnalytics"
  sku_name            = "S"
}

# Databricks
resource "azurerm_databricks_workspace" "dbx" {
  name                = "pogi-tf-dbx"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "standard"
}

# AI Search
resource "azurerm_search_service" "search" {
  name                = "pogi-tf-search"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "standard"
}

# Synapse (its default file system is the Data Lake one of files.tf)
resource "azurerm_synapse_workspace" "syn" {
  name                                 = "pogi-tf-syn"
  resource_group_name                  = azurerm_resource_group.rg.name
  location                             = azurerm_resource_group.rg.location
  storage_data_lake_gen2_filesystem_id = azurerm_storage_data_lake_gen2_filesystem.files_lake.id
  sql_administrator_login              = "sqladminuser"
  sql_administrator_login_password     = "H@Sh1CoR3!"
}

resource "azurerm_synapse_sql_pool" "syn" {
  name                 = "synpool"
  synapse_workspace_id = azurerm_synapse_workspace.syn.id
  sku_name             = "DW100c"
  create_mode          = "Default"
  storage_account_type = "GRS"
}

resource "azurerm_synapse_firewall_rule" "syn" {
  name                 = "allowall"
  synapse_workspace_id = azurerm_synapse_workspace.syn.id
  start_ip_address     = "0.0.0.0"
  end_ip_address       = "255.255.255.255"
}

# Data Explorer
resource "azurerm_kusto_cluster" "kusto" {
  name                = "pogitfkusto"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku {
    name     = "Dev(No SLA)_Standard_D11_v2"
    capacity = 1
  }
}

resource "azurerm_kusto_database" "kusto" {
  name                = "pogidb"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  cluster_name        = azurerm_kusto_cluster.kusto.name
}

# Stream Analytics
resource "azurerm_stream_analytics_job" "asa" {
  name                                     = "pogi-tf-asa"
  resource_group_name                      = azurerm_resource_group.rg.name
  location                                 = azurerm_resource_group.rg.location
  compatibility_level                      = "1.2"
  data_locale                              = "en-GB"
  events_late_arrival_max_delay_in_seconds = 60
  events_out_of_order_max_delay_in_seconds = 50
  events_out_of_order_policy               = "Adjust"
  output_error_policy                      = "Drop"
  streaming_units                          = 3
  transformation_query                     = "SELECT * INTO [out] FROM [in]"
}
