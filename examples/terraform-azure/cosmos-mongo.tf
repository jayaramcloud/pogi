# Cosmos DB for MongoDB: an account of kind MongoDB runs a real MongoDB container (needs Docker),
# and the database and collection are made in it too. Connect a Mongo driver with the output.
resource "azurerm_cosmosdb_account" "cosmosmongo" {
  name                = "pogi-tf-mongo"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  offer_type          = "Standard"
  kind                = "MongoDB"

  mongo_server_version = "4.2"
  capabilities {
    name = "EnableMongo"
  }
  consistency_policy {
    consistency_level = "Session"
  }
  geo_location {
    location          = azurerm_resource_group.rg.location
    failover_priority = 0
  }
}

resource "azurerm_cosmosdb_mongo_database" "cosmosmongo" {
  name                = "shop"
  resource_group_name = azurerm_resource_group.rg.name
  account_name        = azurerm_cosmosdb_account.cosmosmongo.name
  throughput          = 400
}

resource "azurerm_cosmosdb_mongo_collection" "cosmosmongo" {
  name                = "users"
  resource_group_name = azurerm_resource_group.rg.name
  account_name        = azurerm_cosmosdb_account.cosmosmongo.name
  database_name       = azurerm_cosmosdb_mongo_database.cosmosmongo.name
  shard_key           = "city"

  index {
    keys   = ["_id"]
    unique = true
  }
  index {
    keys   = ["email"]
    unique = true
  }
}

output "cosmosmongo_connection_string" {
  value     = azurerm_cosmosdb_account.cosmosmongo.primary_mongodb_connection_string
  sensitive = true
}
