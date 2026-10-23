provider "aws" { region = "us-east-1" }
provider "aws" {
  alias  = "west"
  region = "us-west-2"
}
provider "google" {
  region = "europe-west1"
  zone   = "europe-west1-b"
}
resource "aws_instance" "default" {}
resource "aws_instance" "west" { provider = aws.west }
resource "aws_instance" "own" { region = "eu-central-1" }
module "m" {
  source    = "./mod"
  providers = { aws = aws.west }
}
module "inherit" { source = "./mod" }
resource "google_compute_instance" "z" { zone = "us-central1-a" }
resource "google_compute_instance" "p" {}
resource "google_container_cluster" "r" { location = "asia-east1" }
resource "google_container_cluster" "zonal" { location = "asia-east1-b" }
resource "google_storage_bucket" "multi" { location = "US" }
resource "azurerm_resource_group" "rg" { location = "West Europe" }
resource "azurerm_mssql_server" "s" {
  location            = azurerm_resource_group.rg.location
  resource_group_name = "rg"
}
resource "azurerm_mssql_database" "db" { server_id = azurerm_mssql_server.s.id }
resource "azurerm_mssql_database_extended_auditing_policy" "a" { database_id = azurerm_mssql_database.db.id }
