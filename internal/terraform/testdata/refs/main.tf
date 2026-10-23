provider "azurerm" {}
variable "loc" { default = "East US" }
locals { rg_name = azurerm_resource_group.rg.name }
resource "azurerm_resource_group" "rg" {
  name     = "rg1"
  location = var.loc
}
resource "azurerm_storage_account" "sa" {
  name                = "sa${local.rg_name}"
  resource_group_name = local.rg_name
  location            = azurerm_resource_group.rg.location
  account_tier        = "Standard"
  tags                = { id = azurerm_resource_group.rg.id }
}
data "aws_vpc" "default" { default = true }
resource "aws_subnet" "s" {
  vpc_id     = data.aws_vpc.default.id
  cidr_block = "10.0.0.0/24"
}
resource "aws_instance" "i" {
  subnet_id  = aws_subnet.s.id
  depends_on = [azurerm_storage_account.sa]
}
