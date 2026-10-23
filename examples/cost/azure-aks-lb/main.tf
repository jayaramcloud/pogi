# An AKS cluster with a second node pool, a NAT gateway and a Standard load balancer.
provider "azurerm" {
  features {}
}

variable "location" {
  default = "westeurope"
}

resource "azurerm_kubernetes_cluster" "main" {
  name     = "aks-lb"
  location = var.location
  sku_tier = "Standard"

  default_node_pool {
    name            = "system"
    vm_size         = "Standard_D4s_v5"
    node_count      = 3
    os_disk_size_gb = 128
  }
}

resource "azurerm_kubernetes_cluster_node_pool" "work" {
  name                  = "work"
  kubernetes_cluster_id = azurerm_kubernetes_cluster.main.id
  vm_size               = "Standard_D8s_v5"
  node_count            = 2
  os_disk_size_gb       = 128
}

resource "azurerm_nat_gateway" "main" {
  name     = "aks-nat"
  location = var.location
}

resource "azurerm_public_ip" "nat" {
  name              = "aks-nat-ip"
  location          = var.location
  allocation_method = "Static"
}

resource "azurerm_lb" "main" {
  name     = "aks-lb"
  location = var.location
  sku      = "Standard"
}
