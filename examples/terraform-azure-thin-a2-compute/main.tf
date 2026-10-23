# Thin compute and workplace services of pogi's generic Azure engine (group a2-compute): Azure Arc, Azure Local,
# Azure VMware Solution, Azure Red Hat OpenShift, Kubernetes Fleet Manager, Service Fabric, Azure Virtual Desktop,
# DevTest Labs, Dev Box, Azure Load Testing, Managed DevOps Pools and Center for SAP solutions.
# They are stored and read back; nothing runs behind them. The azurerm provider against pogi:
# `pogi tf --cloud azure > pogi_override.tf`, then run Terraform in a container that trusts pogi's CA.
terraform {
  required_providers {
    azurerm = { source = "hashicorp/azurerm" }
  }
}

data "azurerm_client_config" "current" {}

resource "azurerm_resource_group" "rg" {
  name     = "pogi-tf-a2-compute"
  location = "eastus"
  tags     = { env = "dev" }
}
