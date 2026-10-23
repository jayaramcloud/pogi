# Security, identity and management services on the resource-manager engine: resources are stored and read
# back, nothing runs behind them. `pogi tf --cloud azure > pogi_override.tf`, then run Terraform in a container.
terraform {
  required_providers {
    azurerm = { source = "hashicorp/azurerm" }
  }
}

data "azurerm_client_config" "current" {}

resource "azurerm_resource_group" "rg" {
  name     = "pogi-tf-a2-sec"
  location = "eastus"
}

# Confidential Ledger
resource "azurerm_confidential_ledger" "cl" {
  name                = "pogitfledger"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  ledger_type         = "Public"
  azuread_based_service_principal {
    ledger_role_name = "Administrator"
    principal_id     = "11111111-2222-3333-4444-555555555555"
    tenant_id        = data.azurerm_client_config.current.tenant_id
  }
}

# Trusted Signing
resource "azurerm_trusted_signing_account" "ts" {
  name                = "pogitfsign"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku_name            = "Basic"
}

# Purview
resource "azurerm_purview_account" "pv" {
  name                = "pogitfpurview"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  identity {
    type = "SystemAssigned"
  }
}

# Lighthouse: a made-up managing tenant, the built-in Reader role
resource "azurerm_lighthouse_definition" "lh" {
  name               = "pogi-tf-lighthouse"
  description        = "Read-only access for a managing tenant"
  managing_tenant_id = "22222222-3333-4444-5555-666666666666"
  scope              = "/subscriptions/${data.azurerm_client_config.current.subscription_id}"
  authorization {
    principal_id           = "33333333-4444-5555-6666-777777777777"
    role_definition_id     = "acdd72a7-3385-48ef-bd42-f606fba81ae7"
    principal_display_name = "Reader group"
  }
}

resource "azurerm_lighthouse_assignment" "lh" {
  scope                    = "/subscriptions/${data.azurerm_client_config.current.subscription_id}"
  lighthouse_definition_id = azurerm_lighthouse_definition.lh.id
}

# Managed Applications
resource "azurerm_managed_application_definition" "app" {
  name                = "pogitfappdef"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  lock_level          = "ReadOnly"
  display_name        = "Pogi app"
  description         = "A managed application definition"
  package_file_uri    = "https://example.com/app.zip"
  authorization {
    service_principal_id = "44444444-5555-6666-7777-888888888888"
    role_definition_id   = "acdd72a7-3385-48ef-bd42-f606fba81ae7"
  }
}

resource "azurerm_managed_application" "app" {
  name                        = "pogitfapp"
  resource_group_name         = azurerm_resource_group.rg.name
  location                    = azurerm_resource_group.rg.location
  kind                        = "ServiceCatalog"
  managed_resource_group_name = "pogi-tf-a2-sec-managed"
  application_definition_id   = azurerm_managed_application_definition.app.id
}

# Defender for Cloud
resource "azurerm_security_center_subscription_pricing" "vm" {
  resource_type = "VirtualMachines"
  tier          = "Standard"
}

resource "azurerm_security_center_contact" "sec" {
  name                = "default"
  email               = "security@example.com"
  phone               = "+1-555-0100"
  alert_notifications = true
  alerts_to_admins    = true
}

resource "azurerm_security_center_setting" "mcas" {
  setting_name = "MCAS"
  enabled      = true
}

# Sentinel
resource "azurerm_log_analytics_workspace" "ws" {
  name                = "pogi-tf-sentinel"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "PerGB2018"
}

resource "azurerm_sentinel_log_analytics_workspace_onboarding" "on" {
  workspace_id = azurerm_log_analytics_workspace.ws.id
}

resource "azurerm_sentinel_alert_rule_scheduled" "rule" {
  name                       = "pogi-tf-rule"
  log_analytics_workspace_id = azurerm_sentinel_log_analytics_workspace_onboarding.on.workspace_id
  display_name               = "Many failed sign-ins"
  severity                   = "Low"
  query                      = "SigninLogs | where ResultType != 0"
}

resource "azurerm_sentinel_watchlist" "wl" {
  name                       = "pogi-tf-watchlist"
  log_analytics_workspace_id = azurerm_sentinel_log_analytics_workspace_onboarding.on.workspace_id
  display_name               = "Known hosts"
  item_search_key            = "host"
}

# Cost Management: a budget and an export to a made-up storage container
resource "azurerm_consumption_budget_subscription" "budget" {
  name            = "pogi-tf-budget"
  subscription_id = "/subscriptions/${data.azurerm_client_config.current.subscription_id}"
  amount          = 100
  time_grain      = "Monthly"
  time_period {
    start_date = "2026-10-01T00:00:00Z"
  }
  notification {
    enabled        = true
    threshold      = 90
    operator       = "GreaterThan"
    contact_emails = ["owner@example.com"]
  }
}

resource "azurerm_subscription_cost_management_export" "export" {
  name                         = "pogi-tf-export"
  subscription_id              = "/subscriptions/${data.azurerm_client_config.current.subscription_id}"
  recurrence_type              = "Monthly"
  recurrence_period_start_date = "2026-11-01T00:00:00Z"
  recurrence_period_end_date   = "2027-11-01T00:00:00Z"
  export_data_storage_location {
    container_id     = "/subscriptions/${data.azurerm_client_config.current.subscription_id}/resourceGroups/${azurerm_resource_group.rg.name}/providers/Microsoft.Storage/storageAccounts/pogiexports/blobServices/default/containers/exports"
    root_folder_path = "/exports"
  }
  export_data_options {
    type       = "Usage"
    time_frame = "TheLastMonth"
  }
}

# Chaos Studio: a target on a made-up virtual machine, and an experiment that uses it
resource "azurerm_chaos_studio_target" "vm" {
  location           = azurerm_resource_group.rg.location
  target_resource_id = "/subscriptions/${data.azurerm_client_config.current.subscription_id}/resourceGroups/${azurerm_resource_group.rg.name}/providers/Microsoft.Compute/virtualMachines/pogivm"
  target_type        = "Microsoft-Agent"
}

resource "azurerm_chaos_studio_capability" "cpu" {
  chaos_studio_target_id = azurerm_chaos_studio_target.vm.id
  capability_type        = "CPUPressure-1.0"
}

resource "azurerm_chaos_studio_experiment" "exp" {
  name                = "pogi-tf-experiment"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  identity {
    type = "SystemAssigned"
  }
  selectors {
    name                    = "Selector1"
    chaos_studio_target_ids = [azurerm_chaos_studio_target.vm.id]
  }
  steps {
    name = "step1"
    branch {
      name = "branch1"
      actions {
        action_type   = "continuous"
        selector_name = "Selector1"
        parameters    = { pCpuUtilization = "95" }
        urn           = "urn:csci:microsoft:agent:cpuPressure/1.0"
        duration      = "PT10M"
      }
    }
  }
  depends_on = [azurerm_chaos_studio_capability.cpu]
}

# Entra Domain Services: needs a subnet
resource "azurerm_virtual_network" "aadds" {
  name                = "pogi-tf-aadds"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  address_space       = ["10.20.0.0/16"]
}

resource "azurerm_subnet" "aadds" {
  name                 = "aadds"
  resource_group_name  = azurerm_resource_group.rg.name
  virtual_network_name = azurerm_virtual_network.aadds.name
  address_prefixes     = ["10.20.0.0/24"]
}

resource "azurerm_active_directory_domain_service" "aadds" {
  name                = "pogi-tf-aadds"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  domain_name         = "pogi.example.com"
  sku                 = "Standard"
  initial_replica_set {
    subnet_id = azurerm_subnet.aadds.id
  }
}

# Advisor: pogi makes no recommendations, so the list is empty
data "azurerm_advisor_recommendations" "all" {}
