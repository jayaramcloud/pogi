# Services of pogi's generic Azure engine: DNS, Policy and Logic Apps. Nothing runs behind a zone or a workflow:
# a zone answers no queries and a workflow never fires. The policy is evaluated: it only audits, so it denies nothing.
resource "azurerm_dns_zone" "generic" {
  name                = "generic.example.test"
  resource_group_name = azurerm_resource_group.rg.name
  tags                = { env = "dev" }
}

resource "azurerm_dns_a_record" "generic_www" {
  name                = "www"
  zone_name           = azurerm_dns_zone.generic.name
  resource_group_name = azurerm_resource_group.rg.name
  ttl                 = 300
  records             = ["10.0.0.4", "10.0.0.5"]
}

resource "azurerm_dns_cname_record" "generic_app" {
  name                = "app"
  zone_name           = azurerm_dns_zone.generic.name
  resource_group_name = azurerm_resource_group.rg.name
  ttl                 = 300
  record              = "www.generic.example.test"
}

resource "azurerm_policy_definition" "generic_audit" {
  name         = "pogi-tf-audit-location"
  policy_type  = "Custom"
  mode         = "All"
  display_name = "Audit resources outside East US"

  policy_rule = jsonencode({
    if   = { field = "location", notEquals = "eastus" }
    then = { effect = "audit" }
  })
}

resource "azurerm_resource_group_policy_assignment" "generic_audit" {
  name                 = "pogi-tf-audit"
  resource_group_id    = azurerm_resource_group.rg.id
  policy_definition_id = azurerm_policy_definition.generic_audit.id
}

resource "azurerm_logic_app_workflow" "generic" {
  name                = "pogi-tf-workflow"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name
  tags                = { env = "dev" }
}
