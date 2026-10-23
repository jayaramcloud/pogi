# App Service: a plan, a Linux web app that runs a public image (pogi starts it in Docker and serves it at
# <app>.azurewebsites.localhost), and a Windows web app (metadata only: pogi does not run Windows apps).
resource "azurerm_service_plan" "appservice" {
  name                = "pogi-appservice-plan"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  os_type             = "Linux"
  sku_name            = "B1"
}

resource "azurerm_linux_web_app" "appservice" {
  name                = "pogi-appservice-web"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_service_plan.appservice.location
  service_plan_id     = azurerm_service_plan.appservice.id
  https_only          = true

  app_settings = {
    WEBSITES_PORT = "80"
    GREETING      = "hello"
  }

  site_config {
    always_on = true
    application_stack {
      docker_image_name   = "nginx:alpine"
      docker_registry_url = "https://index.docker.io"
    }
  }
}

resource "azurerm_service_plan" "appservice_win" {
  name                = "pogi-appservice-winplan"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  os_type             = "Windows"
  sku_name            = "B1"
}

resource "azurerm_windows_web_app" "appservice_win" {
  name                = "pogi-appservice-win"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_service_plan.appservice_win.location
  service_plan_id     = azurerm_service_plan.appservice_win.id

  site_config {}
}
