# Networking services of pogi's generic Azure engine: Front Door, Application Gateway, Load Balancer,
# Firewall, Bastion, Traffic Manager, Private Link and Virtual WAN. They are stored and read back; no traffic
# flows through any of them.
resource "azurerm_virtual_network" "athin" {
  name                = "athin-vnet"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  address_space       = ["10.20.0.0/16"]
}

resource "azurerm_subnet" "athin_gw" {
  name                 = "athin-gw"
  resource_group_name  = azurerm_resource_group.rg.name
  virtual_network_name = azurerm_virtual_network.athin.name
  address_prefixes     = ["10.20.1.0/24"]
}

resource "azurerm_subnet" "athin_fw" {
  name                 = "AzureFirewallSubnet"
  resource_group_name  = azurerm_resource_group.rg.name
  virtual_network_name = azurerm_virtual_network.athin.name
  address_prefixes     = ["10.20.2.0/24"]
}

resource "azurerm_subnet" "athin_bastion" {
  name                 = "AzureBastionSubnet"
  resource_group_name  = azurerm_resource_group.rg.name
  virtual_network_name = azurerm_virtual_network.athin.name
  address_prefixes     = ["10.20.3.0/24"]
}

resource "azurerm_subnet" "athin_pe" {
  name                 = "athin-pe"
  resource_group_name  = azurerm_resource_group.rg.name
  virtual_network_name = azurerm_virtual_network.athin.name
  address_prefixes     = ["10.20.4.0/24"]
}

resource "azurerm_public_ip" "athin" {
  for_each            = toset(["gw", "fw", "bastion", "lb"])
  name                = "athin-pip-${each.key}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  allocation_method   = "Static"
  sku                 = "Standard"
}

# Front Door
resource "azurerm_cdn_frontdoor_profile" "athin" {
  name                = "athin-fd"
  resource_group_name = azurerm_resource_group.rg.name
  sku_name            = "Standard_AzureFrontDoor"
}

resource "azurerm_cdn_frontdoor_endpoint" "athin" {
  name                     = "athin-ep"
  cdn_frontdoor_profile_id = azurerm_cdn_frontdoor_profile.athin.id
}

resource "azurerm_cdn_frontdoor_origin_group" "athin" {
  name                     = "athin-og"
  cdn_frontdoor_profile_id = azurerm_cdn_frontdoor_profile.athin.id
  load_balancing {}
  health_probe {
    path                = "/"
    protocol            = "Https"
    interval_in_seconds = 100
    request_type        = "HEAD"
  }
}

resource "azurerm_cdn_frontdoor_origin" "athin" {
  name                           = "athin-origin"
  cdn_frontdoor_origin_group_id  = azurerm_cdn_frontdoor_origin_group.athin.id
  enabled                        = true
  certificate_name_check_enabled = false
  host_name                      = "origin.example.test"
  origin_host_header             = "origin.example.test"
}

resource "azurerm_cdn_frontdoor_route" "athin" {
  name                          = "athin-route"
  cdn_frontdoor_endpoint_id     = azurerm_cdn_frontdoor_endpoint.athin.id
  cdn_frontdoor_origin_group_id = azurerm_cdn_frontdoor_origin_group.athin.id
  cdn_frontdoor_origin_ids      = [azurerm_cdn_frontdoor_origin.athin.id]
  patterns_to_match             = ["/*"]
  supported_protocols           = ["Http", "Https"]
  forwarding_protocol           = "HttpsOnly"
}

# Load Balancer and Private Link
resource "azurerm_lb" "athin" {
  name                = "athin-lb"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "Standard"
  frontend_ip_configuration {
    name                 = "front"
    public_ip_address_id = azurerm_public_ip.athin["lb"].id
  }
}

resource "azurerm_lb_backend_address_pool" "athin" {
  name            = "athin-pool"
  loadbalancer_id = azurerm_lb.athin.id
}

resource "azurerm_lb_probe" "athin" {
  name            = "athin-probe"
  loadbalancer_id = azurerm_lb.athin.id
  port            = 80
}

resource "azurerm_lb_rule" "athin" {
  name                           = "athin-rule"
  loadbalancer_id                = azurerm_lb.athin.id
  protocol                       = "Tcp"
  frontend_port                  = 80
  backend_port                   = 80
  frontend_ip_configuration_name = "front"
  backend_address_pool_ids       = [azurerm_lb_backend_address_pool.athin.id]
  probe_id                       = azurerm_lb_probe.athin.id
}

resource "azurerm_lb" "athin_internal" {
  name                = "athin-lb-internal"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "Standard"
  frontend_ip_configuration {
    name      = "front"
    subnet_id = azurerm_subnet.athin_gw.id
  }
}

resource "azurerm_private_link_service" "athin" {
  name                = "athin-pls"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  nat_ip_configuration {
    name      = "primary"
    primary   = true
    subnet_id = azurerm_subnet.athin_gw.id
  }
  load_balancer_frontend_ip_configuration_ids = [azurerm_lb.athin_internal.frontend_ip_configuration[0].id]
}

resource "azurerm_private_endpoint" "athin" {
  name                = "athin-pe"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  subnet_id           = azurerm_subnet.athin_pe.id
  private_service_connection {
    name                           = "athin-conn"
    private_connection_resource_id = azurerm_private_link_service.athin.id
    is_manual_connection           = false
  }
}

# Application Gateway
resource "azurerm_application_gateway" "athin" {
  name                = "athin-agw"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku {
    name     = "Standard_v2"
    tier     = "Standard_v2"
    capacity = 1
  }
  gateway_ip_configuration {
    name      = "gwip"
    subnet_id = azurerm_subnet.athin_gw.id
  }
  frontend_port {
    name = "http"
    port = 80
  }
  frontend_ip_configuration {
    name                 = "front"
    public_ip_address_id = azurerm_public_ip.athin["gw"].id
  }
  backend_address_pool {
    name = "pool"
  }
  backend_http_settings {
    name                  = "settings"
    cookie_based_affinity = "Disabled"
    port                  = 80
    protocol              = "Http"
    request_timeout       = 30
  }
  http_listener {
    name                           = "listener"
    frontend_ip_configuration_name = "front"
    frontend_port_name             = "http"
    protocol                       = "Http"
  }
  request_routing_rule {
    name                       = "rule"
    rule_type                  = "Basic"
    priority                   = 100
    http_listener_name         = "listener"
    backend_address_pool_name  = "pool"
    backend_http_settings_name = "settings"
  }
}

# Firewall
resource "azurerm_firewall_policy" "athin" {
  name                = "athin-fwp"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
}

resource "azurerm_firewall_policy_rule_collection_group" "athin" {
  name               = "athin-rcg"
  firewall_policy_id = azurerm_firewall_policy.athin.id
  priority           = 500
  network_rule_collection {
    name     = "allow-dns"
    priority = 400
    action   = "Allow"
    rule {
      name                  = "dns"
      protocols             = ["UDP"]
      source_addresses      = ["10.20.0.0/16"]
      destination_addresses = ["8.8.8.8"]
      destination_ports     = ["53"]
    }
  }
}

resource "azurerm_firewall" "athin" {
  name                = "athin-fw"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku_name            = "AZFW_VNet"
  sku_tier            = "Standard"
  firewall_policy_id  = azurerm_firewall_policy.athin.id
  ip_configuration {
    name                 = "config"
    subnet_id            = azurerm_subnet.athin_fw.id
    public_ip_address_id = azurerm_public_ip.athin["fw"].id
  }
}

# Bastion
resource "azurerm_bastion_host" "athin" {
  name                = "athin-bastion"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  ip_configuration {
    name                 = "config"
    subnet_id            = azurerm_subnet.athin_bastion.id
    public_ip_address_id = azurerm_public_ip.athin["bastion"].id
  }
}

# Traffic Manager
resource "azurerm_traffic_manager_profile" "athin" {
  name                   = "athin-tm"
  resource_group_name    = azurerm_resource_group.rg.name
  traffic_routing_method = "Weighted"
  dns_config {
    relative_name = "athin-tm-pogi"
    ttl           = 60
  }
  monitor_config {
    protocol = "HTTP"
    port     = 80
    path     = "/"
  }
}

resource "azurerm_traffic_manager_external_endpoint" "athin" {
  name       = "athin-ext"
  profile_id = azurerm_traffic_manager_profile.athin.id
  target     = "www.example.test"
  weight     = 100
}

# Virtual WAN
resource "azurerm_virtual_wan" "athin" {
  name                = "athin-wan"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
}

resource "azurerm_virtual_hub" "athin" {
  name                = "athin-hub"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  virtual_wan_id      = azurerm_virtual_wan.athin.id
  address_prefix      = "10.30.0.0/24"
}
