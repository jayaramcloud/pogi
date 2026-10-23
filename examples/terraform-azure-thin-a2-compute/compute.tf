# Azure Arc
resource "azurerm_arc_private_link_scope" "c" {
  name                          = "azarc-pogi"
  resource_group_name           = azurerm_resource_group.rg.name
  location                      = azurerm_resource_group.rg.location
  public_network_access_enabled = false
}

# Azure Local
resource "azurerm_stack_hci_cluster" "c" {
  name                = "azlocal-pogi"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  client_id           = "11111111-2222-3333-4444-555555555555"
  tenant_id           = data.azurerm_client_config.current.tenant_id
}

# Azure VMware Solution
resource "azurerm_vmware_private_cloud" "c" {
  name                = "azavs-pogi"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku_name            = "av36p"
  network_subnet_cidr = "192.168.48.0/22"
  management_cluster {
    size = 3
  }
}

resource "azurerm_vmware_cluster" "c" {
  name               = "extra"
  vmware_cloud_id    = azurerm_vmware_private_cloud.c.id
  cluster_node_count = 3
  sku_name           = "av36p"
}

resource "azurerm_vmware_express_route_authorization" "c" {
  name             = "azavs-auth"
  private_cloud_id = azurerm_vmware_private_cloud.c.id
}

# Azure Red Hat OpenShift
resource "azurerm_virtual_network" "aro" {
  name                = "azopenshift-vnet"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  address_space       = ["10.0.0.0/22"]
}

resource "azurerm_subnet" "aro_main" {
  name                 = "main"
  resource_group_name  = azurerm_resource_group.rg.name
  virtual_network_name = azurerm_virtual_network.aro.name
  address_prefixes     = ["10.0.0.0/23"]
}

resource "azurerm_subnet" "aro_worker" {
  name                 = "worker"
  resource_group_name  = azurerm_resource_group.rg.name
  virtual_network_name = azurerm_virtual_network.aro.name
  address_prefixes     = ["10.0.2.0/23"]
}

resource "azurerm_redhat_openshift_cluster" "c" {
  name                = "azopenshift-pogi"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  cluster_profile {
    domain  = "pogi.example"
    version = "4.14.16"
  }
  network_profile {
    pod_cidr     = "10.128.0.0/14"
    service_cidr = "172.30.0.0/16"
  }
  main_profile {
    vm_size   = "Standard_D8s_v3"
    subnet_id = azurerm_subnet.aro_main.id
  }
  api_server_profile {
    visibility = "Public"
  }
  ingress_profile {
    visibility = "Public"
  }
  worker_profile {
    vm_size      = "Standard_D4s_v3"
    disk_size_gb = 128
    node_count   = 3
    subnet_id    = azurerm_subnet.aro_worker.id
  }
  service_principal {
    client_id     = "11111111-2222-3333-4444-555555555555"
    client_secret = "pogi-not-a-secret"
  }
}

# Kubernetes Fleet Manager
resource "azurerm_kubernetes_fleet_manager" "c" {
  name                = "azfleet-pogi"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
}

resource "azurerm_kubernetes_fleet_update_strategy" "c" {
  name                        = "azfleet-strategy"
  kubernetes_fleet_manager_id = azurerm_kubernetes_fleet_manager.c.id
  stage {
    name = "stage-1"
    group {
      name = "group-1"
    }
  }
}

# Service Fabric
resource "azurerm_service_fabric_cluster" "c" {
  name                 = "azservicefabric-pogi"
  resource_group_name  = azurerm_resource_group.rg.name
  location             = azurerm_resource_group.rg.location
  reliability_level    = "Bronze"
  upgrade_mode         = "Manual"
  cluster_code_version = "9.1.1436.9590"
  vm_image             = "Windows"
  management_endpoint  = "https://example:80"
  add_on_features      = ["DnsService"]
  node_type {
    name                 = "first"
    instance_count       = 3
    is_primary           = true
    client_endpoint_port = 2020
    http_endpoint_port   = 80
  }
}

# Azure Virtual Desktop
resource "azurerm_virtual_desktop_host_pool" "c" {
  name                     = "azavd-pool"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  type                     = "Pooled"
  load_balancer_type       = "DepthFirst"
  maximum_sessions_allowed = 16
  preferred_app_group_type = "Desktop"
}

resource "azurerm_virtual_desktop_application_group" "c" {
  name                = "azavd-apps"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  type                = "RemoteApp"
  host_pool_id        = azurerm_virtual_desktop_host_pool.c.id
}

resource "azurerm_virtual_desktop_application" "c" {
  name                         = "notepad"
  application_group_id         = azurerm_virtual_desktop_application_group.c.id
  path                         = "C:\\Windows\\notepad.exe"
  command_line_argument_policy = "DoNotAllow"
}

resource "azurerm_virtual_desktop_workspace" "c" {
  name                = "azavd-workspace"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
}

resource "azurerm_virtual_desktop_workspace_application_group_association" "c" {
  workspace_id         = azurerm_virtual_desktop_workspace.c.id
  application_group_id = azurerm_virtual_desktop_application_group.c.id
}

# DevTest Labs
resource "azurerm_dev_test_lab" "c" {
  name                = "azdevtestlabs-pogi"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  tags                = { env = "dev" }
}

resource "azurerm_dev_test_virtual_network" "c" {
  name                = "azdevtestlabs-vnet"
  lab_name            = azurerm_dev_test_lab.c.name
  resource_group_name = azurerm_resource_group.rg.name
  subnet {
    use_public_ip_address           = "Allow"
    use_in_virtual_machine_creation = "Allow"
  }
}

resource "azurerm_dev_test_schedule" "c" {
  name                = "LabVmsShutdown"
  lab_name            = azurerm_dev_test_lab.c.name
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  task_type           = "LabVmsShutdownTask"
  daily_recurrence {
    time = "1900"
  }
  time_zone_id = "Pacific Standard Time"
  notification_settings {}
}

# Dev Box
resource "azurerm_dev_center" "c" {
  name                = "azdevbox-pogi"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
}

resource "azurerm_dev_center_project" "c" {
  name                = "azdevbox-project"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  dev_center_id       = azurerm_dev_center.c.id
}

resource "azurerm_dev_center_dev_box_definition" "c" {
  name               = "azdevbox-definition"
  location           = azurerm_resource_group.rg.location
  dev_center_id      = azurerm_dev_center.c.id
  image_reference_id = "${azurerm_dev_center.c.id}/galleries/default/images/microsoftwindowsdesktop_windows-ent-cpc_win11-22h2-ent-cpc-m365"
  sku_name           = "general_i_8c32gb256ssd_v2"
}

resource "azurerm_virtual_network" "devbox" {
  name                = "azdevbox-vnet"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  address_space       = ["10.1.0.0/16"]
}

resource "azurerm_subnet" "devbox" {
  name                 = "devbox"
  resource_group_name  = azurerm_resource_group.rg.name
  virtual_network_name = azurerm_virtual_network.devbox.name
  address_prefixes     = ["10.1.0.0/24"]
}

resource "azurerm_dev_center_network_connection" "c" {
  name                = "azdevbox-network"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  domain_join_type    = "AzureADJoin"
  subnet_id           = azurerm_subnet.devbox.id
}

resource "azurerm_dev_center_attached_network" "c" {
  name                  = "azdevbox-attached"
  dev_center_id         = azurerm_dev_center.c.id
  network_connection_id = azurerm_dev_center_network_connection.c.id
}

resource "azurerm_dev_center_project_pool" "c" {
  name                                    = "azdevbox-pool"
  location                                = azurerm_resource_group.rg.location
  dev_center_project_id                   = azurerm_dev_center_project.c.id
  dev_box_definition_name                 = azurerm_dev_center_dev_box_definition.c.name
  dev_center_attached_network_name        = azurerm_dev_center_attached_network.c.name
  local_administrator_enabled             = true
  stop_on_disconnect_grace_period_minutes = 60
}

# Azure Load Testing
resource "azurerm_load_test" "c" {
  name                = "azloadtesting-pogi"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  description         = "pogi example"
}

# Managed DevOps Pools
resource "azurerm_managed_devops_pool" "c" {
  name                  = "azdevopspools-pogi"
  resource_group_name   = azurerm_resource_group.rg.name
  location              = azurerm_resource_group.rg.location
  dev_center_project_id = azurerm_dev_center_project.c.id
  maximum_concurrency   = 1
  virtual_machine_scale_set_fabric {
    sku_name = "Standard_D2ads_v5"
    image {
      well_known_image_name = "ubuntu-22.04/latest"
    }
  }
  stateless_agent {
  }
  azure_devops_organization {
    organization {
      url         = "https://dev.azure.com/pogi-example"
      parallelism = 1
    }
  }
}

# Center for SAP solutions
resource "azurerm_user_assigned_identity" "sap" {
  name                = "azsapworkloads-id"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
}

resource "azurerm_workloads_sap_discovery_virtual_instance" "c" {
  name                              = "PG1"
  resource_group_name               = azurerm_resource_group.rg.name
  location                          = azurerm_resource_group.rg.location
  environment                       = "NonProd"
  sap_product                       = "S4HANA"
  central_server_virtual_machine_id = "${azurerm_resource_group.rg.id}/providers/Microsoft.Compute/virtualMachines/sap-central"
  managed_resource_group_name       = "pogi-tf-a2-compute-sap"
  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.sap.id]
  }
}
