# Apigee
resource "google_apigee_organization" "apigee" {
  project_id       = local.project
  analytics_region = local.region
  runtime_type     = "HYBRID"
}

resource "google_apigee_environment" "apigee" {
  org_id = google_apigee_organization.apigee.id
  name   = "apigee-env"
}

resource "google_apigee_envgroup" "apigee" {
  org_id    = google_apigee_organization.apigee.id
  name      = "apigee-group"
  hostnames = ["apigee.example.com"]
}

resource "google_apigee_developer" "apigee" {
  org_id     = google_apigee_organization.apigee.id
  email      = "dev@example.com"
  first_name = "Ada"
  last_name  = "Lovelace"
  user_name  = "ada"
}

resource "google_apigee_api_product" "apigee" {
  org_id        = google_apigee_organization.apigee.id
  name          = "apigee-product"
  display_name  = "Apigee product"
  approval_type = "auto"
  environments  = []
}

# API hub
resource "google_apihub_api_hub_instance" "apihub" {
  location            = local.region
  api_hub_instance_id = "apihub-main"
  config {
    cmek_key_name = "projects/${local.project}/locations/${local.region}/keyRings/kr/cryptoKeys/k"
  }
}

# App Hub
resource "google_apphub_application" "apphub" {
  location       = local.region
  application_id = "apphub-app"
  scope {
    type = "REGIONAL"
  }
}

# Application Integration
resource "google_integrations_auth_config" "integrations" {
  location     = local.region
  display_name = "integrations-auth"
  decrypted_credential {
    credential_type = "USERNAME_AND_PASSWORD"
    username_and_password {
      username = "u"
      password = "p"
    }
  }
}

# Integration Connectors
resource "google_integration_connectors_managed_zone" "connectors" {
  name           = "connectors-zone"
  description    = "a zone"
  target_project = local.project
  target_vpc     = "default"
  dns            = "example.com."
}

# Developer Connect
resource "google_developer_connect_connection" "devconnect" {
  location      = local.region
  connection_id = "devconnect-main"
  github_config {
    github_app = "FIREBASE"
  }
}

# Secure Source Manager
resource "google_secure_source_manager_instance" "ssm" {
  location        = local.region
  instance_id     = "ssm-main"
  deletion_policy = "DELETE"
}

resource "google_secure_source_manager_repository" "ssm" {
  location        = local.region
  repository_id   = "ssm-repo"
  deletion_policy = "DELETE"
  instance        = google_secure_source_manager_instance.ssm.name
}

# Parameter Manager
resource "google_parameter_manager_parameter" "param" {
  parameter_id = "param-main"
  format       = "JSON"
}

resource "google_parameter_manager_parameter_version" "param" {
  parameter            = google_parameter_manager_parameter.param.id
  parameter_version_id = "v1"
  parameter_data       = jsonencode({ key = "value" })
}

# Cloud Billing budgets
resource "google_billing_budget" "budget" {
  billing_account = "000000-000000-000000"
  display_name    = "budget-main"
  amount {
    specified_amount {
      currency_code = "USD"
      units         = "100"
    }
  }
}

# Migration Center
resource "google_migration_center_group" "migration" {
  location    = local.region
  group_id    = "migration-group"
  description = "a group"
}

# Blockchain Node Engine
resource "google_blockchain_node_engine_blockchain_nodes" "chain" {
  location           = local.region
  blockchain_node_id = "chain-node"
  blockchain_type    = "ETHEREUM"
  ethereum_details {
    network   = "MAINNET"
    node_type = "FULL"
  }
}

# Firebase App Check
resource "google_firebase_app_check_debug_token" "appcheck" {
  app_id       = "1:123456789:web:abc123"
  display_name = "appcheck-debug"
  token        = "11111111-2222-3333-4444-555555555555"
}

# Firebase App Hosting
resource "google_firebase_app_hosting_backend" "hosting" {
  location         = local.region
  backend_id       = "hosting-main"
  app_id           = "1:123456789:web:abc123"
  serving_locality = "GLOBAL_ACCESS"
  service_account  = "pogi-app@${local.project}.iam.gserviceaccount.com"
}

# Firebase Data Connect
resource "google_firebase_data_connect_service" "dataconnect" {
  location   = local.region
  service_id = "dataconnect-main"
}

# Cloud Endpoints
resource "google_endpoints_service" "endpoints" {
  service_name   = "endpoints-main.endpoints.${local.project}.cloud.goog"
  project        = local.project
  openapi_config = <<-EOT
    swagger: "2.0"
    info:
      title: endpoints-main
      version: "1.0.0"
    host: endpoints-main.endpoints.${local.project}.cloud.goog
    schemes: ["https"]
    produces: ["application/json"]
    paths:
      /ping:
        get:
          operationId: ping
          responses:
            "200":
              description: pong
  EOT
}

# License Manager
resource "google_license_manager_configuration" "license" {
  location         = local.region
  configuration_id = "license-main"
  product          = "windows-server"
  license_count    = 5
  active           = true
}

resource "google_integration_connectors_connection" "connectors" {
  name              = "connectors-conn"
  location          = local.region
  connector_version = "projects/${local.project}/locations/global/providers/gcp/connectors/pubsub/versions/1"
  config_variable {
    key          = "project_id"
    string_value = local.project
  }
}
