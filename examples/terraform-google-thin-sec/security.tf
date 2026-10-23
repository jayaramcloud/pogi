# VPC Service Controls
resource "google_access_context_manager_access_policy" "sec" {
  parent = local.org
  title  = "sec policy"
}

resource "google_access_context_manager_service_perimeter" "sec" {
  parent = "accessPolicies/${google_access_context_manager_access_policy.sec.name}"
  name   = "accessPolicies/${google_access_context_manager_access_policy.sec.name}/servicePerimeters/sec_perimeter"
  title  = "sec_perimeter"
  status {
    restricted_services = ["storage.googleapis.com"]
  }
}

# Managed Microsoft AD
resource "google_active_directory_domain" "sec" {
  domain_name         = "sec.example.com"
  locations           = ["us-central1"]
  reserved_ip_range   = "192.168.255.0/24"
  deletion_protection = false
}

# Assured Workloads
resource "google_assured_workloads_workload" "sec" {
  organization      = "123456789012"
  location          = "us-central1"
  display_name      = "sec-workload"
  compliance_regime = "FEDRAMP_MODERATE"
  billing_account   = "billingAccounts/000000-000000-000000"
}

# Chrome Enterprise Premium
resource "google_beyondcorp_security_gateway" "sec" {
  security_gateway_id = "sec-gateway"
  display_name        = "sec gateway"
}

# Google Security Operations
resource "google_chronicle_reference_list" "sec" {
  location          = "us"
  instance          = "00000000-0000-0000-0000-000000000001"
  reference_list_id = "sec-list"
  description       = "sec reference list"
  syntax_type       = "REFERENCE_LIST_SYNTAX_TYPE_PLAIN_TEXT_STRING"
  entries {
    value = "evil.example.com"
  }
}

# Cloud Identity
resource "google_cloud_identity_group" "sec" {
  display_name         = "sec group"
  initial_group_config = "WITH_INITIAL_OWNER"
  parent               = "customers/C01234567"
  group_key {
    id = "sec-group@example.com"
  }
  labels = {
    "cloudidentity.googleapis.com/groups.discussion_forum" = ""
  }
}

# Cloud IDS
resource "google_cloud_ids_endpoint" "sec" {
  name     = "sec-ids"
  location = "us-central1-a"
  network  = "projects/${local.project}/global/networks/default"
  severity = "INFORMATIONAL"
}

# Compliance Manager
resource "google_cloud_security_compliance_cloud_control" "sec" {
  parent                    = local.org
  location                  = "global"
  cloud_control_id          = "sec-control"
  description               = "sec control"
  categories                = ["CC_CATEGORY_INFRASTRUCTURE"]
  severity                  = "MEDIUM"
  finding_category          = "SEC_FINDING"
  supported_cloud_providers = ["GCP"]
  rules {
    description       = "sec rule"
    rule_action_types = ["RULE_ACTION_TYPE_DETECTIVE"]
    cel_expression {
      expression = "true"
      resource_types_values {
        values = ["compute.googleapis.com/Instance"]
      }
    }
  }
}

# Sensitive Data Protection
resource "google_data_loss_prevention_inspect_template" "sec" {
  parent       = "projects/${local.project}"
  description  = "sec inspect template"
  display_name = "sec template"
  inspect_config {
    info_types {
      name = "EMAIL_ADDRESS"
    }
    min_likelihood = "POSSIBLE"
  }
}

# Identity-Aware Proxy
resource "google_iap_tunnel_dest_group" "sec" {
  region     = "us-central1"
  group_name = "sec-group"
  cidrs      = ["10.1.2.0/24"]
}

# Privileged Access Manager
resource "google_privileged_access_manager_entitlement" "sec" {
  entitlement_id       = "sec-entitlement"
  location             = "global"
  parent               = "projects/${local.project}"
  max_request_duration = "3600s"
  eligible_users {
    principals = ["group:sec-group@example.com"]
  }
  privileged_access {
    gcp_iam_access {
      resource_type = "cloudresourcemanager.googleapis.com/Project"
      resource      = "//cloudresourcemanager.googleapis.com/projects/${local.project}"
      role_bindings {
        role = "roles/storage.admin"
      }
    }
  }
  requester_justification_config {
    not_mandatory {}
  }
}

# reCAPTCHA
resource "google_recaptcha_enterprise_key" "sec" {
  display_name = "sec key"
  web_settings {
    integration_type  = "SCORE"
    allow_all_domains = true
  }
}

# Security Command Center
resource "google_scc_source" "sec" {
  display_name = "sec source"
  organization = "123456789012"
  description  = "sec source"
}

# Public CA
resource "google_public_ca_external_account_key" "sec" {
  location = "global"
}
