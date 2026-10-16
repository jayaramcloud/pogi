# Security and governance services of the generated long tail (internal/aws/generic): IAM Identity Center,
# Identity Store, RAM, WAF v2, Security Hub, Detective, Inspector, Macie, Account, Access Analyzer,
# Network Firewall and Control Tower. Every resource here is created, planned (no changes)
# and destroyed against pogi.


locals {
  account = data.aws_caller_identity.current.account_id
}

# --- a VPC with two subnets, for RAM, CloudHSM and Network Firewall

resource "aws_vpc" "security" {
  cidr_block = "10.60.0.0/16"
  tags       = { Name = "tf-security" }
}

resource "aws_subnet" "security" {
  count             = 2
  vpc_id            = aws_vpc.security.id
  cidr_block        = "10.60.${count.index}.0/24"
  availability_zone = "us-east-1${count.index == 0 ? "a" : "b"}"
}

# --- IAM Identity Center and Identity Store

data "aws_ssoadmin_instances" "this" {}

locals {
  sso_instance = tolist(data.aws_ssoadmin_instances.this.arns)[0]
  sso_store    = tolist(data.aws_ssoadmin_instances.this.identity_store_ids)[0]
}

resource "aws_ssoadmin_permission_set" "admin" {
  name             = "TfAdmin"
  description      = "administrators"
  instance_arn     = local.sso_instance
  session_duration = "PT4H"
  tags             = { env = "dev" }
}

resource "aws_ssoadmin_managed_policy_attachment" "admin" {
  instance_arn       = local.sso_instance
  managed_policy_arn = "arn:aws:iam::aws:policy/ReadOnlyAccess"
  permission_set_arn = aws_ssoadmin_permission_set.admin.arn
}

resource "aws_ssoadmin_permission_set_inline_policy" "admin" {
  instance_arn       = local.sso_instance
  permission_set_arn = aws_ssoadmin_permission_set.admin.arn
  inline_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = ["s3:ListAllMyBuckets"], Resource = "*" }]
  })
}

resource "aws_identitystore_user" "jane" {
  identity_store_id = local.sso_store
  user_name         = "jane"
  display_name      = "Jane Doe"
  name {
    given_name  = "Jane"
    family_name = "Doe"
  }
  emails {
    value   = "jane@example.com"
    primary = true
  }
}

resource "aws_identitystore_group" "admins" {
  identity_store_id = local.sso_store
  display_name      = "tf-admins"
  description       = "administrators"
}

resource "aws_identitystore_group_membership" "jane" {
  identity_store_id = local.sso_store
  group_id          = aws_identitystore_group.admins.group_id
  member_id         = aws_identitystore_user.jane.user_id
}

resource "aws_ssoadmin_account_assignment" "admins" {
  instance_arn       = local.sso_instance
  permission_set_arn = aws_ssoadmin_permission_set.admin.arn
  principal_id       = aws_identitystore_group.admins.group_id
  principal_type     = "GROUP"
  target_id          = local.account
  target_type        = "AWS_ACCOUNT"
}

# --- Resource Access Manager

resource "aws_ram_resource_share" "net" {
  name                      = "tf-net"
  allow_external_principals = false
  tags                      = { env = "dev" }
}

resource "aws_ram_resource_association" "subnet" {
  resource_arn       = aws_subnet.security[0].arn
  resource_share_arn = aws_ram_resource_share.net.arn
}

resource "aws_ram_principal_association" "self" {
  principal          = local.account
  resource_share_arn = aws_ram_resource_share.net.arn
}

# --- WAF v2

resource "aws_wafv2_ip_set" "block" {
  name               = "tf-block"
  scope              = "REGIONAL"
  ip_address_version = "IPV4"
  addresses          = ["10.0.0.0/8", "192.168.0.0/16"]
  tags               = { env = "dev" }
}

resource "aws_wafv2_regex_pattern_set" "bots" {
  name  = "tf-bots"
  scope = "REGIONAL"
  regular_expression { regex_string = "^bot" }
}

resource "aws_wafv2_rule_group" "uris" {
  name     = "tf-uris"
  scope    = "REGIONAL"
  capacity = 30
  rule {
    name     = "block-bots"
    priority = 1
    action {
      block {}
    }
    statement {
      regex_pattern_set_reference_statement {
        arn = aws_wafv2_regex_pattern_set.bots.arn
        field_to_match {
          uri_path {}
        }
        text_transformation {
          priority = 0
          type     = "NONE"
        }
      }
    }
    visibility_config {
      cloudwatch_metrics_enabled = false
      metric_name                = "block-bots"
      sampled_requests_enabled   = false
    }
  }
  visibility_config {
    cloudwatch_metrics_enabled = false
    metric_name                = "tf-uris"
    sampled_requests_enabled   = false
  }
}

resource "aws_wafv2_web_acl" "front" {
  name        = "tf-front"
  scope       = "REGIONAL"
  description = "front door"
  default_action {
    allow {}
  }
  rule {
    name     = "block-ips"
    priority = 1
    action {
      block {}
    }
    statement {
      ip_set_reference_statement {
        arn = aws_wafv2_ip_set.block.arn
      }
    }
    visibility_config {
      cloudwatch_metrics_enabled = false
      metric_name                = "block-ips"
      sampled_requests_enabled   = false
    }
  }
  rule {
    name     = "uris"
    priority = 2
    override_action {
      none {}
    }
    statement {
      rule_group_reference_statement {
        arn = aws_wafv2_rule_group.uris.arn
      }
    }
    visibility_config {
      cloudwatch_metrics_enabled = false
      metric_name                = "uris"
      sampled_requests_enabled   = false
    }
  }
  visibility_config {
    cloudwatch_metrics_enabled = false
    metric_name                = "tf-front"
    sampled_requests_enabled   = false
  }
  tags = { env = "dev" }
}

# --- Security Hub, Detective, Inspector, Macie

resource "aws_securityhub_account" "this" {}

resource "aws_securityhub_standards_subscription" "cis" {
  standards_arn = "arn:aws:securityhub:::ruleset/cis-aws-foundations-benchmark/v/1.2.0"
  depends_on    = [aws_securityhub_account.this]
}

resource "aws_securityhub_action_target" "notify" {
  identifier  = "notify"
  name        = "Notify"
  description = "send a message"
  depends_on  = [aws_securityhub_account.this]
}

resource "aws_detective_graph" "this" {
  tags = { env = "dev" }
}

resource "aws_inspector2_enabler" "this" {
  account_ids    = [local.account]
  resource_types = ["EC2", "ECR"]
}

resource "aws_inspector2_filter" "quiet" {
  name   = "tf-quiet"
  action = "SUPPRESS"
  filter_criteria {
    severity {
      comparison = "EQUALS"
      value      = "LOW"
    }
  }
  depends_on = [aws_inspector2_enabler.this]
}

resource "aws_macie2_account" "this" {
  finding_publishing_frequency = "SIX_HOURS"
  status                       = "ENABLED"
}

resource "aws_macie2_custom_data_identifier" "badge" {
  name                   = "tf-badge"
  regex                  = "B-[0-9]{6}"
  description            = "badge ids"
  maximum_match_distance = 30
  keywords               = ["badge"]
  depends_on             = [aws_macie2_account.this]
}

resource "aws_macie2_findings_filter" "noise" {
  name     = "tf-noise"
  action   = "ARCHIVE"
  position = 1
  finding_criteria {
    criterion {
      field = "severity.description"
      eq    = ["Low"]
    }
  }
  depends_on = [aws_macie2_account.this]
}

# --- Account, Access Analyzer

resource "aws_account_alternate_contact" "security" {
  alternate_contact_type = "SECURITY"
  name                   = "Security Team"
  title                  = "CISO"
  email_address          = "security@example.com"
  phone_number           = "+911234567890"
}

resource "aws_accessanalyzer_analyzer" "this" {
  analyzer_name = "tf-analyzer"
  type          = "ACCOUNT"
  tags          = { env = "dev" }
}

resource "aws_accessanalyzer_archive_rule" "known" {
  analyzer_name = aws_accessanalyzer_analyzer.this.analyzer_name
  rule_name     = "known"
  filter {
    criteria = "isPublic"
    eq       = ["false"]
  }
}

# --- Network Firewall
# (aws_cloudhsm_v2_cluster works too, but its provider waits 30 seconds on create and again on delete.)

resource "aws_networkfirewall_rule_group" "web" {
  name     = "tf-web"
  type     = "STATEFUL"
  capacity = 100
  rule_group {
    rules_source {
      rules_source_list {
        generated_rules_type = "DENYLIST"
        target_types         = ["HTTP_HOST"]
        targets              = [".example.com"]
      }
    }
  }
  tags = { env = "dev" }
}

resource "aws_networkfirewall_firewall_policy" "this" {
  name = "tf-policy"
  firewall_policy {
    stateless_default_actions          = ["aws:forward_to_sfe"]
    stateless_fragment_default_actions = ["aws:forward_to_sfe"]
    stateful_rule_group_reference {
      resource_arn = aws_networkfirewall_rule_group.web.arn
    }
  }
}

resource "aws_networkfirewall_firewall" "this" {
  name                = "tf-firewall"
  firewall_policy_arn = aws_networkfirewall_firewall_policy.this.arn
  vpc_id              = aws_vpc.security.id
  subnet_mapping {
    subnet_id = aws_subnet.security[0].id
  }
  tags = { env = "dev" }
}

# --- Control Tower

resource "aws_organizations_organization" "this" {
  aws_service_access_principals = ["controltower.amazonaws.com"]
  feature_set                   = "ALL"
}

resource "aws_organizations_organizational_unit" "workloads" {
  name      = "Workloads"
  parent_id = aws_organizations_organization.this.roots[0].id
}

resource "aws_controltower_control" "volumes" {
  control_identifier = "arn:aws:controltower:us-east-1::control/AWS-GR_EC2_VOLUME_INUSE_CHECK"
  target_identifier  = aws_organizations_organizational_unit.workloads.arn
}
