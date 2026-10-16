# An organization with an OU, a service control policy on it and a tag policy.
resource "aws_organizations_organization" "org" {
  feature_set                   = "ALL"
  enabled_policy_types          = ["SERVICE_CONTROL_POLICY", "TAG_POLICY"]
  aws_service_access_principals = ["sso.amazonaws.com"]
}

resource "aws_organizations_organizational_unit" "dev" {
  name      = "dev"
  parent_id = aws_organizations_organization.org.roots[0].id
  tags      = { env = "dev" }
}

resource "aws_organizations_policy" "no_leaving" {
  name        = "no-leaving"
  description = "Member accounts stay in the organization"
  type        = "SERVICE_CONTROL_POLICY"
  content = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Deny", Action = "organizations:LeaveOrganization", Resource = "*" }]
  })
  tags       = { env = "dev" }
  depends_on = [aws_organizations_organization.org]
}

resource "aws_organizations_policy_attachment" "dev" {
  policy_id = aws_organizations_policy.no_leaving.id
  target_id = aws_organizations_organizational_unit.dev.id
}

# Verified Permissions: a policy store with a Cedar policy.
resource "aws_verifiedpermissions_policy_store" "photos" {
  description = "photo sharing"
  validation_settings {
    mode = "OFF"
  }
}

resource "aws_verifiedpermissions_policy" "owner" {
  policy_store_id = aws_verifiedpermissions_policy_store.photos.policy_store_id
  definition {
    static {
      description = "owners see their photos"
      statement   = "permit (principal, action, resource) when { resource.owner == principal };"
    }
  }
}

# Resource Explorer: an index and a default view that searches buckets.
resource "aws_resourceexplorer2_index" "main" {
  type = "LOCAL"
  tags = { env = "dev" }
}

resource "aws_resourceexplorer2_view" "buckets" {
  name         = "buckets"
  default_view = true
  filters {
    filter_string = "service:s3"
  }
  included_property {
    name = "tags"
  }
  depends_on = [aws_resourceexplorer2_index.main]
}
