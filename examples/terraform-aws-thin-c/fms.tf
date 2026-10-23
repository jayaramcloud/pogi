# Firewall Manager: the administrator account and a WAFv2 policy.
data "aws_caller_identity" "k" {}

resource "aws_fms_admin_account" "k" {
  account_id = data.aws_caller_identity.k.account_id
}

resource "aws_fms_policy" "k" {
  name                  = "k-waf"
  remediation_enabled   = false
  resource_type         = "AWS::ElasticLoadBalancingV2::LoadBalancer"
  exclude_resource_tags = false
  security_service_policy_data {
    type = "WAFV2"
    managed_service_data = jsonencode({
      type                              = "WAFV2"
      preProcessRuleGroups              = []
      postProcessRuleGroups             = []
      defaultAction                     = { type = "ALLOW" }
      overrideCustomerWebACLAssociation = false
    })
  }
  depends_on = [aws_fms_admin_account.k]
}
