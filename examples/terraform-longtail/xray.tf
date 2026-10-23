# X-Ray: a group, a sampling rule and the encryption setting. Created, planned (no changes) and destroyed against pogi.

resource "aws_xray_group" "errors" {
  group_name        = "tf-errors"
  filter_expression = "responsetime > 5 OR fault"
  tags              = { env = "dev" }
}

resource "aws_xray_sampling_rule" "api" {
  rule_name      = "tf-api"
  priority       = 100
  version        = 1
  reservoir_size = 2
  fixed_rate     = 0.1
  url_path       = "/api/*"
  host           = "*"
  http_method    = "GET"
  service_type   = "*"
  service_name   = "api"
  resource_arn   = "*"
  tags           = { env = "dev" }
}

resource "aws_xray_encryption_config" "this" {
  type = "NONE"
}
