# AppConfig: an application whose environment rolls back when a CloudWatch
# alarm fires, a hosted JSON configuration checked by a JSON schema, a custom
# deployment strategy and a deployment. Clients read it through AppConfigData
# (StartConfigurationSession, then GetLatestConfiguration).
resource "aws_cloudwatch_metric_alarm" "shop_errors" {
  alarm_name          = "tf-shop-errors"
  namespace           = "Shop"
  metric_name         = "Errors"
  statistic           = "Sum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 1
  comparison_operator = "GreaterThanOrEqualToThreshold"
}

resource "aws_appconfig_application" "shop" {
  name        = "tf-shop"
  description = "the web shop"
  tags        = { team = "web" }
}

resource "aws_appconfig_environment" "prod" {
  name           = "prod"
  application_id = aws_appconfig_application.shop.id
  monitor {
    alarm_arn = aws_cloudwatch_metric_alarm.shop_errors.arn
  }
  tags = { stage = "prod" }
}

resource "aws_appconfig_configuration_profile" "settings" {
  application_id = aws_appconfig_application.shop.id
  name           = "settings"
  location_uri   = "hosted"
  validator {
    type    = "JSON_SCHEMA"
    content = jsonencode({ type = "object", required = ["color"], properties = { color = { type = "string" } } })
  }
  tags = { team = "web" }
}

resource "aws_appconfig_hosted_configuration_version" "v1" {
  application_id           = aws_appconfig_application.shop.id
  configuration_profile_id = aws_appconfig_configuration_profile.settings.configuration_profile_id
  content_type             = "application/json"
  content                  = jsonencode({ color = "red" })
  description              = "red"
}

resource "aws_appconfig_deployment_strategy" "quick" {
  name                           = "tf-quick"
  deployment_duration_in_minutes = 1
  growth_factor                  = 50
  final_bake_time_in_minutes     = 1
  replicate_to                   = "NONE"
}

resource "aws_appconfig_deployment" "v1" {
  application_id           = aws_appconfig_application.shop.id
  environment_id           = aws_appconfig_environment.prod.environment_id
  configuration_profile_id = aws_appconfig_configuration_profile.settings.configuration_profile_id
  configuration_version    = aws_appconfig_hosted_configuration_version.v1.version_number
  deployment_strategy_id   = aws_appconfig_deployment_strategy.quick.id
  description              = "first"
  tags                     = { by = "terraform" }
}
