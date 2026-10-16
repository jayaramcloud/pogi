# Cognito identity pools, IoT Core and Cloud Map. The topic rule sends hot
# readings published over MQTT (localhost:1883, or ws://localhost:7644/mqtt)
# to the jobs queue of main.tf.
resource "aws_iam_role" "identity" {
  for_each = toset(["authenticated", "unauthenticated"])
  name     = "tf-identity-${each.key}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRoleWithWebIdentity"
      Principal = { Federated = "cognito-identity.amazonaws.com" }
    }]
  })
}
resource "aws_cognito_identity_pool" "app" {
  identity_pool_name               = "tf app"
  allow_unauthenticated_identities = true
  developer_provider_name          = "login.tf.app"
  cognito_identity_providers {
    client_id               = aws_cognito_user_pool_client.web.id
    provider_name           = aws_cognito_user_pool.users.endpoint
    server_side_token_check = false
  }
  tags = { team = "app" }
}
resource "aws_cognito_identity_pool_roles_attachment" "app" {
  identity_pool_id = aws_cognito_identity_pool.app.id
  roles = {
    authenticated   = aws_iam_role.identity["authenticated"].arn
    unauthenticated = aws_iam_role.identity["unauthenticated"].arn
  }
  role_mapping {
    identity_provider         = "${aws_cognito_user_pool.users.endpoint}:${aws_cognito_user_pool_client.web.id}"
    ambiguous_role_resolution = "AuthenticatedRole"
    type                      = "Rules"
    mapping_rule {
      claim      = "email"
      match_type = "Contains"
      value      = "@example.com"
      role_arn   = aws_iam_role.identity["authenticated"].arn
    }
  }
}

resource "aws_iot_thing" "lamp" {
  name       = "tf-lamp"
  attributes = { room = "hall" }
}
resource "aws_iot_certificate" "lamp" {
  active = true
}
resource "aws_iot_thing_principal_attachment" "lamp" {
  thing     = aws_iot_thing.lamp.name
  principal = aws_iot_certificate.lamp.arn
}
resource "aws_iot_policy" "devices" {
  name = "tf-devices"
  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = ["iot:Connect", "iot:Publish", "iot:Subscribe", "iot:Receive"], Resource = "*" }]
  })
}
resource "aws_iot_policy_attachment" "lamp" {
  policy = aws_iot_policy.devices.name
  target = aws_iot_certificate.lamp.arn
}
resource "aws_iam_role" "iot_rule" {
  name = "tf-iot-rule"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "sts:AssumeRole", Principal = { Service = "iot.amazonaws.com" } }]
  })
}
resource "aws_iot_topic_rule" "hot" {
  name        = "tf_hot_readings"
  enabled     = true
  sql         = "SELECT temp, topic(2) AS device FROM 'sensors/+/data' WHERE temp > 30"
  sql_version = "2016-03-23"
  sqs {
    queue_url  = aws_sqs_queue.jobs.id
    role_arn   = aws_iam_role.iot_rule.arn
    use_base64 = false
  }
  tags = { team = "iot" }
}

resource "aws_service_discovery_http_namespace" "apps" {
  name        = "tf-apps"
  description = "services found through DiscoverInstances"
  tags        = { team = "platform" }
}
resource "aws_service_discovery_service" "api" {
  name         = "api"
  namespace_id = aws_service_discovery_http_namespace.apps.id
}
