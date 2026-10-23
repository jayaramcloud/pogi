# User Notifications: a configuration and a rule that sends EC2 events to it.
resource "aws_notifications_notification_configuration" "k" {
  name        = "k-config"
  description = "EC2 state changes"
}

resource "aws_notifications_event_rule" "k" {
  notification_configuration_arn = aws_notifications_notification_configuration.k.arn
  event_type                     = "EC2 Instance State-change Notification"
  source                         = "aws.ec2"
  regions                        = ["us-east-1"]
}
