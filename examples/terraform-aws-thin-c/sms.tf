# End User Messaging SMS and voice: a configuration set and an opt-out list.
resource "aws_pinpointsmsvoicev2_configuration_set" "k" {
  name = "k-config-set"
}

resource "aws_pinpointsmsvoicev2_opt_out_list" "k" {
  name = "k-opt-outs"
}
