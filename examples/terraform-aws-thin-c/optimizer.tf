# Compute Optimizer and Cost Optimization Hub: the opt-in settings.
resource "aws_computeoptimizer_enrollment_status" "k" {
  status = "Active"
}

resource "aws_costoptimizationhub_enrollment_status" "k" {
  include_member_accounts = false
}

resource "aws_costoptimizationhub_preferences" "k" {
  member_account_discount_visibility = "None"
  savings_estimation_mode            = "BeforeDiscounts"
  depends_on                         = [aws_costoptimizationhub_enrollment_status.k]
}
