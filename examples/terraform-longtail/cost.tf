# Cost services (internal/aws/generic): an AWS budget with a notification, a Cost and Usage Report
# definition and a BCM data export, the last two delivering to an S3 bucket, and Cost Explorer's
# cost category and anomaly monitor.

resource "aws_s3_bucket" "cost_reports" {
  bucket        = "tf-cost-reports"
  force_destroy = true
}

resource "aws_budgets_budget" "monthly" {
  name         = "tf-monthly"
  account_id   = data.aws_caller_identity.current.account_id
  budget_type  = "COST"
  limit_amount = "100"
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  cost_filter {
    name   = "Service"
    values = ["Amazon Simple Storage Service"]
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = ["ops@example.com"]
  }

  tags = { env = "dev" }
}

resource "aws_cur_report_definition" "daily" {
  report_name                = "tf-daily"
  time_unit                  = "DAILY"
  format                     = "textORcsv"
  compression                = "GZIP"
  additional_schema_elements = ["RESOURCES"]
  s3_bucket                  = aws_s3_bucket.cost_reports.id
  s3_prefix                  = "cur"
  s3_region                  = "us-east-1"
  additional_artifacts       = ["REDSHIFT", "QUICKSIGHT"]
}

resource "aws_bcmdataexports_export" "costs" {
  export {
    name        = "tf-costs"
    description = "daily cost and usage"

    data_query {
      query_statement = "SELECT line_item_usage_account_id, line_item_unblended_cost FROM COST_AND_USAGE_REPORT"
      table_configurations = {
        COST_AND_USAGE_REPORT = {
          TIME_GRANULARITY                      = "DAILY"
          INCLUDE_RESOURCES                     = "FALSE"
          INCLUDE_MANUAL_DISCOUNT_COMPATIBILITY = "FALSE"
          INCLUDE_SPLIT_COST_ALLOCATION_DATA    = "FALSE"
        }
      }
    }

    destination_configurations {
      s3_destination {
        s3_bucket = aws_s3_bucket.cost_reports.id
        s3_prefix = "bcm"
        s3_region = "us-east-1"
        s3_output_configurations {
          compression = "GZIP"
          format      = "TEXT_OR_CSV"
          output_type = "CUSTOM"
          overwrite   = "OVERWRITE_REPORT"
        }
      }
    }

    refresh_cadence {
      frequency = "SYNCHRONOUS"
    }
  }

  depends_on = [aws_cur_report_definition.daily]
}

resource "aws_ce_cost_category" "env" {
  name         = "tf-env"
  rule_version = "CostCategoryExpression.v1"

  rule {
    value = "prod"
    rule {
      dimension {
        key           = "LINKED_ACCOUNT"
        values        = [data.aws_caller_identity.current.account_id]
        match_options = ["EQUALS"]
      }
    }
  }

  tags = { env = "dev" }
}

resource "aws_ce_anomaly_monitor" "service" {
  name              = "tf-services"
  monitor_type      = "DIMENSIONAL"
  monitor_dimension = "SERVICE"
}
