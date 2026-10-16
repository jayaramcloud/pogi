# Glue Data Catalog and Athena: a database, a partitioned CSV table, a
# workgroup that writes results to S3 and a saved query. With Docker
# running, Athena runs queries on this table on DuckDB.
data "aws_caller_identity" "current" {}

resource "aws_s3_bucket" "athena" {
  bucket        = "tf-athena"
  force_destroy = true
}

resource "aws_glue_catalog_database" "shop" {
  name        = "tf_shop"
  catalog_id  = data.aws_caller_identity.current.account_id
  description = "sales of the shop"
  parameters  = { owner = "data" }
}

resource "aws_glue_catalog_table" "sales" {
  name          = "sales"
  database_name = aws_glue_catalog_database.shop.name
  catalog_id    = data.aws_caller_identity.current.account_id
  table_type    = "EXTERNAL_TABLE"
  parameters = {
    classification           = "csv"
    "skip.header.line.count" = "1"
  }
  partition_keys {
    name = "dt"
    type = "string"
  }
  storage_descriptor {
    location      = "s3://${aws_s3_bucket.athena.bucket}/sales/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
      parameters            = { "field.delim" = "," }
    }
    columns {
      name = "id"
      type = "bigint"
    }
    columns {
      name    = "item"
      type    = "string"
      comment = "what was sold"
    }
    columns {
      name = "price"
      type = "double"
    }
  }
}

resource "aws_athena_workgroup" "shop" {
  name          = "tf-shop"
  force_destroy = true
  configuration {
    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena.bucket}/results/"
    }
  }
  tags = { team = "data" }
}

resource "aws_athena_named_query" "top" {
  name      = "top-items"
  workgroup = aws_athena_workgroup.shop.id
  database  = aws_glue_catalog_database.shop.name
  query     = "SELECT item, sum(price) AS total FROM ${aws_glue_catalog_table.sales.name} GROUP BY item ORDER BY total DESC"
}
