# Amplify, Shield, CodeCommit, Location, Keyspaces, FSx, Resource Groups and CodeStar Connections of
# the generated long tail (internal/aws/generic). Each is created, planned (no changes) and destroyed against pogi.
# Polly has no Terraform resource for the lexicon that pogi emulates, so the SDK test covers it.

# --- Amplify

resource "aws_amplify_app" "site" {
  name        = "tf-site"
  description = "a static site"
  tags        = { env = "dev" }
}

resource "aws_amplify_branch" "main" {
  app_id      = aws_amplify_app.site.id
  branch_name = "main"
  stage       = "PRODUCTION"
}

# --- Shield: a subscription lasts a year, so it is never destroyed (skip_destroy), as against real AWS

resource "aws_shield_subscription" "this" {
  auto_renew   = "ENABLED"
  skip_destroy = true
}

resource "aws_eip" "protected" {
  domain = "vpc"
}

resource "aws_shield_protection" "eip" {
  name         = "tf-eip"
  resource_arn = "arn:${data.aws_partition.current.partition}:ec2:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:eip-allocation/${aws_eip.protected.id}"
  depends_on   = [aws_shield_subscription.this]
}

data "aws_partition" "current" {}
data "aws_region" "current" {}

# --- CodeCommit

resource "aws_codecommit_repository" "app" {
  repository_name = "tf-app"
  description     = "an application"
  tags            = { env = "dev" }
}

# --- Location

resource "aws_location_map" "city" {
  map_name = "tf-city"
  configuration {
    style = "VectorEsriNavigation"
  }
}

resource "aws_location_place_index" "places" {
  index_name  = "tf-places"
  data_source = "Esri"
}

resource "aws_location_route_calculator" "routes" {
  calculator_name = "tf-routes"
  data_source     = "Esri"
}

resource "aws_location_geofence_collection" "fences" {
  collection_name = "tf-fences"
}

resource "aws_location_tracker" "cars" {
  tracker_name = "tf-cars"
}

resource "aws_location_tracker_association" "cars" {
  tracker_name = aws_location_tracker.cars.tracker_name
  consumer_arn = aws_location_geofence_collection.fences.collection_arn
}

# --- Keyspaces

resource "aws_keyspaces_keyspace" "shop" {
  name = "tf_shop"
}

resource "aws_keyspaces_table" "orders" {
  keyspace_name = aws_keyspaces_keyspace.shop.name
  table_name    = "orders"
  schema_definition {
    column {
      name = "id"
      type = "uuid"
    }
    column {
      name = "total"
      type = "decimal"
    }
    partition_key {
      name = "id"
    }
  }
}

# --- FSx is left out on purpose. The provider's aws_fsx_lustre_file_system creates in 30 seconds here, plans clean
# and destroys fine, but its delete waits ten minutes before it looks, whatever pogi answers. The SDK test
# (internal/aws/generic/fsx_test.go) covers it. The resource, for a manual run:
#
#   resource "aws_fsx_lustre_file_system" "scratch" {
#     storage_capacity = 1200
#     subnet_ids       = [aws_subnet.more.id]
#     deployment_type  = "SCRATCH_2"
#   }

# --- Resource Groups

resource "aws_resourcegroups_group" "dev" {
  name        = "tf-dev"
  description = "everything tagged env=dev"
  resource_query {
    query = jsonencode({
      ResourceTypeFilters = ["AWS::AllSupported"]
      TagFilters          = [{ Key = "env", Values = ["dev"] }]
    })
  }
  tags = { team = "platform" }
}

# --- CodeStar Connections

resource "aws_codestarconnections_connection" "github" {
  name          = "tf-github"
  provider_type = "GitHub"
  tags          = { env = "dev" }
}
