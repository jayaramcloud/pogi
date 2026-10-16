# AppSync: a GraphQL API with a schema and an API key, tf-items behind a VTL
# resolver and tf-fn behind an APPSYNC_JS resolver. Query it with
#   curl -H "x-api-key: $(terraform output -raw graphql_key)" -d '{"query":"{ hello }"}' $(terraform output -raw graphql_url)
resource "aws_appsync_graphql_api" "app" {
  name                = "tf-graphql"
  authentication_type = "API_KEY"
  schema              = <<-EOT
    type Item {
      pk: String!
      sk: String!
    }
    type Query {
      item(pk: String!, sk: String!): Item
      hello: AWSJSON
    }
  EOT
}

resource "aws_appsync_api_key" "app" {
  api_id = aws_appsync_graphql_api.app.id
}

resource "aws_appsync_datasource" "items" {
  api_id           = aws_appsync_graphql_api.app.id
  name             = "items"
  type             = "AMAZON_DYNAMODB"
  service_role_arn = aws_iam_role.fn.arn
  dynamodb_config {
    table_name = aws_dynamodb_table.items.name
  }
}

resource "aws_appsync_datasource" "fn" {
  api_id           = aws_appsync_graphql_api.app.id
  name             = "fn"
  type             = "AWS_LAMBDA"
  service_role_arn = aws_iam_role.fn.arn
  lambda_config {
    function_arn = aws_lambda_function.fn.arn
  }
}

resource "aws_appsync_resolver" "item" {
  api_id            = aws_appsync_graphql_api.app.id
  type              = "Query"
  field             = "item"
  data_source       = aws_appsync_datasource.items.name
  request_template  = <<-EOT
    {
      "version": "2018-05-29",
      "operation": "GetItem",
      "key": {
        "pk": $util.dynamodb.toDynamoDBJson($ctx.args.pk),
        "sk": $util.dynamodb.toDynamoDBJson($ctx.args.sk)
      }
    }
  EOT
  response_template = "$util.toJson($ctx.result)"
}

resource "aws_appsync_resolver" "hello" {
  api_id      = aws_appsync_graphql_api.app.id
  type        = "Query"
  field       = "hello"
  data_source = aws_appsync_datasource.fn.name
  code        = <<-EOT
    export function request(ctx) { return { operation: 'Invoke', payload: ctx.args }; }
    export function response(ctx) { return ctx.result; }
  EOT
  runtime {
    name            = "APPSYNC_JS"
    runtime_version = "1.0.0"
  }
}

output "graphql_url" { value = aws_appsync_graphql_api.app.uris["GRAPHQL"] }
output "graphql_key" {
  value     = aws_appsync_api_key.app.key
  sensitive = true
}
