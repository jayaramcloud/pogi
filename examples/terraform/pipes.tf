# EventBridge Pipes and a WebSocket API: new items of the table go to the
# jobs queue, and the WebSocket API hands its connections and messages to
# the function, answering on $default.
resource "aws_pipes_pipe" "new_items" {
  name     = "tf-new-items"
  role_arn = aws_iam_role.fn.arn
  source   = aws_dynamodb_table.items.stream_arn
  target   = aws_sqs_queue.jobs.arn
  source_parameters {
    dynamodb_stream_parameters {
      starting_position = "LATEST"
      batch_size        = 10
    }
    filter_criteria {
      filter { pattern = jsonencode({ eventName = ["INSERT"] }) }
    }
  }
  target_parameters {
    input_template = "{\"pk\": <$.dynamodb.Keys.pk.S>}"
  }
}

resource "aws_apigatewayv2_api" "ws" {
  name                       = "tf-ws"
  protocol_type              = "WEBSOCKET"
  route_selection_expression = "$request.body.action"
}
resource "aws_apigatewayv2_integration" "ws" {
  api_id             = aws_apigatewayv2_api.ws.id
  integration_type   = "AWS_PROXY"
  integration_method = "POST"
  integration_uri    = aws_lambda_function.fn.invoke_arn
}
resource "aws_apigatewayv2_route" "ws" {
  for_each  = toset(["$connect", "$disconnect", "$default"])
  api_id    = aws_apigatewayv2_api.ws.id
  route_key = each.key
  target    = "integrations/${aws_apigatewayv2_integration.ws.id}"
}
resource "aws_apigatewayv2_route_response" "ws" {
  api_id             = aws_apigatewayv2_api.ws.id
  route_id           = aws_apigatewayv2_route.ws["$default"].id
  route_response_key = "$default"
}
resource "aws_apigatewayv2_deployment" "ws" {
  api_id     = aws_apigatewayv2_api.ws.id
  depends_on = [aws_apigatewayv2_route.ws]
}
resource "aws_apigatewayv2_stage" "ws" {
  api_id        = aws_apigatewayv2_api.ws.id
  name          = "dev"
  deployment_id = aws_apigatewayv2_deployment.ws.id
}
