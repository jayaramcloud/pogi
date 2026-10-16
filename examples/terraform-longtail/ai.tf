# AI services (internal/aws/generic and the local model gateway): Bedrock guardrails and agents,
# SageMaker model, endpoint configuration, endpoint and notebook instance, Rekognition.
resource "aws_iam_role" "ai" {
  name = "tf-ai"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = ["sagemaker.amazonaws.com", "bedrock.amazonaws.com"] }
    }]
  })
}

resource "aws_bedrock_guardrail" "ai" {
  name                      = "tf-ai"
  description               = "blocks nothing, says no"
  blocked_input_messaging   = "Sorry, I cannot answer that."
  blocked_outputs_messaging = "Sorry, I cannot say that."
}

resource "aws_bedrockagent_agent" "ai" {
  agent_name              = "tf-ai"
  agent_resource_role_arn = aws_iam_role.ai.arn
  foundation_model        = "anthropic.claude-3-haiku-20240307-v1:0"
  instruction             = "You answer questions about orders and always say which order you mean."
  idle_session_ttl_in_seconds = 600
  prepare_agent           = true
}

resource "aws_sagemaker_model" "ai" {
  name               = "tf-ai"
  execution_role_arn = aws_iam_role.ai.arn
  primary_container {
    image          = "000000000000.dkr.ecr.us-east-1.amazonaws.com/tf-ai:latest"
    model_data_url = "s3://tf-ai-models/model.tar.gz"
  }
}

resource "aws_sagemaker_endpoint_configuration" "ai" {
  name = "tf-ai"
  production_variants {
    variant_name           = "AllTraffic"
    model_name             = aws_sagemaker_model.ai.name
    initial_instance_count = 1
    instance_type          = "ml.m5.large"
  }
}

resource "aws_sagemaker_endpoint" "ai" {
  name                 = "tf-ai"
  endpoint_config_name = aws_sagemaker_endpoint_configuration.ai.name
}

resource "aws_sagemaker_notebook_instance" "ai" {
  name          = "tf-ai"
  role_arn      = aws_iam_role.ai.arn
  instance_type = "ml.t3.medium"
}

resource "aws_rekognition_collection" "ai" {
  collection_id = "tf-ai"
}

resource "aws_bedrockagent_agent_alias" "ai" {
  agent_alias_name = "live"
  agent_id         = aws_bedrockagent_agent.ai.agent_id
  description      = "the live alias"
}

resource "aws_bedrockagentcore_memory" "ai" {
  name                  = "tf_ai"
  event_expiry_duration = 30
}

resource "aws_bedrock_inference_profile" "ai" {
  name = "tf-ai"
  model_source {
    copy_from = "arn:aws:bedrock:us-east-1::foundation-model/amazon.nova-lite-v1:0"
  }
}
