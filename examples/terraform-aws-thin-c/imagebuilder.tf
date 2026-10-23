# EC2 Image Builder: a component, infrastructure, a recipe and a pipeline that never runs.
resource "aws_iam_role" "k_ib" {
  name = "k-imagebuilder"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "sts:AssumeRole", Principal = { Service = "ec2.amazonaws.com" } }]
  })
}

resource "aws_iam_instance_profile" "k_ib" {
  name = "k-imagebuilder"
  role = aws_iam_role.k_ib.name
}

resource "aws_imagebuilder_component" "k" {
  name     = "k-hello"
  platform = "Linux"
  version  = "1.0.0"
  data = yamlencode({
    name          = "hello"
    schemaVersion = 1.0
    phases = [{
      name  = "build"
      steps = [{ name = "say", action = "ExecuteBash", inputs = { commands = ["echo hello"] } }]
    }]
  })
}

resource "aws_imagebuilder_infrastructure_configuration" "k" {
  name                  = "k-infra"
  instance_profile_name = aws_iam_instance_profile.k_ib.name
  instance_types        = ["t3.micro"]
}

resource "aws_imagebuilder_image_recipe" "k" {
  name         = "k-recipe"
  version      = "1.0.0"
  parent_image = "arn:aws:imagebuilder:us-east-1:aws:image/amazon-linux-2023-x86/x.x.x"
  component {
    component_arn = aws_imagebuilder_component.k.arn
  }
}

resource "aws_imagebuilder_image_pipeline" "k" {
  name                             = "k-pipeline"
  image_recipe_arn                 = aws_imagebuilder_image_recipe.k.arn
  infrastructure_configuration_arn = aws_imagebuilder_infrastructure_configuration.k.arn
  status                           = "DISABLED"
}
