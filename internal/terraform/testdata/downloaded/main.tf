module "vpc" {
  source = "terraform-aws-modules/vpc/aws"
  name   = "x"
}
module "missing" {
  source = "git::https://example.com/x.git"
}
