variable "vpc" {}
variable "size" { default = "gp2" }
resource "aws_subnet" "s" {
  vpc_id = var.vpc
}
resource "aws_ebs_volume" "v" {
  type = var.size
}
output "subnet_id" { value = aws_subnet.s.id }
