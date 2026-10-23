resource "aws_vpc" "main" {
  cidr_block = "10.0.0.0/16"
}
module "net" {
  source = "./net"
  vpc    = aws_vpc.main.id
  size   = "gp3"
}
module "multi" {
  for_each = toset(["a", "b"])
  source   = "./net"
  vpc      = "v-${each.key}"
  size     = "io2"
}
resource "aws_instance" "app" {
  subnet_id = module.net.subnet_id
}
