variable "size" { default = "small" }
variable "count_n" { type = number }
variable "tags" {
  type    = map(string)
  default = { a = "1" }
}
variable "nodefault" {}
variable "fromfile" { default = "d" }
variable "auto" { default = "d" }
resource "aws_instance" "a" {
  instance_type = var.size
  tags          = var.tags
  ami           = var.nodefault
  user_data     = "${var.fromfile}-${var.auto}"
  count         = var.count_n
}
