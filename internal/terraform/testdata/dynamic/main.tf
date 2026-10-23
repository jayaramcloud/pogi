variable "ports" { default = [80, 443] }
resource "aws_instance" "a" {
  instance_type = "t3.micro"
  root_block_device {
    volume_size = 50
  }
  ebs_block_device {
    device_name = "/dev/sdb"
    volume_size = 10
  }
}
resource "aws_security_group" "sg" {
  dynamic "ingress" {
    for_each = var.ports
    content {
      from_port = ingress.value
      to_port   = ingress.value
      protocol  = "tcp"
    }
  }
  dynamic "egress" {
    for_each = { a = 1 }
    iterator = e
    content {
      from_port = e.value
      description = e.key
    }
  }
}
