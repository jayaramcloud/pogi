locals {
  disks = { data = 100, logs = 20 }
  names = toset(["x", "y"])
}
resource "aws_ebs_volume" "d" {
  for_each = local.disks
  size     = each.value
  tags     = { Name = each.key }
}
resource "aws_sqs_queue" "q" {
  for_each = local.names
  name     = each.key
}
resource "aws_instance" "web" {
  count         = 3
  instance_type = count.index == 0 ? "m5.large" : "t3.micro"
}
resource "aws_instance" "none" {
  count = 0
}
resource "aws_eip" "e" {
  instance = aws_instance.web[1].id
}
