resource "aws_instance" "a" { subnet_id = aws_subnet.b.id }
resource "aws_subnet" "b" { vpc_id = aws_instance.a.id }
