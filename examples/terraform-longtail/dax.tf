# DAX: a cluster whose endpoint is pogi's own DynamoDB endpoint. Point a plain DynamoDB client at
# `dax_url`; the DAX client protocol (dax://) is not spoken.
resource "aws_vpc" "dax" {
  cidr_block = "10.40.0.0/16"
}

resource "aws_subnet" "dax" {
  count      = 2
  vpc_id     = aws_vpc.dax.id
  cidr_block = "10.40.${count.index}.0/24"
}

resource "aws_iam_role" "dax" {
  name = "dax-service"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "sts:AssumeRole", Principal = { Service = "dax.amazonaws.com" } }]
  })
}

resource "aws_dax_subnet_group" "dax" {
  name       = "dax-subnets"
  subnet_ids = aws_subnet.dax[*].id
}

resource "aws_dax_parameter_group" "dax" {
  name = "dax-ttl"
  parameters {
    name  = "query-ttl-millis"
    value = "60000"
  }
  parameters {
    name  = "record-ttl-millis"
    value = "120000"
  }
}

resource "aws_dax_cluster" "dax" {
  cluster_name         = "dax-cache"
  iam_role_arn         = aws_iam_role.dax.arn
  node_type            = "dax.t3.small"
  replication_factor   = 2
  subnet_group_name    = aws_dax_subnet_group.dax.name
  parameter_group_name = aws_dax_parameter_group.dax.name
}

output "dax_url" {
  value = "http://${aws_dax_cluster.dax.cluster_address}:${aws_dax_cluster.dax.port}"
}
