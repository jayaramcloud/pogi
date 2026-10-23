# Aurora DSQL: a cluster.
resource "aws_dsql_cluster" "k" {
  deletion_protection_enabled = false
}
