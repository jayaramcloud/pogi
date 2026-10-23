# Oracle Database@AWS: an ODB network.
resource "aws_odb_network" "k" {
  display_name         = "k-network"
  availability_zone_id = "use1-az1"
  client_subnet_cidr   = "10.2.0.0/24"
  backup_subnet_cidr   = "10.2.1.0/24"
  s3_access            = "DISABLED"
  zero_etl_access      = "DISABLED"
}
