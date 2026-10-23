# Storage Gateway: a tape pool and a gateway.
resource "aws_storagegateway_tape_pool" "k" {
  pool_name     = "k-pool"
  storage_class = "GLACIER"
}

# A gateway made with an activation key: no appliance is contacted.
resource "aws_storagegateway_gateway" "k" {
  gateway_name     = "k-gateway"
  gateway_timezone = "GMT-5:00"
  gateway_type     = "FILE_S3"
  activation_key   = "AAAAA-BBBBB-CCCCC-DDDDD-EEEEE"
}
