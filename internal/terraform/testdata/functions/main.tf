locals {
  m = { small = "t3.micro", big = "m5.large" }
}
resource "aws_instance" "f" {
  instance_type = lookup(local.m, "big", "t3.nano")
  tags          = merge({ a = "1" }, { b = upper("x") })
  ami           = try(local.m.missing, "fallback")
  user_data     = format("%s-%d", "n", min(3, 5))
  key_name      = join(",", concat(["a"], split(",", "b,c")))
  subnet_id     = element(["s1", "s2"], 1)
  cidr          = cidrsubnet("10.0.0.0/16", 8, 1)
  host          = cidrhost("10.0.1.0/24", -2)
  mask          = cidrnetmask("10.0.0.0/12")
  v6            = cidrsubnet("fd00::/56", 8, 3)
  b64           = base64decode(base64encode("hi"))
  quiet         = file("user_data.sh")
  odd           = yamlencode({ a = 1 })
  ok            = can(local.m.small)
  n             = length(flatten([[1], [2, 3]]))
}
