# Generated long-tail services (internal/aws/generic): EFS, CodeArtifact, Backup, GuardDuty,
# Classic ELB and Route 53 Resolver, all served from botocore models.
resource "aws_efs_file_system" "longtail" {
  creation_token = "tf-longtail"
  encrypted      = true
  tags           = { Name = "tf-longtail" }
}

resource "aws_codeartifact_domain" "longtail" {
  domain = "tf-longtail"
  tags   = { env = "dev" }
}

resource "aws_codeartifact_repository" "longtail" {
  repository  = "tf-longtail"
  domain      = aws_codeartifact_domain.longtail.domain
  description = "packages"
}

resource "aws_backup_vault" "longtail" {
  name = "tf-longtail"
  tags = { env = "dev" }
}

resource "aws_backup_plan" "longtail" {
  name = "tf-longtail"
  rule {
    rule_name         = "daily"
    target_vault_name = aws_backup_vault.longtail.name
    schedule          = "cron(0 12 * * ? *)"
  }
}

resource "aws_guardduty_detector" "longtail" {
  enable                       = true
  finding_publishing_frequency = "FIFTEEN_MINUTES"
  tags                         = { env = "dev" }
}

resource "aws_elb" "longtail" {
  name               = "tf-longtail"
  availability_zones = ["us-east-1a"]
  listener {
    instance_port     = 80
    instance_protocol = "http"
    lb_port           = 80
    lb_protocol       = "http"
  }
  tags = { env = "dev" }
}

resource "aws_vpc" "longtail" {
  cidr_block = "10.42.0.0/16"
}

resource "aws_subnet" "longtail" {
  count             = 2
  vpc_id            = aws_vpc.longtail.id
  cidr_block        = "10.42.${count.index}.0/24"
  availability_zone = "us-east-1${count.index == 0 ? "a" : "b"}"
}

resource "aws_security_group" "longtail" {
  name   = "tf-longtail"
  vpc_id = aws_vpc.longtail.id
}

resource "aws_route53_resolver_endpoint" "longtail" {
  name               = "tf-longtail"
  direction          = "INBOUND"
  security_group_ids = [aws_security_group.longtail.id]
  dynamic "ip_address" {
    for_each = aws_subnet.longtail
    content {
      subnet_id = ip_address.value.id
    }
  }
  tags = { env = "dev" }
}
