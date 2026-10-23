# The generic Google engine (internal/gcp/generic): Cloud DNS, Filestore and Certificate Manager.
# Needs main.tf in the same folder (it sets the provider).

resource "google_dns_managed_zone" "generic_zone" {
  name        = "generic-zone"
  dns_name    = "generic.example.com."
  description = "Managed by Terraform"
  labels      = { env = "dev" }
}

resource "google_dns_record_set" "generic_www" {
  managed_zone = google_dns_managed_zone.generic_zone.name
  name         = "www.${google_dns_managed_zone.generic_zone.dns_name}"
  type         = "A"
  ttl          = 300
  rrdatas      = ["10.1.2.3"]
}

resource "google_filestore_instance" "generic_share" {
  name        = "generic-share"
  location    = "us-central1-b"
  tier        = "BASIC_HDD"
  description = "shared files"
  labels      = { env = "dev" }

  file_shares {
    name        = "share1"
    capacity_gb = 1024
  }

  networks {
    network = "default"
    modes   = ["MODE_IPV4"]
  }
}

resource "google_certificate_manager_dns_authorization" "generic_auth" {
  name        = "generic-auth"
  description = "domain proof"
  domain      = "generic.example.com"
}

resource "google_certificate_manager_certificate" "generic_cert" {
  name        = "generic-cert"
  description = "managed certificate"
  managed {
    domains            = ["generic.example.com"]
    dns_authorizations = [google_certificate_manager_dns_authorization.generic_auth.id]
  }
}

resource "google_certificate_manager_certificate_map" "generic_map" {
  name        = "generic-map"
  description = "certificate map"
}
