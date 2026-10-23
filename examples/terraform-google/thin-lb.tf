# Cloud Load Balancing and Cloud Armor on the Compute API: stored and read back, nothing carries traffic.
resource "google_compute_security_policy" "lb" {
  name = "lb-policy"
  rule {
    action   = "deny(403)"
    priority = 1000
    match {
      versioned_expr = "SRC_IPS_V1"
      config {
        src_ip_ranges = ["9.9.9.0/24"]
      }
    }
    description = "block one range"
  }
  rule {
    action   = "allow"
    priority = 2147483647
    match {
      versioned_expr = "SRC_IPS_V1"
      config {
        src_ip_ranges = ["*"]
      }
    }
    description = "default rule"
  }
}

resource "google_compute_health_check" "lb" {
  name = "lb-hc"
  tcp_health_check {
    port = 80
  }
}

resource "google_compute_backend_service" "lb" {
  name            = "lb-backend"
  protocol        = "HTTP"
  health_checks   = [google_compute_health_check.lb.id]
  security_policy = google_compute_security_policy.lb.id
}

resource "google_compute_url_map" "lb" {
  name            = "lb-urlmap"
  default_service = google_compute_backend_service.lb.id
}

resource "google_compute_target_http_proxy" "lb" {
  name    = "lb-proxy"
  url_map = google_compute_url_map.lb.id
}

resource "google_compute_managed_ssl_certificate" "lb" {
  name = "lb-cert"
  managed {
    domains = ["lb.example.com."]
  }
}

resource "google_compute_target_https_proxy" "lb" {
  name             = "lb-https-proxy"
  url_map          = google_compute_url_map.lb.id
  ssl_certificates = [google_compute_managed_ssl_certificate.lb.id]
}

resource "google_compute_global_forwarding_rule" "lb" {
  name       = "lb-rule"
  target     = google_compute_target_http_proxy.lb.id
  port_range = "80"
}
