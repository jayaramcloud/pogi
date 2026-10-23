# Memorystore for Redis: a real Redis container. Needs Docker.
resource "google_redis_instance" "redis_cache" {
  name           = "pogi-tf-redis"
  tier           = "BASIC"
  memory_size_gb = 1
  region         = local.region
  labels         = { env = "dev" }
}

output "redis_host" {
  value = google_redis_instance.redis_cache.host
}

output "redis_port" {
  value = google_redis_instance.redis_cache.port
}
