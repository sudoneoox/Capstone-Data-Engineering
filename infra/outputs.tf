output "bucket_name" {
  description = "GCS bucket name"
  value       = google_storage_bucket.lakehouse.name
}

output "bucket_url" {
  description = "GCS bucket URL"
  value       = google_storage_bucket.lakehouse.url
}

output "service_account_email" {
  description = "Pipeline service account email"
  value       = google_service_account.pipeline.email
}

output "sa_key_path" {
  description = "Path to the generated SA key file"
  value       = local_file.sa_key_json.filename
  sensitive   = true
}
