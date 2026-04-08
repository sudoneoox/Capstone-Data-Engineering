variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCS bucket region"
  type        = string
  default     = "us-central1"
}

variable "bucket_name" {
  description = "Name for the lakehouse GCS bucket"
  type        = string
  default     = "job-market-lakehouse"
}

variable "sa_account_id" {
  description = "Service account ID for pipeline access"
  type        = string
  default     = "lakehouse-pipeline"
}
