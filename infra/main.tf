terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ---------------------------------------------------------
#INFO: GCS bucket — single bucket, path prefixes for layers
# ---------------------------------------------------------
resource "google_storage_bucket" "lakehouse" {
  name     = var.bucket_name
  location = var.region
  project  = var.project_id

  uniform_bucket_level_access = true
  force_destroy               = true

  versioning {
    enabled = false
  }

  # auto-delete raw landing files after 90 days
  lifecycle_rule {
    condition {
      age            = 90
      matches_prefix = ["raw/"]
    }
    action {
      type = "Delete"
    }
  }

  # move old raw files to nearline after 30 days
  lifecycle_rule {
    condition {
      age            = 30
      matches_prefix = ["raw/"]
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
}

# ---------------------------------------------------------
#INFO: Service account for pipeline access
# ---------------------------------------------------------
resource "google_service_account" "pipeline" {
  account_id   = var.sa_account_id
  display_name = "Lakehouse Pipeline SA"
  project      = var.project_id
}

# SA can read/write objects in the bucket
resource "google_storage_bucket_iam_member" "pipeline_object_admin" {
  bucket = google_storage_bucket.lakehouse.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

# SA can list bucket metadata (needed for gsutil/SDK)
resource "google_storage_bucket_iam_member" "pipeline_bucket_reader" {
  bucket = google_storage_bucket.lakehouse.name
  role   = "roles/storage.legacyBucketReader"
  member = "serviceAccount:${google_service_account.pipeline.email}"
}

# ---------------------------------------------------------
#INFO: SA key (for local dev / CI — use workload identity in prod)
# ---------------------------------------------------------
resource "google_service_account_key" "pipeline_key" {
  service_account_id = google_service_account.pipeline.name
}

# Write the key to a local file (gitignored)
resource "local_file" "sa_key_json" {
  content  = base64decode(google_service_account_key.pipeline_key.private_key)
  filename = "${path.module}/sa-key.json"

  file_permission = "0600"
}
