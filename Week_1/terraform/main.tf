terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.21.0"
    }
  }
}

provider "google" {
  # credentials = file(var.credentials)PS1
  project     = var.project
  region      = var.region
}

resource "google_storage_bucket" "nyc-taxi-raw-data" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "nyc_taxi_analytics" {
  dataset_id = var.bq_dataset_name
  project    = var.project
  location   = var.location
  delete_contents_on_destroy  = true
}
