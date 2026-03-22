variable "project" { 
    description = "Name of the project"
    default = "project-a3416167-bd30-4a48-987"
}

variable "location" { 
    description = "Location where the dataset resides"
    default = "US"
}

variable "region" { 
    description = "Region where the dataset resides"
    default = "us-central1"
}

variable "credentials" { 
    description = "credentials"
    default = "/workspaces/de-zoomcamp/Week_1/terraform/keys/my-creds.json"
}

variable "gcs_bucket_name" { 
    description = "Name of the bucket"
    default = "project-a3416167-bd30-4a48-987-nyc-taxi-raw"
}

variable "bq_dataset_name" { 
    description = "Name of the dataset"
    default = "nyc_taxi_analytics"
}
