import pandas as pd
from google.cloud import bigquery, storage
import os

# Config
PROJECT_ID = "project-a3416167-bd30-4a48-987"
BUCKET_NAME = "project-a3416167-bd30-4a48-987-nyc-taxi-raw"
DATASET_ID = "nyc_taxi_analytics"
TABLE_ID = "yellow_taxi_raw"
CSV_FILE = "yellow_tripdata_2021-01.csv.gz"
GCS_BLOB_NAME = f"raw/{CSV_FILE}" 
ZONES_FILE = "taxi_zone_lookup.csv"
ZONES_TABLE_ID = "taxi_zones"
ZONES_BLOB_NAME = f"raw/{ZONES_FILE}"

def upload_to_gcs(bucket_name, source_file, destination_blob):
    print(f"Uploading {source_file} to GCS...")
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)
    blob.upload_from_filename(source_file)
    print(f"Uploaded to gs://{bucket_name}/{destination_blob}")

def load_to_bigquery(bucket_name, blob_name, dataset_id, table_id):
    print(f"Loading data into BigQuery...")
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{dataset_id}.{table_id}"
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    uri = f"gs://{bucket_name}/{blob_name}"
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()
    print(f"Loaded data into {table_ref}")

def transform_data():
    client = bigquery.Client(project=PROJECT_ID)
    with open("transform.sql", "r") as f:
        sql = f.read()
    client.query(sql).result()
    print("Transformation complete!")

def load_zones_to_bigquery():
    print("Loading taxi zones reference table...")
    upload_to_gcs(BUCKET_NAME, ZONES_FILE, ZONES_BLOB_NAME)
    
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{ZONES_TABLE_ID}"
    
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    
    uri = f"gs://{BUCKET_NAME}/{ZONES_BLOB_NAME}"
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()
    print(f"Loaded zones into {table_ref}")

if __name__ == "__main__":
    upload_to_gcs(BUCKET_NAME, CSV_FILE, GCS_BLOB_NAME)
    load_to_bigquery(BUCKET_NAME, GCS_BLOB_NAME, DATASET_ID, TABLE_ID)
    load_zones_to_bigquery()
    transform_data()
    print("Pipeline complete!")