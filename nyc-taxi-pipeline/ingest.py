# Import dependencies
import os
import requests
import pandas as pd
from google.cloud import storage, bigquery
import json

# Variables
JAN_CSV_PATH = "data/raw/yellow_tripdata_2021-01.csv.gz"
FEB_PARQUET_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-02.parquet"
ZONE_CSV_PATH = "data/raw/taxi_zone_lookup.csv"
CREDENTIALS = "keys/nyc-creds.json"
PROJECT_ID = "project-a3416167-bd30-4a48-987"
BUCKET_NAME = "project-a3416167-bd30-4a48-987-nyc-taxi-v2-raw"
DATASET_ID = "nyc_taxi_analytics_v2"

# Google Cloud Credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS

# Functions
def ingest_january():
    """Read local Jan 2021 CSV.gz and convert to Parquet"""
    print("Ingesting January 2021 data...")
    df = pd.read_csv(JAN_CSV_PATH, compression="gzip", dtype={"store_and_fwd_flag": str})
    print(f" Jan rows: {len(df)}")
    # Convert datetime columns
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    # Convert clustering columns to correct types
    df["VendorID"] = df["VendorID"].fillna(0).astype(int)
    df["PULocationID"] = df["PULocationID"].fillna(0).astype(int)
    df["payment_type"] = df["payment_type"].fillna(0).astype(int)
    df["passenger_count"] = df["passenger_count"].fillna(0).astype(int)
    # Fix mixed type column
    df["store_and_fwd_flag"] = df["store_and_fwd_flag"].astype(str)
    # Filter data outlier
    df = df[(df["tpep_pickup_datetime"] >= "2021-01-01") & (df["tpep_pickup_datetime"] < "2021-02-01")]
    # Convert to parquet
    jan_parquet_path = "data/processed/yellow_tripdata_2021-01.parquet"
    os.makedirs("data/processed", exist_ok=True)
    df.to_parquet(jan_parquet_path, index=False)
    print(f" Converted to Parquet: {jan_parquet_path}")

    return df, jan_parquet_path

def ingest_february():
    """Download Feb 2021 Parquet from TLC website"""
    print("Ingesting February 2021 data...")
    # Download raw Parquet file
    os.makedirs("data/raw", exist_ok=True)
    feb_raw_path = "data/raw/yellow_tripdata_2021-02.parquet"
    response = requests.get(FEB_PARQUET_URL)
    with open(feb_raw_path, "wb") as f:
        f.write(response.content)
    print(f"  Downloaded to: {feb_raw_path}")
    # Read into DataFrame
    df = pd.read_parquet(feb_raw_path)
    print(f"  Feb rows: {len(df)}")
    # Convert types to match January schema
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    df["VendorID"] = df["VendorID"].fillna(0).astype(int)
    df["PULocationID"] = df["PULocationID"].fillna(0).astype(int)
    df["payment_type"] = df["payment_type"].fillna(0).astype(int)
    df["passenger_count"] = df["passenger_count"].fillna(0).astype(int)
    df["store_and_fwd_flag"] = df["store_and_fwd_flag"].astype(str)
    df = df[(df["tpep_pickup_datetime"] >= "2021-02-01") & (df["tpep_pickup_datetime"] < "2021-03-01")]
    # Save processed copy
    feb_parquet_path = "data/processed/yellow_tripdata_2021-02.parquet"
    df.to_parquet(feb_parquet_path, index=False)
    print(f"  Saved processed: {feb_parquet_path}")
    
    return df, feb_parquet_path

def ingest_zone():
    """Read local zone lookup csv and convert to Parquet"""
    print("Ingesting zone lookup data...")
    df = pd.read_csv(ZONE_CSV_PATH)
    print(f" zone rows: {len(df)}")
    # Convert to parquet
    zone_parquet_path = "data/processed/taxi_zone_lookup.parquet"
    os.makedirs("data/processed", exist_ok=True)
    df.to_parquet(zone_parquet_path, index=False)
    print(f" Converted to Parquet: {zone_parquet_path}")

    return df, zone_parquet_path

def ingest_weather():
    """Fetch weather data from Open-Meteo API for Jan and Feb 2021"""
    print("Ingesting weather data...")
    
    os.makedirs("data/raw", exist_ok=True)
    
    all_weather = []
    
    months = [
        ("2021-01-01", "2021-01-31", "data/raw/weather_nyc_2021-01.json"),
        ("2021-02-01", "2021-02-28", "data/raw/weather_nyc_2021-02.json"),
    ]
    
    for start_date, end_date, raw_path in months:
        url = (
            f"https://archive-api.open-meteo.com/v1/archive?"
            f"latitude=40.7128&longitude=-74.0060"
            f"&start_date={start_date}&end_date={end_date}"
            f"&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max"
            f"&timezone=America/New_York"
        )
        
        response = requests.get(url)
        data = response.json()
        # Save raw JSON
        with open(raw_path, "w") as f:
            json.dump(data, f)
        print(f"  Saved raw: {raw_path}")
        # Parse into DataFrame
        daily = data["daily"]
        df = pd.DataFrame({
            "date": pd.to_datetime(daily["time"]),
            "temp_max": daily["temperature_2m_max"],
            "temp_min": daily["temperature_2m_min"],
            "precipitation": daily["precipitation_sum"],
            "wind_speed_max": daily["windspeed_10m_max"],
        })
        all_weather.append(df)
    
    # Combine both months
    weather_df = pd.concat(all_weather, ignore_index=True)
    print(f"  Total weather rows: {len(weather_df)}")
    
    # Save processed Parquet
    weather_parquet_path = "data/processed/weather_nyc_2021.parquet"
    weather_df.to_parquet(weather_parquet_path, index=False)
    print(f"  Saved processed: {weather_parquet_path}")
    
    return weather_df, weather_parquet_path

def upload_to_gcs(local_path, destination_blob):
    """Upload a file to GCS bucket"""
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(destination_blob)
    blob.upload_from_filename(local_path)
    print(f"  Uploaded: {destination_blob}")

def upload_all_to_gcs():
    """Upload raw and processed files to GCS"""
    print("Uploading to GCS...")
    
    # Raw files
    upload_to_gcs(JAN_CSV_PATH, "raw/yellow_tripdata_2021-01.csv.gz")
    upload_to_gcs("data/raw/yellow_tripdata_2021-02.parquet", "raw/yellow_tripdata_2021-02.parquet")
    upload_to_gcs("data/raw/weather_nyc_2021-01.json", "raw/weather_nyc_2021-01.json")
    upload_to_gcs("data/raw/weather_nyc_2021-02.json", "raw/weather_nyc_2021-02.json")
    upload_to_gcs(ZONE_CSV_PATH, "raw/taxi_zone_lookup.csv")
    
    # Processed files
    upload_to_gcs("data/processed/yellow_tripdata_2021-01.parquet", "processed/yellow_tripdata_2021-01.parquet")
    upload_to_gcs("data/processed/yellow_tripdata_2021-02.parquet", "processed/yellow_tripdata_2021-02.parquet")
    upload_to_gcs("data/processed/weather_nyc_2021.parquet", "processed/weather_nyc_2021.parquet")
    upload_to_gcs("data/processed/taxi_zone_lookup.parquet", "processed/taxi_zone_lookup.parquet")
    
    print("All files uploaded to GCS!")

def load_to_bigquery():
    """Load processed Parquet files from GCS into BigQuery"""
    print("Loading into BigQuery...")
    client = bigquery.Client(project=PROJECT_ID)
    
    # Load trips (Jan + Feb combined)
    trips_table = f"{PROJECT_ID}.{DATASET_ID}.yellow_trips"
    trips_job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="tpep_pickup_datetime",
        ),
        clustering_fields=["VendorID", "PULocationID", "payment_type", "passenger_count"]
    )
    
    for i, month in enumerate(["01", "02"]):
        if i > 0:
            trips_job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            trips_job_config.schema_update_options = [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
        uri = f"gs://{BUCKET_NAME}/processed/yellow_tripdata_2021-{month}.parquet"
        load_job = client.load_table_from_uri(uri, trips_table, job_config=trips_job_config)
        load_job.result()
        print(f"  Loaded 2021-{month} into {trips_table}")

    # Load zone lookup
    zone_table = f"{PROJECT_ID}.{DATASET_ID}.zone_lookup"
    zone_job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    uri = f"gs://{BUCKET_NAME}/processed/taxi_zone_lookup.parquet"
    load_job = client.load_table_from_uri(uri, zone_table, job_config=zone_job_config)
    load_job.result()
    print(f"  Loaded zone lookup into {zone_table}")
    
    # Load weather
    weather_table = f"{PROJECT_ID}.{DATASET_ID}.weather"
    weather_job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    uri = f"gs://{BUCKET_NAME}/processed/weather_nyc_2021.parquet"
    load_job = client.load_table_from_uri(uri, weather_table, job_config=weather_job_config)
    load_job.result()
    print(f"  Loaded weather into {weather_table}")
    
    print("All tables loaded into BigQuery!")

def create_joined_view():
    """Create a BigQuery view joining trips, zones, and weather"""
    print("Creating joined view...")
    client = bigquery.Client(project=PROJECT_ID)
    
    view_id = f"{PROJECT_ID}.{DATASET_ID}.trips_enriched"
    
    view_query = f"""
    CREATE OR REPLACE VIEW `{view_id}` AS
    SELECT
        t.*,
        pz.Borough AS pickup_borough,
        pz.Zone AS pickup_zone,
        dz.Borough AS dropoff_borough,
        dz.Zone AS dropoff_zone,
        w.temp_max,
        w.temp_min,
        w.precipitation,
        w.wind_speed_max
    FROM `{PROJECT_ID}.{DATASET_ID}.yellow_trips` t
    LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.zone_lookup` pz
        ON t.PULocationID = pz.LocationID
    LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.zone_lookup` dz
        ON t.DOLocationID = dz.LocationID
    LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.weather` w
        ON DATE(t.tpep_pickup_datetime) = DATE(w.date)
    """
    
    client.query(view_query).result()
    print(f"  Created view: {view_id}")
 
def demo_run():
    """Demo: Load March 2021 data and append to existing tables"""
    print("=== NYC Taxi Pipeline v2 — DEMO RUN ===\n")
    
    # Step 1: Ingest March trip data from local file
    print("Ingesting March 2021 data...")
    MARCH_PATH = "/workspaces/de-zoomcamp/nyc-taxi-pipeline/data/raw/yellow_tripdata_2021-03.parquet"
    df = pd.read_parquet(MARCH_PATH)
    print(f"  March rows: {len(df)}")
    
    # Fix types to match Jan/Feb schema
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    df["VendorID"] = df["VendorID"].fillna(0).astype(int)
    df["PULocationID"] = df["PULocationID"].fillna(0).astype(int)
    df["payment_type"] = df["payment_type"].fillna(0).astype(int)
    df["passenger_count"] = df["passenger_count"].fillna(0).astype(int)
    df["store_and_fwd_flag"] = df["store_and_fwd_flag"].astype(str)
    
    # Filter to March only
    df = df[(df["tpep_pickup_datetime"] >= "2021-03-01") & (df["tpep_pickup_datetime"] < "2021-04-01")]
    print(f"  March rows after filtering: {len(df)}")
    
    # Save processed Parquet
    os.makedirs("data/processed", exist_ok=True)
    march_parquet_path = "data/processed/yellow_tripdata_2021-03.parquet"
    df.to_parquet(march_parquet_path, index=False)
    print(f"  Saved processed: {march_parquet_path}")
    
    # Step 2: Load March weather from local file
    print("Loading March weather data...")
    with open("data/raw/weather_nyc_2021-03.json", "r") as f:
        data = json.load(f)
    print(f"  Loaded: data/raw/weather_nyc_2021-03.json")
    
    # Parse into DataFrame
    daily = data["daily"]
    march_weather = pd.DataFrame({
        "date": pd.to_datetime(daily["time"]),
        "temp_max": daily["temperature_2m_max"],
        "temp_min": daily["temperature_2m_min"],
        "precipitation": daily["precipitation_sum"],
        "wind_speed_max": daily["windspeed_10m_max"],
    })
    print(f"  March weather rows: {len(march_weather)}")
    
    # Save processed
    march_weather_path = "data/processed/weather_nyc_2021-03.parquet"
    march_weather.to_parquet(march_weather_path, index=False)
    
    # Step 3: Upload to GCS
    print("Uploading to GCS...")
    upload_to_gcs(MARCH_PATH, "raw/yellow_tripdata_2021-03.parquet")
    upload_to_gcs(march_parquet_path, "processed/yellow_tripdata_2021-03.parquet")
    upload_to_gcs("data/raw/weather_nyc_2021-03.json", "raw/weather_nyc_2021-03.json")
    upload_to_gcs(march_weather_path, "processed/weather_nyc_2021-03.parquet")
    
    # Step 4: Append to BigQuery
    print("Loading into BigQuery...")
    client = bigquery.Client(project=PROJECT_ID)
    
    # Append March trips
    trips_table = f"{PROJECT_ID}.{DATASET_ID}.yellow_trips"
    trip_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )
    uri = f"gs://{BUCKET_NAME}/processed/yellow_tripdata_2021-03.parquet"
    load_job = client.load_table_from_uri(uri, trips_table, job_config=trip_config)
    load_job.result()
    print(f"  Appended March trips into {trips_table}")
    
    # Append March weather
    weather_table = f"{PROJECT_ID}.{DATASET_ID}.weather"
    weather_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    uri = f"gs://{BUCKET_NAME}/processed/weather_nyc_2021-03.parquet"
    load_job = client.load_table_from_uri(uri, weather_table, job_config=weather_config)
    load_job.result()
    print(f"  Appended March weather into {weather_table}")
    
    print("\n=== Demo complete! Dashboard now includes March 2021 data ===")

def main():
    """Run the full pipeline"""
    print("=== NYC Taxi Pipeline v2 ===\n")
    
    # Step 1: Ingest from all sources
    jan_df, jan_path = ingest_january()
    feb_df, feb_path = ingest_february()
    zone_df, zone_path = ingest_zone()
    weather_df, weather_path = ingest_weather()
    
    # Step 2: Upload to GCS
    upload_all_to_gcs()
    
    # Step 3: Load into BigQuery
    load_to_bigquery()
    
    # Step 4: Create joined view
    create_joined_view()
    
    print("\n=== Pipeline complete! ===")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "demo":
        demo_run()
    else:
        main()