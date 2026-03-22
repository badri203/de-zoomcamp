# NYC Taxi Pipeline V2

End-to-end data pipeline built on GCP as part of my **GCP Professional Data Engineer** certification preparation.

The pipeline ingests NYC Yellow Taxi trip data from multiple sources, enriches it with weather data and zone lookups, loads it into BigQuery, and surfaces it in a live Tableau dashboard.

## Architecture

<img width="844" height="481" alt="image" src="https://github.com/user-attachments/assets/ef647aff-c13c-417a-9e8a-73afe0df0b84" />


## Data Sources

| Source | Format | Ingestion Method |
|--------|--------|-----------------|
| NYC Taxi Trips — Jan 2021 | CSV (compressed) | Local file read |
| NYC Taxi Trips — Feb 2021 | Parquet | HTTP download from TLC |
| NYC Weather — Jan & Feb 2021 | JSON | Open-Meteo API |
| Taxi Zone Lookup | CSV | Local file read |

## GCS Bucket Structure

```
nyc-taxi-v2-raw/
├── raw/                          # Original files, untouched
│   ├── yellow_tripdata_2021-01.csv.gz
│   ├── yellow_tripdata_2021-02.parquet
│   ├── weather_nyc_2021-01.json
│   ├── weather_nyc_2021-02.json
│   └── taxi_zone_lookup.csv
└── processed/                    # Cleaned, converted to Parquet
    ├── yellow_tripdata_2021-01.parquet
    ├── yellow_tripdata_2021-02.parquet
    ├── weather_nyc_2021.parquet
    └── taxi_zone_lookup.parquet
```

## BigQuery Schema

**Tables:**
- `yellow_trips` — Trip data (partitioned by `tpep_pickup_datetime`, clustered by `VendorID`, `PULocationID`, `payment_type`, `passenger_count`)
- `zone_lookup` — Maps zone IDs to borough and zone names
- `weather` — Daily temperature, precipitation, and wind speed for NYC

**View:**
- `trips_enriched` — Joins trips with pickup/dropoff borough names and daily weather conditions

## Project Structure

```
nyc-taxi-pipeline/
├── data/
│   ├── raw/              # Raw source files (gitignored)
│   └── processed/        # Cleaned Parquet files (gitignored)
├── keys/                 # GCP service account key (gitignored)
├── terraform/
│   ├── main.tf           # GCS bucket + BigQuery dataset
│   ├── variables.tf      # Variable definitions
│   └── terraform.tfvars  # Variable values (gitignored)
├── ingest.py             # Pipeline script
├── pyproject.toml        # Python dependencies
├── uv.lock               # Locked dependency versions
└── README.md
```

## Prerequisites

- Python 3.13+
- [uv](https://docs.astral.sh/uv/) package manager
- [Terraform](https://developer.hashicorp.com/terraform/install)
- GCP account with a project
- GCP service account with roles: `Storage Object Admin`, `BigQuery Data Editor`, `BigQuery Job User`

## Setup

**1. Clone the repo**

```bash
git clone https://github.com/<your-username>/de-zoomcamp.git
cd de-zoomcamp/nyc-taxi-pipeline
```

**2. Install Python dependencies**

```bash
uv sync
```

**3. Add GCP credentials**

Place your service account JSON key in `keys/` (e.g., `keys/nyc-creds.json`).

**4. Configure Terraform**

Create `terraform/terraform.tfvars` with your values:

```hcl
project         = "your-gcp-project-id"
location        = "US"
region          = "us-central1"
credentials     = "/path/to/keys/nyc-creds.json"
gcs_bucket_name = "your-project-id-nyc-taxi-v2-raw"
bq_dataset_name = "nyc_taxi_analytics_v2"
```

**5. Provision infrastructure**

```bash
cd terraform
terraform init
terraform apply
cd ..
```

**6. Add source data**

Place these files in the project root (or `data/raw/`):
- `yellow_tripdata_2021-01.csv.gz` — [Download from TLC](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- `taxi_zone_lookup.csv` — [Download from TLC](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv)

February data and weather data are downloaded automatically by the pipeline.

## Usage

**Run the full pipeline (Jan + Feb):**

```bash
uv run ingest.py
```

This ingests all sources, uploads to GCS, loads into BigQuery, and creates the joined view.

**Run the demo (append March):**

```bash
uv run ingest.py demo
```

This appends March trip data and weather to the existing tables. Run the full pipeline first to reset.

## Pipeline Steps

1. **Ingest** — Read local CSV, download Parquet from TLC, fetch weather from Open-Meteo API, read zone lookup
2. **Transform** — Convert datetime columns, fix integer types, align schemas between CSV and Parquet sources, filter date ranges
3. **Upload to GCS** — Raw files and processed Parquet files to separate folders
4. **Load to BigQuery** — Partitioned and clustered trips table, zone lookup, weather table
5. **Create view** — `trips_enriched` joins trips + borough names + weather by date

## Key Design Decisions

- **Raw + processed layers in GCS** — Preserves original data for reprocessing; processed files are schema-aligned Parquet
- **Partitioning by pickup date** — Reduces BigQuery query cost and latency for time-scoped queries
- **Clustering by VendorID, PULocationID, payment_type, passenger_count** — Optimizes the most common analytical filters
- **WRITE_TRUNCATE for full run, WRITE_APPEND for demo** — Full pipeline is idempotent; demo run is incremental
- **Schema evolution handling** — `ALLOW_FIELD_ADDITION` accommodates the `airport_fee` column present in Feb but not Jan
- **Weather enrichment** — External API data joined with trip data to enable weather impact analysis

## Technologies

- **Python** — pandas, pyarrow, google-cloud-storage, google-cloud-bigquery, requests
- **GCP** — Google Cloud Storage, BigQuery, IAM
- **Terraform** — Infrastructure as Code for GCS bucket and BigQuery dataset
- **Tableau Desktop** — Live dashboard connected to BigQuery
- **uv** — Python package management
