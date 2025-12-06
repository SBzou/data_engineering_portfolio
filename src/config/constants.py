from pathlib import Path
from datetime import date

# ---------- Run Configuration ----------
#RUN_DATE = str(date.today())
RUN_DATE = "2025-12-01"
VERBOSE = False

# ---------- Project Paths ----------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DATA_PATH = str((PROJECT_ROOT / "data" / "raw").resolve())
KEY_PATH = str((PROJECT_ROOT / "key" / "service_account.json").resolve())
JAR_GCS_CONNECTOR = str((PROJECT_ROOT / "jars" / "gcs-connector-hadoop3-latest.jar").resolve())

# ---------- GCP Configuration ----------
PROJECT_NAME = "data-engineering-portfolio"
PROJECT_ID = "data-portfolio-sami"
REGION = "europe-west9"

# ---------- GCS Configuration ----------
GCS_BUCKET_NAME = "data-engineering-portfolio-bucket"
RAW_PREFIX = f"gs://{GCS_BUCKET_NAME}/raw/"
PROCESSED_PREFIX = f"gs://{GCS_BUCKET_NAME}/processed/"

# ---------- BigQuery Configuration ----------
BQ_DATASET_NAME = "processed_data"

# ---------- Raw Files ----------
RAW_FILES = {
    "customers": "customers.parquet",
    "products": "products.parquet",
    "orders": "orders.parquet",
    "payments": "payments.parquet"
}

# ---------- Processed Tables ----------
PROCESSED_TABLES = [
    "orders_enriched",
    "customers_revenue",
    "products_sales",
    "category_revenue",
    "payments_summary"
]
