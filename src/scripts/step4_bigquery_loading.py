import os
from google.cloud import bigquery
from src.config.constants import (
    KEY_PATH,
    PROJECT_ID,
    BQ_DATASET_NAME,
    REGION,
    PROCESSED_PREFIX,
    PROCESSED_TABLES,
)
from src.config import runtime_config
import logging

def main():
    # ---------- 0. Setup ----------
    logger = logging.getLogger(__name__)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_PATH

    # ---------- 1. BigQuery client ----------
    bq_client = bigquery.Client(project=PROJECT_ID)

    # ---------- 2. Dataset ----------
    dataset_id = f"{PROJECT_ID}.{BQ_DATASET_NAME}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = REGION
    bq_client.create_dataset(dataset, exists_ok=True)

    # ---------- 3. Write Disposition Rules ----------
    WRITE_MODES = {
        "orders_enriched": "WRITE_APPEND",   # <- Fact table
        "customers_revenue": "WRITE_APPEND",
        "products_sales": "WRITE_APPEND",
        "category_revenue": "WRITE_APPEND",
        "payments_summary": "WRITE_APPEND",
    }

    # ---------- 4. Load processed tables ----------
    for table_name in PROCESSED_TABLES:

        gcs_uri = f"{PROCESSED_PREFIX}{table_name}/run_date={runtime_config.RUN_DATE}/*.parquet"
        table_id = f"{dataset_id}.{table_name}"

        write_mode = WRITE_MODES.get(table_name, "WRITE_TRUNCATE")

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            autodetect=True,
            write_disposition=write_mode
        )

        load_job = bq_client.load_table_from_uri(
            gcs_uri,
            table_id,
            job_config=job_config
        )
        load_job.result()

        logger.info(f"✅ Loaded {table_name} into BigQuery")

    # ---------- 5. Schema Cleanup for category_revenue ----------
    final_table_name = "category_revenue_clean"
    source_table_name = "category_revenue"

    clean_copy_query = f"""
    CREATE OR REPLACE TABLE `{dataset_id}.{final_table_name}` AS
    SELECT
    category,
    SAFE_CAST(category_revenue AS FLOAT64) AS category_revenue
    FROM `{dataset_id}.{source_table_name}`;
    """

    query_job = bq_client.query(clean_copy_query)
    query_job.result()

    logger.info(f"✅ Clean table created: {final_table_name}")
