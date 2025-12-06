import os
from pathlib import Path
from google.cloud import storage
from google.cloud.storage import Bucket
from src.config.constants import KEY_PATH, GCS_BUCKET_NAME, RAW_DATA_PATH, RAW_FILES
from src.config import runtime_config
import logging

def main():
    # ---------- 0. Setup ----------
    logger = logging.getLogger(__name__)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_PATH

    # ---------- 1. Build GCS client ----------
    client = storage.Client()
    bucket = client.get_bucket(GCS_BUCKET_NAME)

    # ---------- 2. Upload to GCS ----------
    def upload_to_gcs(local_file: str, table_name: str, bucket: Bucket) -> None:
        """
        Upload a local Parquet file to GCS using the path structure:
        gs://<bucket>/raw/<table_name>/run_date=<run_date>/<file>
        """
        # Blob path in GCS
        gcs_blob = f"raw/{table_name}/run_date={runtime_config.RUN_DATE}/{os.path.basename(local_file)}"

        blob = bucket.blob(gcs_blob)
        blob.upload_from_filename(local_file)
        logger.info(f"✅ Uploaded : gs://{GCS_BUCKET_NAME}/{gcs_blob}")

    # ---------- 3. Upload all tables ----------
    for table_name, filename in RAW_FILES.items():
        local_file = Path(RAW_DATA_PATH) / table_name / f"run_date={runtime_config.RUN_DATE}" / filename
        upload_to_gcs(str(local_file), table_name, bucket)
