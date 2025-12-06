import logging
import argparse

from src.config import runtime_config
from src.config.constants import RUN_DATE, VERBOSE

from src.scripts import (
    step1_raw_data_generation,
    step2_gcs_ingestion,
    step3_spark_processing,
    step4_bigquery_loading,
    step5_bigquery_validation
)

from src.scripts.logger import setup_logging

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_date", type=str, default=None)
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()

def main():
    # ---------- 1. Parse CLI arguments ----------
    args = parse_args()

    # ---------- 2. Override constants ----------
    runtime_config.RUN_DATE = args.run_date or RUN_DATE
    runtime_config.VERBOSE = args.verbose if args.verbose else VERBOSE

    # ---------- 3. Setup logging ----------
    setup_logging()
    logger = logging.getLogger(__name__)

    logger.info(f"🏁 Starting pipeline with RUN_DATE={runtime_config.RUN_DATE}, VERBOSE={runtime_config.VERBOSE}")

    # ---------- 4. Pipeline definition ----------
    pipeline = [
        step1_raw_data_generation,
        step2_gcs_ingestion,
        step3_spark_processing,
        step4_bigquery_loading,
        step5_bigquery_validation
    ]

    # ---------- 5. Run pipeline ----------
    for step in pipeline:
        logger.info(f"🚀 Running {step.__name__}")
        try:
            step.main()
        except Exception as e:
            logger.exception(f"❌ Pipeline failed in: {step.__name__}")
            break
        logger.info(f"✅ Completed {step.__name__}")

if __name__ == "__main__":
    main()
