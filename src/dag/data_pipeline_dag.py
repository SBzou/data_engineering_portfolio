from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import subprocess

default_args = {
    'owner': 'SB',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_script(script_name):
    subprocess.run(["python", f"./scripts/{script_name}"], check=True)

with DAG(
    'data_engineering_pipeline',
    default_args=default_args,
    description='ETL pipeline SB project',
    schedule_interval=None,
    start_date=datetime(2025, 11, 21),
    catchup=False,
) as dag:

    t1 = PythonOperator(task_id='raw_data_generation', python_callable=lambda: run_script("step1_raw_data_generation.py"))
    t2 = PythonOperator(task_id='gcs_ingestion', python_callable=lambda: run_script("step2_gcs_ingestion.py"))
    t3 = PythonOperator(task_id='spark_processing', python_callable=lambda: run_script("step3_spark_processing.py"))
    t4 = PythonOperator(task_id='bigquery_loading', python_callable=lambda: run_script("step4_big_query_loading.py"))
    t5 = PythonOperator(task_id='bigquery_validation', python_callable=lambda: run_script("step5_big_query_validation.py"))

    t1 >> t2 >> t3 >> t4 >> t5
