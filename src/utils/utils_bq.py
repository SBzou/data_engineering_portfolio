from google.cloud import bigquery

def ensure_dataset(client: bigquery.Client, dataset_id: str, region: str) -> bigquery.Dataset:
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = region
    client.create_dataset(dataset, exists_ok=True)
    return dataset

def clean_category_revenue(client: bigquery.Client, dataset_id: str) -> None:
    query = f"""
    CREATE OR REPLACE TABLE `{dataset_id}.category_revenue_clean` AS
    SELECT
      category,
      SAFE_CAST(category_revenue AS FLOAT64) AS category_revenue
    FROM `{dataset_id}.category_revenue`;
    """
    client.query(query).result()
