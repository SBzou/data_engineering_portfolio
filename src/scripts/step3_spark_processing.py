import os
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType
from pyspark.sql.functions import col, sum as _sum, count, lit
from src.config.constants import (
    KEY_PATH, 
    JAR_GCS_CONNECTOR, 
    RAW_PREFIX, 
    RAW_FILES,
    PROCESSED_PREFIX
)
from src.config import runtime_config
import logging

def main():
    # ---------- 0. Setup ----------
    logger = logging.getLogger(__name__)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_PATH

    # ---------- 1. Spark session ----------
    spark = SparkSession.builder \
        .appName("DataEngineeringPortfolio") \
        .config("spark.jars", JAR_GCS_CONNECTOR) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")  #Remove log messages

    # ---------- 2. Hadoop config ----------
    hconf = spark._jsc.hadoopConfiguration()
    hconf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    hconf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    hconf.set("google.cloud.auth.service.account.enable", "true")
    hconf.set("google.cloud.auth.service.account.json.keyfile", KEY_PATH)

    # ---------- 3. Paths ----------
    paths = {
        f: f"{RAW_PREFIX}{f}/run_date={runtime_config.RUN_DATE}/*.parquet"
        for f in RAW_FILES
    }
    # ---------- 4. Load data ----------
    customers_df = spark.read.option("header", True).parquet(paths["customers"]).alias("c")
    products_df = spark.read.option("header", True).parquet(paths["products"]).alias("p")
    orders_df = spark.read.option("header", True).parquet(paths["orders"]).alias("o")
    payments_df = spark.read.option("header", True).parquet(paths["payments"])

    logger.info("✅ Raw Parquet files loaded from GCS")

    # ---------- 4. Rename columns to avoid duplicates and enforce uniqueness ----------
    orders_clean = orders_df \
        .dropDuplicates(["order_id"]) \
        .withColumnRenamed("quantity", "order_quantity") \
        .withColumnRenamed("price", "order_price") \
        .withColumnRenamed("country", "order_country")

    customers_clean = customers_df \
        .dropDuplicates(["customer_id"]) \
        .withColumnRenamed("name", "customer_name") \
        .withColumnRenamed("country", "customer_country")

    products_clean = products_df \
        .dropDuplicates(["product_id"]) \
        .withColumnRenamed("name", "product_name")

    # ---------- 5. Enrich orders with customer and product info ----------
    orders_enriched = orders_clean.alias("o") \
        .join(customers_clean.alias("c"), on="customer_id", how="left") \
        .join(products_clean.alias("p"), on="product_id", how="left") \
        .withColumn("order_quantity", col("order_quantity").cast("double")) \
        .withColumn("order_price", col("order_price").cast("double")) \
        .withColumn("total_amount", (col("order_quantity") * col("order_price")).cast(DecimalType(18,2))) \
        .withColumn("run_date", lit(runtime_config.RUN_DATE))

    # ---------- 6. Aggregate revenue per customer ----------
    customers_revenue = (
        orders_enriched
        .groupBy("customer_id", "customer_name", "run_date")
        .agg(_sum(col("total_amount").cast(DecimalType(18,2))).alias("total_revenue"))
    )

    # ---------- 7. Aggregate sales per product ----------
    products_sales = (
        orders_enriched
        .groupBy("product_id", "product_name", "category", "run_date")
        .agg(
            _sum("order_quantity").alias("total_quantity_sold"),
            _sum("total_amount").alias("total_revenue")
        )
    )

    # ---------- 8. Aggregate revenue per category ----------
    category_revenue = orders_enriched \
        .groupBy("category", "run_date") \
        .agg(_sum("total_amount").alias("category_revenue"))
        
    # ---------- 9. Aggregate payment summary ----------
    payments_summary = payments_df.withColumn("amount", col("amount").cast("double")) \
        .withColumn("run_date", lit(runtime_config.RUN_DATE)) \
        .groupBy("run_date", "payment_method") \
        .agg(
            _sum("amount").alias("total_amount"),
            count("amount").alias("total_count")
        )

    logger.info("✅ Spark aggregations complete")

    # ---------- 10. Save processed tables in GCS ----------
    outputs = {
        "orders_enriched": orders_enriched,
        "customers_revenue": customers_revenue,
        "products_sales": products_sales,
        "category_revenue": category_revenue,
        "payments_summary": payments_summary
    }
        
    for name, df in outputs.items():
        output_path = f"{PROCESSED_PREFIX}{name}/run_date={runtime_config.RUN_DATE}/"
        df.write.option("header", True).mode("overwrite").parquet(output_path)
        logger.info(f"✅ Saved {name} → {output_path}")
        
    logger.info("✅ All processed Parquet files saved successfully.")
