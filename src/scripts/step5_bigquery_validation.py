import os
from google.cloud import bigquery
from decimal import Decimal, getcontext
from typing import List, Dict, Any, Union
from src.config.constants import KEY_PATH, PROJECT_ID, BQ_DATASET_NAME
from src.config import runtime_config
import logging

Number = Union[int, float, str, Decimal]

def main():
    # ---------- 0. Setup ----------
    logger = logging.getLogger(__name__)

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_PATH
    client = bigquery.Client(project=PROJECT_ID)
    dataset_id = f"{PROJECT_ID}.{BQ_DATASET_NAME}"

    EPSILON = Decimal("0.01")
    getcontext().prec = 6  # precision

    def run_query(query: str) -> List[Dict[str, Any]]:
        query_job = client.query(query)
        return [dict(row) for row in query_job.result()]

    def check_diff(a: Number, b: Number) -> bool:
        return abs(Decimal(a) - Decimal(b)) <= EPSILON

    def run_test(name: str, query: str, check_fn=None, verbose: bool=runtime_config.VERBOSE) -> List[Dict[str, Any]]:
        result = run_query(query)
    
        if check_fn and not check_fn(result):
                logger.error(f"❌ {name} failed: {result}")
                raise AssertionError(f"{name} failed: {result}")
        if verbose:
            logger.info(f"✅ {name} : {result or None}")

        return result

    # ---------- 1. Orders & quantities ----------
    run_test(
        "Test 1: Orders & quantities",
        f"""
        SELECT
            COUNT(DISTINCT order_id) AS n_orders,
            SUM(order_quantity) AS total_quantity
        FROM `{dataset_id}.orders_enriched`
        """,
        lambda r: r[0]["n_orders"] <= r[0]["total_quantity"]
    )

    # ---------- 2. Customers & products existence ----------
    run_test(
        "Test 2: Customers & products existence",
        f"""
        SELECT
            COUNT(DISTINCT customer_id) AS n_customers,
            COUNT(DISTINCT product_id) AS n_products
        FROM `{dataset_id}.orders_enriched`
        """,
        lambda r: r[0]["n_customers"] > 0 and r[0]["n_products"] > 0
    )

    # ---------- 3. Product name consistency ----------
    run_test(
        "Test 3: Product name consistency",
        f"""
        SELECT product_id, COUNT(DISTINCT product_name) AS cnt
        FROM `{dataset_id}.orders_enriched`
        GROUP BY product_id
        HAVING cnt > 1
        """,
        lambda r: len(r) == 0
    )

    # ---------- 4. Missing joins ----------
    run_test(
        "Test 4: Missing customers/products",
        f"""
        SELECT COUNT(*) AS missing
        FROM `{dataset_id}.orders_enriched`
        WHERE customer_id IS NULL OR product_id IS NULL
        """,
        lambda r: r[0]["missing"] == 0
    )

    # ---------- 5. Duplicate orders ----------
    run_test(
        "Test 5: Duplicate orders",
        f"""
        SELECT order_id, COUNT(*) AS cnt
        FROM `{dataset_id}.orders_enriched`
        GROUP BY order_id
        HAVING cnt > 1
        """,
        lambda r: len(r) == 0
    )

    # ---------- 6. Global revenue consistency ----------
    run_test(
        "Test 6: Global revenue consistency",
        f"""
        SELECT
            (SELECT SUM(total_amount) FROM `{dataset_id}.orders_enriched`) AS orders_total,
            (SELECT SUM(total_revenue) FROM `{dataset_id}.customers_revenue`) AS customers_total,
            (SELECT SUM(total_revenue) FROM `{dataset_id}.products_sales`) AS products_total
        """,
        lambda r: check_diff(r[0]["orders_total"], r[0]["customers_total"]) and check_diff(r[0]["orders_total"], r[0]["products_total"])
    )

    # ---------- 7. Customer aggregation correctness ----------
    run_test(
        "Test 7: Customer aggregation correctness",
        f"""
        WITH recalculated AS (
            SELECT customer_id, run_date, SUM(total_amount) AS true_revenue
            FROM `{dataset_id}.orders_enriched`
            GROUP BY customer_id, run_date
        )
        SELECT
            SUM(r.true_revenue) AS recomputed,
            SUM(c.total_revenue) AS stored
        FROM recalculated r
        JOIN `{dataset_id}.customers_revenue` c
        ON r.customer_id = c.customer_id
        AND r.run_date = c.run_date
        """,
        lambda r: check_diff(r[0]["recomputed"], r[0]["stored"])
    )

    # ---------- 8. Product aggregation correctness ----------
    run_test(
        "Test 8: Product aggregation correctness",
        f"""
        WITH recalculated AS (
            SELECT product_id, run_date, SUM(order_quantity) AS true_quantity, SUM(total_amount) AS true_revenue
            FROM `{dataset_id}.orders_enriched`
            GROUP BY product_id, run_date
        )
        SELECT
            SUM(r.true_quantity) AS recomputed_qty,
            SUM(p.total_quantity_sold) AS stored_qty,
            SUM(r.true_revenue) AS recomputed_rev,
            SUM(p.total_revenue) AS stored_rev
        FROM recalculated r
        JOIN `{dataset_id}.products_sales` p
        ON r.product_id = p.product_id
        AND r.run_date = p.run_date
        """,
        lambda r: check_diff(r[0]["recomputed_qty"], r[0]["stored_qty"]) and check_diff(r[0]["recomputed_rev"], r[0]["stored_rev"])
    )

    logger.info("✅ All BigQuery Data Validation passed")
