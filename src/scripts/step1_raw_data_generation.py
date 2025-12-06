from src.config.constants import RAW_DATA_PATH, RAW_FILES
from src.config import runtime_config

from pathlib import Path

import pandas as pd
import random
from faker import Faker
from datetime import timedelta

import logging

def main():
    # ---------- 0. Setup ----------
    logger = logging.getLogger(__name__)

    def build_local_path(table_name: str) -> Path:
        path = Path(RAW_DATA_PATH) / table_name / f"run_date={runtime_config.RUN_DATE}"
        path.mkdir(parents=True, exist_ok=True)

        return path

    def write_parquet(df: pd.DataFrame, table_name: str) -> None:
        local_path = build_local_path(table_name)
        file_path = local_path / RAW_FILES[table_name]

        df.to_parquet(str(file_path), index=False)


    fake = Faker()

    # ---------- 1. Customers ----------
    num_customers = 50
    customers = [
        {
            "customer_id": f"C{i:03d}",
            "name": fake.name(),
            "email": fake.email(),
            "country": fake.country(),
            "signup_date": fake.date_between(start_date="-2y", end_date="today")
        }
        for i in range(1, num_customers + 1)
    ]
    df_customers = pd.DataFrame(customers)
    write_parquet(df_customers, "customers")

    # ---------- 2. Products ----------
    categories = ["Electronics", "Clothing", "Books", "Sports"]
    num_products = 20
    products = []

    for i in range(1, num_products + 1):
        category = categories[i % len(categories)]
        name = f"{category} Product {i:02d}" 
        cost = round(random.uniform(5, 50), 2)
        price = round(random.uniform(20, 200), 2)
        products.append({
            "product_id": f"P{i:03d}",
            "category": category,
            "name": name,
            "cost": cost,
            "price": price
        })

    df_products = pd.DataFrame(products)
    write_parquet(df_products, "products")

    # ---------- 3. Orders ----------
    num_orders = 200
    orders = []

    for i in range(1, num_orders + 1):
        product = random.choice(products)
        customer = random.choice(customers)
        quantity = random.randint(1, 5)
        orders.append({
            "order_id": f"O{runtime_config.RUN_DATE.replace('-', '')}{i:03d}",
            "order_date": fake.date_between(start_date="-1y", end_date="today"),
            "run_date": runtime_config.RUN_DATE,
            "customer_id": customer["customer_id"],
            "product_id": product["product_id"],
            "quantity": quantity,
            "price": product["price"],
            "country": customer["country"]
        })
    df_orders = pd.DataFrame(orders)
    write_parquet(df_orders, "orders")

    # ---------- 4. Payments ----------
    payments = [
        {
            "payment_id": f"PAY{i:03d}",
            "order_id": o["order_id"],
            "payment_method": random.choice(["Credit Card", "PayPal", "Bank Transfer"]),
            "payment_date": o["order_date"] + timedelta(days=random.randint(0, 2)),
            "amount": round(o["quantity"] * o["price"], 2)
        }
        for i, o in enumerate(orders, start=1)
    ]
    df_payments = pd.DataFrame(payments)
    write_parquet(df_payments, "payments")

    logger.info("✅ Local Parquet files generated successfully.")
