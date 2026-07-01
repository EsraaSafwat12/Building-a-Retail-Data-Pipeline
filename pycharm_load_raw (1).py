from pathlib import Path
import os

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

FILES = [
    ("olist_customers_dataset.csv", "customers"),
    ("olist_orders_dataset.csv", "orders"),
    ("olist_order_items_dataset.csv", "order_items"),
    ("olist_order_payments_dataset.csv", "order_payments"),
    ("olist_order_reviews_dataset.csv", "order_reviews"),
    ("olist_products_dataset.csv", "products"),
    ("olist_sellers_dataset.csv", "sellers"),
    ("olist_geolocation_dataset.csv", "geolocation"),
    ("product_category_name_translation.csv", "product_category_name_translation"),
]


def engine():
    user = os.getenv("PGUSER", "postgres")
    pwd = os.getenv("PGPASSWORD")
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    db = os.getenv("PGDATABASE", "Project")
    admin_db = os.getenv("PGADMIN_DB", "postgres")

    admin_engine = create_engine(
        f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{admin_db}"
    )

    with admin_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        exists = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :db"),
            {"db": db},
        ).first()
        if not exists:
            conn.execute(text(f'CREATE DATABASE "{db}"'))

    return create_engine(f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}")


def main():
    csv_dir = Path(os.getenv("CSV_DIR", r"D:\Project DEPI\data\raw_source")).resolve()
    if not csv_dir.exists():
        raise FileNotFoundError(f"CSV_DIR does not exist: {csv_dir}")

    eng = engine()

    with eng.begin() as conn:
        conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS staging")

    for file_name, table_name in FILES:
        file_path = csv_dir / file_name
        if not file_path.exists():
            raise FileNotFoundError(f"Missing required CSV: {file_path}")

        df = pd.read_csv(file_path, dtype=str)
        df.columns = df.columns.str.strip().str.lower()
        df.to_sql(
            f"{table_name}_raw",
            eng,
            schema="staging",
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=5000,
        )
        print(f"Loaded {file_name} -> staging.{table_name}_raw")

    print("All Data Loaded")


if __name__ == "__main__":
    main()