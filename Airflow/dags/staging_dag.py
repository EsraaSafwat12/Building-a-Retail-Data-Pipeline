from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import pandas as pd

DB_USER     = "postgres"
DB_PASSWORD = "YOUR_PASSWORD_HERE"
DB_HOST     = "192.168.1.12"
DB_PORT     = "5432"
DB_NAME     = "Project"
DATA_PATH   = "/opt/airflow/data/raw_source"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

def get_engine():
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)


def task1_load_staging():
    engine = get_engine()
    files = {
        "customers":            "olist_customers_dataset.csv",
        "orders":               "olist_orders_dataset.csv",
        "order_items":          "olist_order_items_dataset.csv",
        "products":             "olist_products_dataset.csv",
        "sellers":              "olist_sellers_dataset.csv",
        "order_payments":       "olist_order_payments_dataset.csv",
        "order_reviews":        "olist_order_reviews_dataset.csv",
        "category_translation": "product_category_name_translation.csv",
    }
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging"))
    for table_name, file_name in files.items():
        df = pd.read_csv(f"{DATA_PATH}/{file_name}", low_memory=False)
        df.to_sql(name=table_name, con=engine, schema="staging", if_exists="replace", index=False)


def task2_clean_staging():
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            DELETE FROM staging.orders
            WHERE ctid NOT IN (SELECT MIN(ctid) FROM staging.orders GROUP BY order_id)
        """))
        conn.execute(text("DELETE FROM staging.orders WHERE order_purchase_timestamp IS NULL"))
        conn.execute(text("UPDATE staging.order_items SET price = ABS(price) WHERE price < 0"))
        conn.execute(text("UPDATE staging.order_items SET freight_value = ABS(freight_value) WHERE freight_value < 0"))
        conn.execute(text("ALTER TABLE staging.order_items ADD COLUMN IF NOT EXISTS total_amount NUMERIC"))
        conn.execute(text("UPDATE staging.order_items SET total_amount = price + freight_value"))
        conn.execute(text("ALTER TABLE staging.products ADD COLUMN IF NOT EXISTS product_name_length INT"))
        conn.execute(text("""
            UPDATE staging.products SET product_name_length = product_name_lenght
            WHERE product_name_length IS NULL
        """))
        conn.execute(text("ALTER TABLE staging.products ADD COLUMN IF NOT EXISTS product_description_length INT"))
        conn.execute(text("""
            UPDATE staging.products SET product_description_length = product_description_lenght
            WHERE product_description_length IS NULL
        """))
        conn.execute(text("ALTER TABLE staging.products ADD COLUMN IF NOT EXISTS product_category_name_english VARCHAR(100)"))
        conn.execute(text("""
            INSERT INTO staging.category_translation (product_category_name, product_category_name_english)
            VALUES
                ('pc_gamer', 'Gaming PC'),
                ('portateis_cozinha_e_preparadores_de_alimentos', 'Kitchen Appliances')
            ON CONFLICT DO NOTHING
        """))
        conn.execute(text("""
            UPDATE staging.products p
            SET product_category_name_english = t.product_category_name_english
            FROM staging.category_translation t
            WHERE p.product_category_name = t.product_category_name
        """))
        conn.execute(text("""
            UPDATE staging.products
            SET product_category_name_english = COALESCE(product_category_name_english, 'Uncategorized')
            WHERE product_category_name_english IS NULL
        """))
        conn.execute(text("""
            UPDATE staging.products
            SET product_category_name = COALESCE(product_category_name, 'uncategorized')
            WHERE product_category_name IS NULL
        """))


with DAG(
    dag_id="staging_pipeline",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    catchup=False,
    description="Staging Pipeline - Load and Clean",
) as dag:
    t1 = PythonOperator(task_id="load_raw_to_staging", python_callable=task1_load_staging)
    t2 = PythonOperator(task_id="clean_transform_staging", python_callable=task2_clean_staging)
    trigger_dw = TriggerDagRunOperator(
        task_id="trigger_dw_pipeline",
        trigger_dag_id="dw_pipeline",
    )

    t1 >> t2 >> trigger_dw