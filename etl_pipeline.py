import pandas as pd
from sqlalchemy import create_engine, text
import logging
from datetime import datetime

print("FILE IS RUNNING")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('pipeline.log')
    ]
)
log = logging.getLogger(__name__)

DB_USER = "postgres"
DB_PASSWORD = "esraa12"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "Project"
DATA_PATH = r"D:\Project DEPI\data\raw_source"


def get_engine():
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)


def step1_load_staging(engine):
    log.info("Step 1: Loading CSV files into staging")
    files = {
        "customers": "olist_customers_dataset.csv",
        "orders": "olist_orders_dataset.csv",
        "order_items": "olist_order_items_dataset.csv",
        "products": "olist_products_dataset.csv",
        "sellers": "olist_sellers_dataset.csv",
        "order_payments": "olist_order_payments_dataset.csv",
        "order_reviews": "olist_order_reviews_dataset.csv",
        "category_translation": "product_category_name_translation.csv",
    }
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging"))
    for table_name, file_name in files.items():
        df = pd.read_csv(f"{DATA_PATH}/{file_name}", low_memory=False)
        df.to_sql(name=table_name, con=engine, schema="staging", if_exists="replace", index=False)
        log.info(f"  staging.{table_name} -> {len(df):,} rows")
    log.info("Step 1 done\n")


def step2_clean_data(engine):
    log.info("Step 2: Cleaning data")
    with engine.begin() as conn:
        conn.execute(text("""
                          DELETE
                          FROM staging.orders
                          WHERE ctid NOT IN (SELECT MIN(ctid) FROM staging.orders GROUP BY order_id)
                          """))
        conn.execute(text("DELETE FROM staging.orders WHERE order_purchase_timestamp IS NULL"))
        conn.execute(text("UPDATE staging.order_items SET price = ABS(price) WHERE price < 0"))
        conn.execute(text("UPDATE staging.order_items SET freight_value = ABS(freight_value) WHERE freight_value < 0"))
        conn.execute(text("ALTER TABLE staging.order_items ADD COLUMN IF NOT EXISTS total_amount NUMERIC"))
        conn.execute(text("UPDATE staging.order_items SET total_amount = price + freight_value"))

        # Correcting typos: product_name_lenght -> product_name_length
        conn.execute(text("ALTER TABLE staging.products ADD COLUMN IF NOT EXISTS product_name_length INT"))
        conn.execute(text("""
                          UPDATE staging.products
                          SET product_name_length = product_name_length
                          WHERE product_name_length IS NULL
                          """))

        # Correcting typos: product_description_lenght -> product_description_length
        conn.execute(text("ALTER TABLE staging.products ADD COLUMN IF NOT EXISTS product_description_length INT"))
        conn.execute(text("""
                          UPDATE staging.products
                          SET product_description_length = product_description_length
                          WHERE product_description_length IS NULL
                          """))

        conn.execute(
            text("ALTER TABLE staging.products ADD COLUMN IF NOT EXISTS product_category_name_english VARCHAR(100)"))
        conn.execute(text("""
                          INSERT INTO staging.category_translation (product_category_name, product_category_name_english)
                          VALUES ('pc_gamer', 'Gaming PC'),
                                 ('portateis_cozinha_e_preparadores_de_alimentos',
                                  'Kitchen Appliances') ON CONFLICT DO NOTHING
                          """))
        conn.execute(text("""
                          UPDATE staging.products p
                          SET product_category_name_english = t.product_category_name_english FROM staging.category_translation t
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
    log.info("Step 2 done\n")


def step3_fill_dimensions(engine):
    log.info("Step 3: Filling Dimension Tables")
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS dw"))
        conn.execute(text("""
                          CREATE TABLE IF NOT EXISTS dw.dim_customers
                          (
                              customer_id
                              VARCHAR
                          (
                              50
                          ) PRIMARY KEY,
                              customer_unique_id VARCHAR
                          (
                              50
                          ),
                              customer_zip_code_prefix VARCHAR
                          (
                              20
                          ),
                              customer_city VARCHAR
                          (
                              100
                          ),
                              customer_state VARCHAR
                          (
                              10
                          )
                              )
                          """))
        conn.execute(text("""
                          CREATE TABLE IF NOT EXISTS dw.dim_products
                          (
                              product_id
                              VARCHAR
                          (
                              50
                          ) PRIMARY KEY,
                              product_category_name VARCHAR
                          (
                              100
                          ),
                              product_category_name_english VARCHAR
                          (
                              100
                          ),
                              product_name_length INT,
                              product_description_length INT,
                              product_photos_qty INT,
                              product_weight_g NUMERIC,
                              product_length_cm NUMERIC,
                              product_height_cm NUMERIC,
                              product_width_cm NUMERIC
                              )
                          """))
        conn.execute(text("""
                          CREATE TABLE IF NOT EXISTS dw.dim_sellers
                          (
                              seller_id
                              VARCHAR
                          (
                              50
                          ) PRIMARY KEY,
                              seller_zip_code_prefix VARCHAR
                          (
                              20
                          ),
                              seller_city VARCHAR
                          (
                              100
                          ),
                              seller_state VARCHAR
                          (
                              10
                          )
                              )
                          """))
        conn.execute(text("""
                          CREATE TABLE IF NOT EXISTS dw.dim_date
                          (
                              date_key
                              INT
                              PRIMARY
                              KEY,
                              full_date
                              DATE
                              NOT
                              NULL,
                              year
                              INT
                              NOT
                              NULL,
                              quarter
                              INT
                              NOT
                              NULL,
                              month
                              INT
                              NOT
                              NULL,
                              month_name
                              VARCHAR
                          (
                              20
                          ) NOT NULL,
                              day INT NOT NULL,
                              day_of_week INT NOT NULL,
                              day_name VARCHAR
                          (
                              20
                          ) NOT NULL,
                              is_weekend BOOLEAN NOT NULL
                              )
                          """))
        conn.execute(text("""
                          CREATE TABLE IF NOT EXISTS dw.fact_order_items
                          (
                              order_id
                              VARCHAR
                          (
                              50
                          ),
                              order_item_id INT,
                              customer_id VARCHAR
                          (
                              50
                          ),
                              product_id VARCHAR
                          (
                              50
                          ),
                              seller_id VARCHAR
                          (
                              50
                          ),
                              order_purchase_date_key INT,
                              order_status VARCHAR
                          (
                              50
                          ),
                              shipping_limit_date TIMESTAMP,
                              price NUMERIC
                          (
                              10,
                              2
                          ),
                              freight_value NUMERIC
                          (
                              10,
                              2
                          ),
                              total_amount NUMERIC
                          (
                              10,
                              2
                          ),
                              PRIMARY KEY
                          (
                              order_id,
                              order_item_id
                          )
                              )
                          """))
        conn.execute(text("""
                          CREATE TABLE IF NOT EXISTS dw.fact_order_payments
                          (
                              order_id
                              VARCHAR
                          (
                              50
                          ),
                              payment_sequential INT,
                              order_purchase_date_key INT,
                              payment_type VARCHAR
                          (
                              50
                          ),
                              payment_installments INT,
                              payment_value NUMERIC
                          (
                              10,
                              2
                          ),
                              PRIMARY KEY
                          (
                              order_id,
                              payment_sequential
                          )
                              )
                          """))
        conn.execute(text("""
                          CREATE TABLE IF NOT EXISTS dw.fact_order_reviews
                          (
                              review_id
                              VARCHAR
                          (
                              100
                          ) PRIMARY KEY,
                              order_id VARCHAR
                          (
                              50
                          ),
                              order_purchase_date_key INT,
                              review_score INT,
                              review_comment_title TEXT,
                              review_comment_message TEXT,
                              review_creation_date TIMESTAMP,
                              review_answer_timestamp TIMESTAMP
                              )
                          """))

        conn.execute(text("TRUNCATE TABLE dw.dim_customers CASCADE"))
        conn.execute(text("""
                          INSERT INTO dw.dim_customers
                          (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
                          SELECT DISTINCT customer_id,
                                          customer_unique_id,
                                          customer_zip_code_prefix,
                                          customer_city,
                                          customer_state
                          FROM staging.customers
                          """))
        log.info("  dim_customers done")

        conn.execute(text("TRUNCATE TABLE dw.dim_products CASCADE"))
        conn.execute(text("""
                          INSERT INTO dw.dim_products
                          (product_id, product_category_name, product_category_name_english,
                           product_name_length, product_description_length, product_photos_qty,
                           product_weight_g, product_length_cm, product_height_cm, product_width_cm)
                          SELECT DISTINCT product_id,
                                          product_category_name,
                                          product_category_name_english,
                                          product_name_length,
                                          product_description_length,
                                          product_photos_qty,
                                          product_weight_g,
                                          product_length_cm,
                                          product_height_cm,
                                          product_width_cm
                          FROM staging.products
                          """))
        log.info("  dim_products done")

        conn.execute(text("TRUNCATE TABLE dw.dim_sellers CASCADE"))
        conn.execute(text("""
                          INSERT INTO dw.dim_sellers (seller_id, seller_zip_code_prefix, seller_city, seller_state)
                          SELECT DISTINCT seller_id, seller_zip_code_prefix, seller_city, seller_state
                          FROM staging.sellers
                          """))
        log.info("  dim_sellers done")

        conn.execute(text("TRUNCATE TABLE dw.dim_date CASCADE"))
        conn.execute(text("""
                          INSERT INTO dw.dim_date
                          (date_key, full_date, year, quarter, month, month_name, day, day_of_week, day_name,
                           is_weekend)
                          SELECT DISTINCT TO_CHAR(order_purchase_timestamp::TIMESTAMP::DATE, 'YYYYMMDD')::INT, order_purchase_timestamp::TIMESTAMP::DATE, EXTRACT(YEAR FROM order_purchase_timestamp::TIMESTAMP)::INT, EXTRACT(QUARTER FROM order_purchase_timestamp::TIMESTAMP)::INT, EXTRACT(MONTH FROM order_purchase_timestamp::TIMESTAMP)::INT, TO_CHAR(order_purchase_timestamp::TIMESTAMP, 'Month'),
                                          EXTRACT(DAY FROM order_purchase_timestamp::TIMESTAMP)::INT, EXTRACT(ISODOW FROM order_purchase_timestamp::TIMESTAMP)::INT, TO_CHAR(order_purchase_timestamp::TIMESTAMP, 'Day'),
                                          EXTRACT(ISODOW FROM order_purchase_timestamp::TIMESTAMP) IN (6, 7)
                          FROM staging.orders
                          WHERE order_purchase_timestamp IS NOT NULL
                          """))
        log.info("  dim_date done")
    log.info("Step 3 done\n")


def step4_fill_fact(engine):
    log.info("Step 4: Filling Fact Tables")
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE dw.fact_order_items CASCADE"))
        conn.execute(text("""
                          INSERT INTO dw.fact_order_items
                          (order_id, order_item_id, customer_id, product_id, seller_id,
                           order_purchase_date_key, order_status, shipping_limit_date,
                           price, freight_value, total_amount)
                          SELECT o.order_id,
                                 oi.order_item_id,
                                 o.customer_id,
                                 oi.product_id,
                                 oi.seller_id,
                                 TO_CHAR(o.order_purchase_timestamp::TIMESTAMP::DATE, 'YYYYMMDD')::INT, o.order_status,
                                 oi.shipping_limit_date::TIMESTAMP, oi.price,
                                 oi.freight_value,
                                 oi.total_amount
                          FROM staging.order_items oi
                                   JOIN staging.orders o ON oi.order_id = o.order_id
                          """))
        result = conn.execute(text("SELECT COUNT(*) FROM dw.fact_order_items"))
        log.info(f"  fact_order_items: {result.scalar():,} rows")

        conn.execute(text("TRUNCATE TABLE dw.fact_order_payments CASCADE"))
        conn.execute(text("""
                          INSERT INTO dw.fact_order_payments
                          (order_id, payment_sequential, order_purchase_date_key,
                           payment_type, payment_installments, payment_value)
                          SELECT op.order_id,
                                 op.payment_sequential,
                                 TO_CHAR(o.order_purchase_timestamp::TIMESTAMP::DATE, 'YYYYMMDD')::INT, CASE
                                                                                                            WHEN LOWER(op.payment_type) = 'not_defined'
                                                                                                                THEN 'other'
                                                                                                            ELSE op.payment_type
                              END,
                                 op.payment_installments,
                                 op.payment_value
                          FROM staging.order_payments op
                                   JOIN staging.orders o ON op.order_id = o.order_id
                          WHERE op.payment_value > 0
                          """))
        result = conn.execute(text("SELECT COUNT(*) FROM dw.fact_order_payments"))
        log.info(f"  fact_order_payments: {result.scalar():,} rows")

        conn.execute(text("TRUNCATE TABLE dw.fact_order_reviews CASCADE"))
        conn.execute(text("""
                          INSERT INTO dw.fact_order_reviews
                          (review_id, order_id, order_purchase_date_key,
                           review_score, review_comment_title, review_comment_message,
                           review_creation_date, review_answer_timestamp)
                          SELECT DISTINCT
                          ON (r.review_id)
                              r.review_id,
                              r.order_id,
                              TO_CHAR(o.order_purchase_timestamp:: TIMESTAMP :: DATE, 'YYYYMMDD'):: INT,
                              r.review_score,
                              COALESCE (r.review_comment_title, ''),
                              COALESCE (r.review_comment_message, ''),
                              r.review_creation_date:: TIMESTAMP,
                              r.review_answer_timestamp:: TIMESTAMP
                          FROM staging.order_reviews r
                              JOIN staging.orders o
                          ON r.order_id = o.order_id
                          ORDER BY r.review_id, r.review_creation_date DESC
                          """))
        result = conn.execute(text("SELECT COUNT(*) FROM dw.fact_order_reviews"))
        log.info(f"  fact_order_reviews: {result.scalar():,} rows")
    log.info("Step 4 done\n")


def step5_aggregate(engine):
    log.info("Step 5: Building Aggregate Tables")
    with engine.begin() as conn:
        conn.execute(text("""
                          CREATE TABLE IF NOT EXISTS dw.agg_sales_daily
                          (
                              full_date
                              DATE
                              PRIMARY
                              KEY,
                              total_orders
                              INT,
                              total_items_sold
                              INT,
                              total_revenue
                              NUMERIC
                          (
                              12,
                              2
                          ),
                              avg_order_value NUMERIC
                          (
                              12,
                              2
                          )
                              )
                          """))
        conn.execute(text("TRUNCATE TABLE dw.agg_sales_daily"))
        conn.execute(text("""
                          INSERT INTO dw.agg_sales_daily (full_date, total_orders, total_items_sold, total_revenue,
                                                          avg_order_value)
                          SELECT d.full_date,
                                 COUNT(DISTINCT f.order_id),
                                 COUNT(*),
                                 ROUND(SUM(f.total_amount)::NUMERIC, 2),
                                 ROUND(SUM(f.total_amount) / COUNT(DISTINCT f.order_id), 2)
                          FROM dw.fact_order_items f
                                   JOIN dw.dim_date d ON f.order_purchase_date_key = d.date_key
                          GROUP BY d.full_date
                          ORDER BY d.full_date
                          """))
        result = conn.execute(text("SELECT COUNT(*) FROM dw.agg_sales_daily"))
        log.info(f"  agg_sales_daily: {result.scalar():,} days")

        conn.execute(text("""
                          CREATE TABLE IF NOT EXISTS dw.agg_sales_monthly
                          (
                              year
                              INT,
                              month
                              INT,
                              month_name
                              VARCHAR
                          (
                              20
                          ),
                              total_orders INT,
                              total_items_sold INT,
                              total_revenue NUMERIC
                          (
                              12,
                              2
                          ),
                              avg_order_value NUMERIC
                          (
                              12,
                              2
                          ),
                              PRIMARY KEY
                          (
                              year,
                              month
                          )
                              )
                          """))
        conn.execute(text("TRUNCATE TABLE dw.agg_sales_monthly"))
        conn.execute(text("""
                          INSERT INTO dw.agg_sales_monthly (year, month, month_name, total_orders, total_items_sold,
                                                            total_revenue, avg_order_value)
                          SELECT d.year,
                                 d.month,
                                 TRIM(MIN(d.month_name)),
                                 COUNT(DISTINCT f.order_id),
                                 COUNT(*),
                                 ROUND(SUM(f.total_amount)::NUMERIC, 2),
                                 ROUND(SUM(f.total_amount) / COUNT(DISTINCT f.order_id), 2)
                          FROM dw.fact_order_items f
                                   JOIN dw.dim_date d ON f.order_purchase_date_key = d.date_key
                          GROUP BY d.year, d.month
                          ORDER BY d.year, d.month
                          """))
        result = conn.execute(text("SELECT COUNT(*) FROM dw.agg_sales_monthly"))
        log.info(f"  agg_sales_monthly: {result.scalar()} months")

        conn.execute(text("DROP TABLE IF EXISTS dw.agg_inventory"))
        conn.execute(text("""
                          CREATE TABLE dw.agg_inventory
                          (
                              product_id                    VARCHAR(50) PRIMARY KEY,
                              product_category_name_english VARCHAR(100),
                              total_units_sold              INT,
                              total_orders                  INT,
                              total_revenue                 NUMERIC(12, 2),
                              avg_price                     NUMERIC(10, 2),
                              stock_qty                     INT,
                              reorder_level                 INT,
                              stock_status                  VARCHAR(20)
                          )
                          """))
        conn.execute(text("""
                          INSERT INTO dw.agg_inventory (product_id, product_category_name_english, total_units_sold,
                                                        total_orders, total_revenue, avg_price, stock_qty,
                                                        reorder_level, stock_status)
                          SELECT sales.product_id,
                                 sales.product_category_name_english,
                                 sales.total_units_sold,
                                 sales.total_orders,
                                 sales.total_revenue,
                                 sales.avg_price,
                                 inv.stock_qty,
                                 inv.reorder_level,
                                 CASE
                                     WHEN inv.stock_qty = 0 THEN 'Out of Stock'
                                     WHEN inv.stock_qty < inv.reorder_level THEN 'Low Stock'
                                     ELSE 'In Stock'
                                     END
                          FROM (SELECT p.product_id,
                                       p.product_category_name_english,
                                       COUNT(*)                               AS total_units_sold,
                                       COUNT(DISTINCT f.order_id)             AS total_orders,
                                       ROUND(SUM(f.total_amount)::NUMERIC, 2) AS total_revenue,
                                       ROUND(AVG(f.price)::NUMERIC, 2)        AS avg_price
                                FROM dw.fact_order_items f
                                         JOIN dw.dim_products p ON f.product_id = p.product_id
                                GROUP BY p.product_id, p.product_category_name_english) sales
                                   JOIN (SELECT product_id,
                                                MOD(ABS(HASHTEXT(product_id)), 500)     AS stock_qty,
                                                MOD(ABS(HASHTEXT(product_id)), 80) + 20 AS reorder_level
                                         FROM dw.dim_products) inv ON inv.product_id = sales.product_id
                          ORDER BY sales.total_revenue DESC
                          """))
        result = conn.execute(text("SELECT COUNT(*) FROM dw.agg_inventory"))
        log.info(f"  agg_inventory: {result.scalar():,} products")

        conn.execute(text("""
                          CREATE TABLE IF NOT EXISTS dw.agg_payment_methods
                          (
                              payment_type
                              VARCHAR
                          (
                              50
                          ) PRIMARY KEY,
                              total_transactions INT,
                              total_orders INT,
                              total_revenue NUMERIC
                          (
                              12,
                              2
                          ),
                              avg_installments NUMERIC
                          (
                              5,
                              2
                          ),
                              avg_payment_value NUMERIC
                          (
                              10,
                              2
                          )
                              )
                          """))
        conn.execute(text("TRUNCATE TABLE dw.agg_payment_methods"))
        conn.execute(text("""
                          INSERT INTO dw.agg_payment_methods
                          SELECT payment_type,
                                 COUNT(*)                                     AS total_transactions,
                                 COUNT(DISTINCT order_id)                     AS total_orders,
                                 ROUND(SUM(payment_value)::NUMERIC, 2)        AS total_revenue,
                                 ROUND(AVG(payment_installments)::NUMERIC, 2) AS avg_installments,
                                 ROUND(AVG(payment_value)::NUMERIC, 2)        AS avg_payment_value
                          FROM dw.fact_order_payments
                          WHERE payment_value > 0
                          GROUP BY payment_type
                          ORDER BY total_revenue DESC
                          """))
        result = conn.execute(text("SELECT COUNT(*) FROM dw.agg_payment_methods"))
        log.info(f"  agg_payment_methods: {result.scalar()} payment types")

        conn.execute(text("""
                          CREATE TABLE IF NOT EXISTS dw.agg_reviews_monthly
                          (
                              year
                              INT,
                              month
                              INT,
                              total_reviews
                              INT,
                              avg_score
                              NUMERIC
                          (
                              3,
                              2
                          ),
                              score_5 INT,
                              score_4 INT,
                              score_3 INT,
                              score_2 INT,
                              score_1 INT,
                              PRIMARY KEY
                          (
                              year,
                              month
                          )
                              )
                          """))
        conn.execute(text("TRUNCATE TABLE dw.agg_reviews_monthly"))
        conn.execute(text("""
                          INSERT INTO dw.agg_reviews_monthly
                          SELECT d.year,
                                 d.month,
                                 COUNT(*)                               AS total_reviews,
                                 ROUND(AVG(r.review_score)::NUMERIC, 2) AS avg_score,
                                 COUNT(*)                                  FILTER (WHERE r.review_score = 5)     AS score_5, COUNT(*) FILTER (WHERE r.review_score = 4)     AS score_4, COUNT(*) FILTER (WHERE r.review_score = 3)     AS score_3, COUNT(*) FILTER (WHERE r.review_score = 2)     AS score_2, COUNT(*) FILTER (WHERE r.review_score = 1)     AS score_1
                          FROM dw.fact_order_reviews r
                                   JOIN dw.dim_date d ON r.order_purchase_date_key = d.date_key
                          GROUP BY d.year, d.month
                          ORDER BY d.year, d.month
                          """))
        result = conn.execute(text("SELECT COUNT(*) FROM dw.agg_reviews_monthly"))
        log.info(f"  agg_reviews_monthly: {result.scalar()} months")
    log.info("Step 5 done\n")


def run_pipeline():
    start_time = datetime.now()
    log.info(f"Starting ETL Pipeline - {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    try:
        engine = get_engine()
        step1_load_staging(engine)
        step2_clean_data(engine)
        step3_fill_dimensions(engine)
        step4_fill_fact(engine)
        step5_aggregate(engine)
        log.info(f"Pipeline finished successfully! Total time: {datetime.now() - start_time}")
    except Exception as e:
        log.error(f"Error: {e}")
        raise


if __name__ == "__main__":
    run_pipeline()