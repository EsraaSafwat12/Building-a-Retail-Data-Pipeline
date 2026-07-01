from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

DB_USER     = "postgres"
DB_PASSWORD = "esraa12"
DB_HOST     = "192.168.1.12"
DB_PORT     = "5432"
DB_NAME     = "Project"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

def get_engine():
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)


def task3_build_dimensions():
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS dw"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dw.dim_customers (
                customer_id              VARCHAR(50) PRIMARY KEY,
                customer_unique_id       VARCHAR(50),
                customer_zip_code_prefix VARCHAR(20),
                customer_city            VARCHAR(100),
                customer_state           VARCHAR(10)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dw.dim_products (
                product_id                    VARCHAR(50) PRIMARY KEY,
                product_category_name         VARCHAR(100),
                product_category_name_english VARCHAR(100),
                product_name_length           INT,
                product_description_length    INT,
                product_photos_qty            INT,
                product_weight_g              NUMERIC,
                product_length_cm             NUMERIC,
                product_height_cm             NUMERIC,
                product_width_cm              NUMERIC
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dw.dim_sellers (
                seller_id               VARCHAR(50) PRIMARY KEY,
                seller_zip_code_prefix  VARCHAR(20),
                seller_city             VARCHAR(100),
                seller_state            VARCHAR(10)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dw.dim_date (
                date_key    INT PRIMARY KEY,
                full_date   DATE NOT NULL,
                year        INT NOT NULL,
                quarter     INT NOT NULL,
                month       INT NOT NULL,
                month_name  VARCHAR(20) NOT NULL,
                day         INT NOT NULL,
                day_of_week INT NOT NULL,
                day_name    VARCHAR(20) NOT NULL,
                is_weekend  BOOLEAN NOT NULL
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dw.fact_order_items (
                order_id                VARCHAR(50),
                order_item_id           INT,
                customer_id             VARCHAR(50),
                product_id              VARCHAR(50),
                seller_id               VARCHAR(50),
                order_purchase_date_key INT,
                order_status            VARCHAR(50),
                shipping_limit_date     TIMESTAMP,
                price                   NUMERIC(10,2),
                freight_value           NUMERIC(10,2),
                total_amount            NUMERIC(10,2),
                PRIMARY KEY (order_id, order_item_id)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dw.fact_order_payments (
                order_id                VARCHAR(50),
                payment_sequential      INT,
                order_purchase_date_key INT,
                payment_type            VARCHAR(50),
                payment_installments    INT,
                payment_value           NUMERIC(10,2),
                PRIMARY KEY (order_id, payment_sequential)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dw.fact_order_reviews (
                review_id               VARCHAR(100) PRIMARY KEY,
                order_id                VARCHAR(50),
                order_purchase_date_key INT,
                review_score            INT,
                review_comment_title    TEXT,
                review_comment_message  TEXT,
                review_creation_date    TIMESTAMP,
                review_answer_timestamp TIMESTAMP
            )
        """))
        conn.execute(text("TRUNCATE TABLE dw.dim_customers CASCADE"))
        conn.execute(text("""
            INSERT INTO dw.dim_customers
                (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
            SELECT DISTINCT customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state
            FROM staging.customers
        """))
        conn.execute(text("TRUNCATE TABLE dw.dim_products CASCADE"))
        conn.execute(text("""
            INSERT INTO dw.dim_products
                (product_id, product_category_name, product_category_name_english,
                 product_name_length, product_description_length, product_photos_qty,
                 product_weight_g, product_length_cm, product_height_cm, product_width_cm)
            SELECT DISTINCT
                product_id, product_category_name, product_category_name_english,
                product_name_length, product_description_length, product_photos_qty,
                product_weight_g, product_length_cm, product_height_cm, product_width_cm
            FROM staging.products
        """))
        conn.execute(text("TRUNCATE TABLE dw.dim_sellers CASCADE"))
        conn.execute(text("""
            INSERT INTO dw.dim_sellers (seller_id, seller_zip_code_prefix, seller_city, seller_state)
            SELECT DISTINCT seller_id, seller_zip_code_prefix, seller_city, seller_state
            FROM staging.sellers
        """))
        conn.execute(text("TRUNCATE TABLE dw.dim_date CASCADE"))
        conn.execute(text("""
            INSERT INTO dw.dim_date
                (date_key, full_date, year, quarter, month, month_name, day, day_of_week, day_name, is_weekend)
            SELECT DISTINCT
                TO_CHAR(order_purchase_timestamp::TIMESTAMP::DATE, 'YYYYMMDD')::INT,
                order_purchase_timestamp::TIMESTAMP::DATE,
                EXTRACT(YEAR    FROM order_purchase_timestamp::TIMESTAMP)::INT,
                EXTRACT(QUARTER FROM order_purchase_timestamp::TIMESTAMP)::INT,
                EXTRACT(MONTH   FROM order_purchase_timestamp::TIMESTAMP)::INT,
                TO_CHAR(order_purchase_timestamp::TIMESTAMP, 'Month'),
                EXTRACT(DAY     FROM order_purchase_timestamp::TIMESTAMP)::INT,
                EXTRACT(ISODOW  FROM order_purchase_timestamp::TIMESTAMP)::INT,
                TO_CHAR(order_purchase_timestamp::TIMESTAMP, 'Day'),
                EXTRACT(ISODOW  FROM order_purchase_timestamp::TIMESTAMP) IN (6, 7)
            FROM staging.orders
            WHERE order_purchase_timestamp IS NOT NULL
        """))


def task4_build_fact():
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE dw.fact_order_items CASCADE"))
        conn.execute(text("""
            INSERT INTO dw.fact_order_items
                (order_id, order_item_id, customer_id, product_id, seller_id,
                 order_purchase_date_key, order_status, shipping_limit_date,
                 price, freight_value, total_amount)
            SELECT
                o.order_id, oi.order_item_id, o.customer_id, oi.product_id, oi.seller_id,
                TO_CHAR(o.order_purchase_timestamp::TIMESTAMP::DATE, 'YYYYMMDD')::INT,
                o.order_status,
                oi.shipping_limit_date::TIMESTAMP,
                oi.price, oi.freight_value, oi.total_amount
            FROM staging.order_items oi
            JOIN staging.orders o ON oi.order_id = o.order_id
        """))
        conn.execute(text("TRUNCATE TABLE dw.fact_order_payments CASCADE"))
        conn.execute(text("""
            INSERT INTO dw.fact_order_payments
                (order_id, payment_sequential, order_purchase_date_key,
                 payment_type, payment_installments, payment_value)
            SELECT
                op.order_id, op.payment_sequential,
                TO_CHAR(o.order_purchase_timestamp::TIMESTAMP::DATE, 'YYYYMMDD')::INT,
                CASE WHEN LOWER(op.payment_type) = 'not_defined' THEN 'other' ELSE op.payment_type END,
                op.payment_installments, op.payment_value
            FROM staging.order_payments op
            JOIN staging.orders o ON op.order_id = o.order_id
            WHERE op.payment_value > 0
        """))
        conn.execute(text("TRUNCATE TABLE dw.fact_order_reviews CASCADE"))
        conn.execute(text("""
            INSERT INTO dw.fact_order_reviews
                (review_id, order_id, order_purchase_date_key,
                 review_score, review_comment_title, review_comment_message,
                 review_creation_date, review_answer_timestamp)
            SELECT DISTINCT ON (r.review_id)
                r.review_id, r.order_id,
                TO_CHAR(o.order_purchase_timestamp::TIMESTAMP::DATE, 'YYYYMMDD')::INT,
                r.review_score,
                COALESCE(r.review_comment_title, ''),
                COALESCE(r.review_comment_message, ''),
                r.review_creation_date::TIMESTAMP,
                r.review_answer_timestamp::TIMESTAMP
            FROM staging.order_reviews r
            JOIN staging.orders o ON r.order_id = o.order_id
            ORDER BY r.review_id, r.review_creation_date DESC
        """))


def task5_quality_checks():
    engine = get_engine()
    with engine.begin() as conn:
        checks = {
            "fact_order_items rows":  "SELECT COUNT(*) FROM dw.fact_order_items",
            "null order_id":          "SELECT COUNT(*) FROM dw.fact_order_items WHERE order_id IS NULL",
            "null product_id":        "SELECT COUNT(*) FROM dw.fact_order_items WHERE product_id IS NULL",
            "negative price":         "SELECT COUNT(*) FROM dw.fact_order_items WHERE price < 0",
            "dim_customers rows":     "SELECT COUNT(*) FROM dw.dim_customers",
            "dim_products rows":      "SELECT COUNT(*) FROM dw.dim_products",
            "dim_date rows":          "SELECT COUNT(*) FROM dw.dim_date",
        }
        errors = []
        for check_name, query in checks.items():
            result = conn.execute(text(query)).scalar()
            if "null" in check_name or "negative" in check_name:
                if result > 0:
                    errors.append(f"FAILED: {check_name} = {result}")
            else:
                if result == 0:
                    errors.append(f"FAILED: {check_name} = 0 rows")
        if errors:
            raise ValueError("Quality checks failed:\n" + "\n".join(errors))


def task6_refresh_aggregates():
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dw.agg_sales_daily (
                full_date        DATE PRIMARY KEY,
                total_orders     INT,
                total_items_sold INT,
                total_revenue    NUMERIC(12,2),
                avg_order_value  NUMERIC(12,2)
            )
        """))
        conn.execute(text("TRUNCATE TABLE dw.agg_sales_daily"))
        conn.execute(text("""
            INSERT INTO dw.agg_sales_daily (full_date, total_orders, total_items_sold, total_revenue, avg_order_value)
            SELECT d.full_date, COUNT(DISTINCT f.order_id), COUNT(*),
                ROUND(SUM(f.total_amount)::NUMERIC, 2),
                ROUND(SUM(f.total_amount) / COUNT(DISTINCT f.order_id), 2)
            FROM dw.fact_order_items f
            JOIN dw.dim_date d ON f.order_purchase_date_key = d.date_key
            GROUP BY d.full_date ORDER BY d.full_date
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dw.agg_sales_monthly (
                year INT, month INT, month_name VARCHAR(20),
                total_orders INT, total_items_sold INT,
                total_revenue NUMERIC(12,2), avg_order_value NUMERIC(12,2),
                PRIMARY KEY (year, month)
            )
        """))
        conn.execute(text("TRUNCATE TABLE dw.agg_sales_monthly"))
        conn.execute(text("""
            INSERT INTO dw.agg_sales_monthly (year, month, month_name, total_orders, total_items_sold, total_revenue, avg_order_value)
            SELECT d.year, d.month, TRIM(MIN(d.month_name)),
                COUNT(DISTINCT f.order_id), COUNT(*),
                ROUND(SUM(f.total_amount)::NUMERIC, 2),
                ROUND(SUM(f.total_amount) / COUNT(DISTINCT f.order_id), 2)
            FROM dw.fact_order_items f
            JOIN dw.dim_date d ON f.order_purchase_date_key = d.date_key
            GROUP BY d.year, d.month ORDER BY d.year, d.month
        """))
        conn.execute(text("DROP TABLE IF EXISTS dw.agg_inventory"))
        conn.execute(text("""
            CREATE TABLE dw.agg_inventory (
                product_id VARCHAR(50) PRIMARY KEY,
                product_category_name_english VARCHAR(100),
                total_units_sold INT, total_orders INT,
                total_revenue NUMERIC(12,2), avg_price NUMERIC(10,2),
                stock_qty INT, reorder_level INT, stock_status VARCHAR(20)
            )
        """))
        conn.execute(text("""
            INSERT INTO dw.agg_inventory (
                product_id, product_category_name_english, total_units_sold,
                total_orders, total_revenue, avg_price, stock_qty, reorder_level, stock_status
            )
            SELECT sales.product_id, sales.product_category_name_english,
                sales.total_units_sold, sales.total_orders,
                sales.total_revenue, sales.avg_price,
                inv.stock_qty, inv.reorder_level,
                CASE
                    WHEN inv.stock_qty = 0                 THEN 'Out of Stock'
                    WHEN inv.stock_qty < inv.reorder_level THEN 'Low Stock'
                    ELSE                                        'In Stock'
                END
            FROM (
                SELECT p.product_id, p.product_category_name_english,
                    COUNT(*) AS total_units_sold, COUNT(DISTINCT f.order_id) AS total_orders,
                    ROUND(SUM(f.total_amount)::NUMERIC, 2) AS total_revenue,
                    ROUND(AVG(f.price)::NUMERIC, 2) AS avg_price
                FROM dw.fact_order_items f
                JOIN dw.dim_products p ON f.product_id = p.product_id
                GROUP BY p.product_id, p.product_category_name_english
            ) sales
            JOIN (
                SELECT product_id,
                    MOD(ABS(HASHTEXT(product_id)), 500)     AS stock_qty,
                    MOD(ABS(HASHTEXT(product_id)), 80) + 20 AS reorder_level
                FROM dw.dim_products
            ) inv ON inv.product_id = sales.product_id
            ORDER BY sales.total_revenue DESC
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dw.agg_payment_methods (
                payment_type VARCHAR(50) PRIMARY KEY, total_transactions INT,
                total_orders INT, total_revenue NUMERIC(12,2),
                avg_installments NUMERIC(5,2), avg_payment_value NUMERIC(10,2)
            )
        """))
        conn.execute(text("TRUNCATE TABLE dw.agg_payment_methods"))
        conn.execute(text("""
            INSERT INTO dw.agg_payment_methods
            SELECT payment_type, COUNT(*), COUNT(DISTINCT order_id),
                ROUND(SUM(payment_value)::NUMERIC, 2),
                ROUND(AVG(payment_installments)::NUMERIC, 2),
                ROUND(AVG(payment_value)::NUMERIC, 2)
            FROM dw.fact_order_payments
            WHERE payment_value > 0
            GROUP BY payment_type ORDER BY SUM(payment_value) DESC
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dw.agg_reviews_monthly (
                year INT, month INT, total_reviews INT, avg_score NUMERIC(3,2),
                score_5 INT, score_4 INT, score_3 INT, score_2 INT, score_1 INT,
                PRIMARY KEY (year, month)
            )
        """))
        conn.execute(text("TRUNCATE TABLE dw.agg_reviews_monthly"))
        conn.execute(text("""
            INSERT INTO dw.agg_reviews_monthly
            SELECT d.year, d.month, COUNT(*),
                ROUND(AVG(r.review_score)::NUMERIC, 2),
                COUNT(*) FILTER (WHERE r.review_score = 5),
                COUNT(*) FILTER (WHERE r.review_score = 4),
                COUNT(*) FILTER (WHERE r.review_score = 3),
                COUNT(*) FILTER (WHERE r.review_score = 2),
                COUNT(*) FILTER (WHERE r.review_score = 1)
            FROM dw.fact_order_reviews r
            JOIN dw.dim_date d ON r.order_purchase_date_key = d.date_key
            GROUP BY d.year, d.month ORDER BY d.year, d.month
        """))


with DAG(
    dag_id="dw_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="DW Pipeline - Build Dimensions, Facts and Aggregates",
) as dag:

    t3 = PythonOperator(task_id="build_dimensions",    python_callable=task3_build_dimensions)
    t4 = PythonOperator(task_id="build_fact_table",    python_callable=task4_build_fact)
    t5 = PythonOperator(task_id="quality_checks",      python_callable=task5_quality_checks)
    t6 = PythonOperator(task_id="refresh_aggregates",  python_callable=task6_refresh_aggregates)

    t3 >> t4 >> t5 >> t6