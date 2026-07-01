-- ==========================================
-- 1) إنشاء السكيما النهائية للـ Data Warehouse
-- ==========================================
CREATE DATABASE IF NOT EXISTS dw;

-- ==========================================
-- 2) إنشاء جداول الأبعاد (Dimension Tables)
-- ==========================================

CREATE TABLE IF NOT EXISTS dw.dim_customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix VARCHAR(20),
    customer_city VARCHAR(100),
    customer_state VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS dw.dim_products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_category_name VARCHAR(100),
    product_category_name_english VARCHAR(100),
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT,
    product_weight_g DECIMAL(10,2),
    product_length_cm DECIMAL(10,2),
    product_height_cm DECIMAL(10,2),
    product_width_cm DECIMAL(10,2)
);

CREATE TABLE IF NOT EXISTS dw.dim_sellers (
    seller_id VARCHAR(50) PRIMARY KEY,
    seller_zip_code_prefix VARCHAR(20),
    seller_city VARCHAR(100),
    seller_state VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- ==========================================
-- 3) إنشاء جداول الحقائق (Fact Tables)
-- ==========================================

CREATE TABLE IF NOT EXISTS dw.fact_order_items (
    order_id VARCHAR(50),
    order_item_id INT,
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    order_purchase_date_key INT,
    order_status VARCHAR(50),
    shipping_limit_date DATETIME,
    price DECIMAL(10, 2),
    freight_value DECIMAL(10, 2),
    total_amount DECIMAL(10, 2),
    PRIMARY KEY (order_id, order_item_id),
    FOREIGN KEY (customer_id) REFERENCES dw.dim_customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES dw.dim_products(product_id),
    FOREIGN KEY (seller_id) REFERENCES dw.dim_sellers(seller_id),
    FOREIGN KEY (order_purchase_date_key) REFERENCES dw.dim_date(date_key)
);

CREATE TABLE IF NOT EXISTS dw.fact_order_payments (
    order_id VARCHAR(50),
    payment_sequential INT,
    order_purchase_date_key INT,
    payment_type VARCHAR(50),
    payment_installments INT,
    payment_value DECIMAL(10, 2),
    PRIMARY KEY (order_id, payment_sequential),
    FOREIGN KEY (order_purchase_date_key) REFERENCES dw.dim_date(date_key)
);

CREATE TABLE IF NOT EXISTS dw.fact_order_reviews (
    review_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50),
    order_purchase_date_key INT,
    review_score INT,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date DATETIME,
    review_answer_timestamp DATETIME,
    FOREIGN KEY (order_purchase_date_key) REFERENCES dw.dim_date(date_key)
);

-- =====================================================================
-- 4) مرحلة الـ Loading (تفريغ وتعبئة البيانات - جاهزة للـ Airflow)
-- =====================================================================

-- خطوة الأمان في MySQL: إيقاف قيود الـ Foreign Keys لتفريغ الجداول بأمان
SET FOREIGN_KEY_CHECKS = 0;

TRUNCATE TABLE dw.fact_order_reviews;
TRUNCATE TABLE dw.fact_order_payments;
TRUNCATE TABLE dw.fact_order_items;
TRUNCATE TABLE dw.dim_customers;
TRUNCATE TABLE dw.dim_products;
TRUNCATE TABLE dw.dim_sellers;
TRUNCATE TABLE dw.dim_date;

SET FOREIGN_KEY_CHECKS = 1;

-- أولاً: توليد الكاليندر الثابت في جدول التواريخ (من 2016 لـ 2020) باستخدام Recursive CTE
-- لضمان عدم حدوث خطأ بسبب عدد الأيام الكبير (حوالي 1826 يوم) بنزود الـ Max Recursion Depth
SET SESSION cte_max_recursion_depth = 3000;

INSERT INTO dw.dim_date (date_key, full_date, year, quarter, month, month_name, day, day_of_week, day_name, is_weekend)
WITH RECURSIVE calendar AS (
    SELECT CAST('2016-01-01' AS DATE) AS datum
    UNION ALL
    SELECT datum + INTERVAL 1 DAY FROM calendar WHERE datum < '2020-12-31'
)
SELECT 
    CAST(DATE_FORMAT(datum, '%Y%m%d') AS SIGNED) AS date_key,
    datum AS full_date,
    YEAR(datum) AS year,
    QUARTER(datum) AS quarter,
    MONTH(datum) AS month,
    DATE_FORMAT(datum, '%M') AS month_name,
    DAY(datum) AS day,
    (WEEKDAY(datum) + 1) AS day_of_week, -- في MySQL الـ WEEKDAY بيبدأ من 0 (الاثنين)، فبنضيف 1 عشان يطابق بوسطجرس
    DATE_FORMAT(datum, '%W') AS day_name,
    CASE WHEN WEEKDAY(datum) >= 5 THEN TRUE ELSE FALSE END AS is_weekend -- 5 يعني السبت و 6 الأحد في نظام WEEKDAY
FROM calendar;

-- ثانياً: نقل أبعاد العملاء والبائعين
INSERT INTO dw.dim_customers SELECT * FROM staging.customers;
INSERT INTO dw.dim_sellers SELECT * FROM staging.sellers;

-- ثالثاً: نقل بعد المنتجات
INSERT INTO dw.dim_products (
    product_id, product_category_name, product_category_name_english,
    product_name_length, product_description_length, product_photos_qty,
    product_weight_g, product_length_cm, product_height_cm, product_width_cm
) 
SELECT 
    product_id, product_category_name, product_category_name_english,
    product_name_lenght, product_description_lenght, product_photos_qty,
    product_weight_g, product_length_cm, product_height_cm, product_width_cm
FROM staging.products;

-- رابعاً: نقل جداول الحقائق (Fact Tables) مع ربط الـ Date Key الذكي
INSERT INTO dw.fact_order_items (
    order_id, order_item_id, customer_id, product_id, seller_id, 
    order_purchase_date_key, order_status, shipping_limit_date, price, freight_value, total_amount
)
SELECT 
    oi.order_id, oi.order_item_id, o.customer_id, oi.product_id, oi.seller_id,
    CAST(DATE_FORMAT(o.order_purchase_timestamp, '%Y%m%d') AS SIGNED),
    o.order_status, oi.shipping_limit_date, oi.price, oi.freight_value, oi.total_amount
FROM staging.order_items oi
JOIN staging.orders o ON oi.order_id = o.order_id;

INSERT INTO dw.fact_order_payments (order_id, payment_sequential, order_purchase_date_key, payment_type, payment_installments, payment_value)
SELECT op.order_id, op.payment_sequential, CAST(DATE_FORMAT(o.order_purchase_timestamp, '%Y%m%d') AS SIGNED), op.payment_type, op.payment_installments, op.payment_value
FROM staging.order_payments op
JOIN staging.orders o ON op.order_id = o.order_id;

INSERT INTO dw.fact_order_reviews (review_id, order_id, order_purchase_date_key, review_score, review_comment_title, review_comment_message, review_creation_date, review_answer_timestamp)
SELECT r.review_id, r.order_id, CAST(DATE_FORMAT(o.order_purchase_timestamp, '%Y%m%d') AS SIGNED), r.review_score, r.review_comment_title, r.review_comment_message, r.review_creation_date, r.review_answer_timestamp
FROM staging.order_reviews r
JOIN staging.orders o ON r.order_id = o.order_id;

-- ==========================================
-- 5) الفهارس (Indexes) - تسريع الاستعلامات والـ Dashboards
-- ==========================================
-- ملحوظة: شيلنا الـ IF NOT EXISTS لأن MySQL لا يدعمها في إنشاء الفهارس بشكل مباشر في بعض الإصدارات، وهيشتغل عادي طالما الجدول جديد
CREATE INDEX idx_fact_items_product ON dw.fact_order_items(product_id);
CREATE INDEX idx_fact_items_customer ON dw.fact_order_items(customer_id);
CREATE INDEX idx_fact_items_date ON dw.fact_order_items(order_purchase_date_key);
CREATE INDEX idx_fact_payments_date ON dw.fact_order_payments(order_purchase_date_key);