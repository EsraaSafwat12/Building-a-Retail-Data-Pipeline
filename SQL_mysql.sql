CREATE SCHEMA IF NOT EXISTS staging;

-- 1. product_category_name_translation
DROP TABLE IF EXISTS staging.product_category_name_translation;
CREATE TABLE staging.product_category_name_translation AS
WITH base AS (
    SELECT 
        NULLIF(TRIM(product_category_name), '') AS product_category_name,
        NULLIF(TRIM(product_category_name_english), '') AS product_category_name_english
    FROM staging.product_category_name_translation_raw
    WHERE product_category_name IS NOT NULL
      AND product_category_name_english IS NOT NULL
),
ranked AS (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY product_category_name ORDER BY product_category_name) AS rn
    FROM base
)
SELECT product_category_name, product_category_name_english
FROM ranked
WHERE rn = 1;

-- 2. customers
DROP TABLE IF EXISTS staging.customers;
CREATE TABLE staging.customers AS
WITH base AS (
    SELECT 
        NULLIF(TRIM(customer_id), '') AS customer_id,
        NULLIF(TRIM(customer_unique_id), '') AS customer_unique_id,
        NULLIF(TRIM(customer_zip_code_prefix), '') AS customer_zip_code_prefix,
        NULLIF(TRIM(customer_city), '') AS customer_city,
        NULLIF(TRIM(customer_state), '') AS customer_state
    FROM staging.customers_raw
    WHERE customer_id IS NOT NULL
),
ranked AS (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY customer_id) AS rn
    FROM base
)
SELECT customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state
FROM ranked
WHERE rn = 1;

-- 3. sellers
DROP TABLE IF EXISTS staging.sellers;
CREATE TABLE staging.sellers AS
WITH base AS (
    SELECT 
        NULLIF(TRIM(seller_id), '') AS seller_id,
        NULLIF(TRIM(seller_zip_code_prefix), '') AS seller_zip_code_prefix,
        NULLIF(TRIM(seller_city), '') AS seller_city,
        NULLIF(TRIM(seller_state), '') AS seller_state
    FROM staging.sellers_raw
    WHERE seller_id IS NOT NULL
),
ranked AS (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY seller_id ORDER BY seller_id) AS rn
    FROM base
)
SELECT seller_id, seller_zip_code_prefix, seller_city, seller_state
FROM ranked
WHERE rn = 1;

-- 4. orders
DROP TABLE IF EXISTS staging.orders;
CREATE TABLE staging.orders AS
WITH base AS (
    SELECT
        NULLIF(TRIM(order_id), '') AS order_id,
        NULLIF(TRIM(customer_id), '') AS customer_id,
        NULLIF(TRIM(order_status), '') AS order_status,
        CAST(NULLIF(TRIM(order_purchase_timestamp), '') AS DATETIME) AS order_purchase_timestamp,
        CAST(NULLIF(TRIM(order_approved_at), '') AS DATETIME) AS order_approved_at,
        CAST(NULLIF(TRIM(order_delivered_carrier_date), '') AS DATETIME) AS order_delivered_carrier_date,
        CAST(NULLIF(TRIM(order_delivered_customer_date), '') AS DATETIME) AS order_delivered_customer_date,
        CAST(NULLIF(TRIM(order_estimated_delivery_date), '') AS DATETIME) AS order_estimated_delivery_date
    FROM staging.orders_raw
),
extracted AS (
    SELECT 
        *,
        CAST(EXTRACT(YEAR FROM order_purchase_timestamp) AS SIGNED) AS year,
        CAST(EXTRACT(MONTH FROM order_purchase_timestamp) AS SIGNED) AS month,
        CAST(EXTRACT(DAY FROM order_purchase_timestamp) AS SIGNED) AS day,
        CAST((WEEKDAY(order_purchase_timestamp) + 1) AS SIGNED) AS isodow,
        CASE WHEN WEEKDAY(order_purchase_timestamp) IN (5, 6) THEN TRUE ELSE FALSE END AS is_weekend
    FROM base
    WHERE order_id IS NOT NULL
      AND customer_id IS NOT NULL
      AND order_status IS NOT NULL
      AND order_purchase_timestamp IS NOT NULL
),
ranked AS (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_purchase_timestamp DESC) AS rn
    FROM extracted
)
SELECT order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at,
       order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date,
       year, month, day, isodow, is_weekend
FROM ranked
WHERE rn = 1;

-- 5. products
DROP TABLE IF EXISTS staging.products;
CREATE TABLE staging.products AS
WITH base AS (
    SELECT
        NULLIF(TRIM(product_id), '') AS product_id,
        NULLIF(TRIM(product_category_name), '') AS product_category_name,
        CAST(NULLIF(TRIM(product_name_lenght), '') AS SIGNED) AS product_name_lenght,
        CAST(NULLIF(TRIM(product_description_lenght), '') AS SIGNED) AS product_description_lenght,
        CAST(NULLIF(TRIM(product_photos_qty), '') AS SIGNED) AS product_photos_qty,
        CAST(NULLIF(TRIM(product_weight_g), '') AS DECIMAL(15,4)) AS product_weight_g,
        CAST(NULLIF(TRIM(product_length_cm), '') AS DECIMAL(15,4)) AS product_length_cm,
        CAST(NULLIF(TRIM(product_height_cm), '') AS DECIMAL(15,4)) AS product_height_cm,
        CAST(NULLIF(TRIM(product_width_cm), '') AS DECIMAL(15,4)) AS product_width_cm
    FROM staging.products_raw
),
joined AS (
    SELECT 
        p.*,
        COALESCE(t.product_category_name_english, 'unknown') AS product_category_name_english
    FROM base p
    LEFT JOIN staging.product_category_name_translation t
      ON p.product_category_name = t.product_category_name
    WHERE p.product_id IS NOT NULL
),
ranked AS (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id) AS rn
    FROM joined
)
SELECT product_id, product_category_name, product_name_lenght, product_description_lenght,
       product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm,
       product_category_name_english
FROM ranked
WHERE rn = 1;

-- 6. order_items
DROP TABLE IF EXISTS staging.order_items;
CREATE TABLE staging.order_items AS
WITH base AS (
    SELECT
        NULLIF(TRIM(order_id), '') AS order_id,
        CAST(NULLIF(TRIM(order_item_id), '') AS SIGNED) AS order_item_id,
        NULLIF(TRIM(product_id), '') AS product_id,
        NULLIF(TRIM(seller_id), '') AS seller_id,
        CAST(NULLIF(TRIM(shipping_limit_date), '') AS DATETIME) AS shipping_limit_date,
        ABS(CAST(NULLIF(TRIM(price), '') AS DECIMAL(15,2))) AS price,
        ABS(CAST(NULLIF(TRIM(freight_value), '') AS DECIMAL(15,2))) AS freight_value
    FROM staging.order_items_raw
),
calculated AS (
    SELECT 
        *,
        ROUND(CAST((price + freight_value) AS DECIMAL(15,2)), 2) AS total_amount
    FROM base
    WHERE order_id IS NOT NULL
      AND order_item_id IS NOT NULL
      AND product_id IS NOT NULL
      AND seller_id IS NOT NULL
      AND shipping_limit_date IS NOT NULL
      AND price IS NOT NULL
      AND freight_value IS NOT NULL
),
ranked AS (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY order_id, order_item_id ORDER BY order_id, order_item_id) AS rn
    FROM calculated
)
SELECT order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value, total_amount
FROM ranked
WHERE rn = 1;

-- 7. order_payments
DROP TABLE IF EXISTS staging.order_payments;
CREATE TABLE staging.order_payments AS
WITH base AS (
    SELECT
        NULLIF(TRIM(order_id), '') AS order_id,
        CAST(NULLIF(TRIM(payment_sequential), '') AS SIGNED) AS payment_sequential,
        CASE
            WHEN LOWER(NULLIF(TRIM(payment_type), '')) = 'not_defined' THEN 'unknown'
            ELSE NULLIF(TRIM(payment_type), '')
        END AS payment_type,
        CAST(NULLIF(TRIM(payment_installments), '') AS SIGNED) AS payment_installments,
        CAST(NULLIF(TRIM(payment_value), '') AS DECIMAL(15,2)) AS payment_value
    FROM staging.order_payments_raw
    WHERE order_id IS NOT NULL
      AND payment_sequential IS NOT NULL
      AND payment_installments IS NOT NULL
      AND payment_value IS NOT NULL
),
ranked AS (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY order_id, payment_sequential ORDER BY order_id, payment_sequential) AS rn
    FROM base
)
SELECT order_id, payment_sequential, payment_type, payment_installments, payment_value
FROM ranked
WHERE rn = 1;

-- 8. order_reviews
DROP TABLE IF EXISTS staging.order_reviews;
CREATE TABLE staging.order_reviews AS
WITH base AS (
    SELECT
        NULLIF(TRIM(review_id), '') AS review_id,
        NULLIF(TRIM(order_id), '') AS order_id,
        CAST(NULLIF(TRIM(review_score), '') AS SIGNED) AS review_score,
        NULLIF(TRIM(review_comment_title), '') AS review_comment_title,
        NULLIF(TRIM(review_comment_message), '') AS review_comment_message,
        CAST(NULLIF(TRIM(review_creation_date), '') AS DATETIME) AS review_creation_date,
        CAST(NULLIF(TRIM(review_answer_timestamp), '') AS DATETIME) AS review_answer_timestamp
    FROM staging.order_reviews_raw
    WHERE review_id IS NOT NULL
      AND order_id IS NOT NULL
      AND review_score IS NOT NULL
      AND review_creation_date IS NOT NULL
),
ranked AS (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY review_id ORDER BY review_creation_date DESC, review_answer_timestamp DESC) AS rn
    FROM base
)
SELECT review_id, order_id, review_score, review_comment_title, review_comment_message, review_creation_date, review_answer_timestamp
FROM ranked
WHERE rn = 1;

-- 9. geolocation
-- ملحوظة: هنا الـ DISTINCT ON واخد كل الأعمدة المتاحة، فـ DISTINCT العادية في MySQL تؤدي الغرض تماماً وأسرع بكتير.
DROP TABLE IF EXISTS staging.geolocation;
CREATE TABLE staging.geolocation AS
SELECT DISTINCT 
    NULLIF(TRIM(geolocation_zip_code_prefix), '') AS geolocation_zip_code_prefix,
    CAST(NULLIF(TRIM(geolocation_lat), '') AS DECIMAL(10,7)) AS geolocation_lat,
    CAST(NULLIF(TRIM(geolocation_lng), '') AS DECIMAL(10,7)) AS geolocation_lng,
    NULLIF(TRIM(geolocation_city), '') AS geolocation_city,
    NULLIF(TRIM(geolocation_state), '') AS geolocation_state
FROM staging.geolocation_raw
WHERE geolocation_zip_code_prefix IS NOT NULL
  AND geolocation_lat IS NOT NULL
  AND geolocation_lng IS NOT NULL
  AND geolocation_city IS NOT NULL
  AND geolocation_state IS NOT NULL;