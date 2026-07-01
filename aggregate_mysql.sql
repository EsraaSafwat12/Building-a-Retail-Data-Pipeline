-- AGGREGATE TABLES — Olist Data Warehouse Project
-- DAILY SALES AGGREGATE
CREATE TABLE IF NOT EXISTS dw.agg_sales_daily (
    full_date          DATE PRIMARY KEY,
    total_orders       INT,
    total_items_sold   INT,
    total_revenue      DECIMAL(12,2),
    avg_order_value    DECIMAL(12,2)
);

TRUNCATE TABLE dw.agg_sales_daily;

INSERT INTO dw.agg_sales_daily (
    full_date,
    total_orders,
    total_items_sold,
    total_revenue,
    avg_order_value
)
SELECT
    d.full_date,
    COUNT(DISTINCT f.order_id) AS total_orders,
    COUNT(*) AS total_items_sold,
    SUM(f.total_amount) AS total_revenue,
    ROUND(
        SUM(f.total_amount) / COUNT(DISTINCT f.order_id),
        2
    ) AS avg_order_value
FROM dw.fact_order_items f
JOIN dw.dim_date d
    ON f.order_purchase_date_key = d.date_key
GROUP BY d.full_date
ORDER BY d.full_date;

-- MONTHLY SALES AGGREGATE
CREATE TABLE IF NOT EXISTS dw.agg_sales_monthly (
    year               INT,
    month              INT,
    month_name         VARCHAR(20),
    total_orders       INT,
    total_items_sold   INT,
    total_revenue      DECIMAL(12,2),
    avg_order_value    DECIMAL(12,2),
    PRIMARY KEY (year, month)
);

TRUNCATE TABLE dw.agg_sales_monthly;

INSERT INTO dw.agg_sales_monthly (
    year,
    month,
    month_name,
    total_orders,
    total_items_sold,
    total_revenue,
    avg_order_value
)
SELECT
    d.year,
    d.month,
    MIN(d.month_name) AS month_name,
    COUNT(DISTINCT f.order_id) AS total_orders,
    COUNT(*) AS total_items_sold,
    SUM(f.total_amount) AS total_revenue,
    ROUND(
        SUM(f.total_amount) / COUNT(DISTINCT f.order_id),
        2
    ) AS avg_order_value
FROM dw.fact_order_items f
JOIN dw.dim_date d
    ON f.order_purchase_date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- INVENTORY AGGREGATE
DROP TABLE IF EXISTS dw.agg_inventory;

CREATE TABLE dw.agg_inventory (
    product_id                     VARCHAR(50) PRIMARY KEY,
    product_category_name_english   VARCHAR(100),
    total_units_sold                INT,
    total_orders                    INT,
    total_revenue                   DECIMAL(12,2),
    avg_price                       DECIMAL(10,2),
    stock_qty                       INT,
    reorder_level                   INT,
    stock_status                    VARCHAR(20)
);

INSERT INTO dw.agg_inventory (
    product_id,
    product_category_name_english,
    total_units_sold,
    total_orders,
    total_revenue,
    avg_price,
    stock_qty,
    reorder_level,
    stock_status
)
SELECT
    sales.product_id,
    sales.product_category_name_english,
    sales.total_units_sold,
    sales.total_orders,
    sales.total_revenue,
    sales.avg_price,
    inv.stock_qty,
    inv.reorder_level,
    CASE
        WHEN inv.stock_qty = 0                 THEN 'Out of Stock'
        WHEN inv.stock_qty < inv.reorder_level THEN 'Low Stock'
        ELSE                                        'In Stock'
    END AS stock_status
FROM (
    SELECT
        p.product_id,
        COALESCE(p.product_category_name_english, 'Uncategorized') AS product_category_name_english,
        COUNT(*)                               AS total_units_sold,
        COUNT(DISTINCT f.order_id)             AS total_orders,
        ROUND(SUM(f.total_amount), 2)          AS total_revenue, -- شيلنا الـ ::NUMERIC الزيادة لأن ROUND بتكفي
        ROUND(AVG(f.price), 2)                 AS avg_price
    FROM dw.fact_order_items f
    JOIN dw.dim_products p ON f.product_id = p.product_id
    GROUP BY p.product_id, p.product_category_name_english
) sales
JOIN (
    -- تم استبدال RANDOM() بـ RAND() وتعديل الـ Casting لـ SIGNED
    SELECT
        product_id,
        MOD(CRC32(product_id), 500)        AS stock_qty,
        MOD(CRC32(product_id), 80) + 20    AS reorder_level
    FROM dw.dim_products
) inv ON inv.product_id = sales.product_id
ORDER BY sales.total_revenue DESC;

-- VERIFICATION QUERIES
SELECT COUNT(*) AS daily_rows    FROM dw.agg_sales_daily;
SELECT COUNT(*) AS monthly_rows  FROM dw.agg_sales_monthly;
SELECT COUNT(*) AS inventory_rows FROM dw.agg_inventory;