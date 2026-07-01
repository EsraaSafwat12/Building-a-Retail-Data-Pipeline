import os
from flask import Flask, jsonify
from flask_cors import CORS
import psycopg2
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
CORS(app)

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "dbname":   os.getenv("DB_NAME", "Project"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD")
}

def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def run_query(query):
    """Runs a query safely: closes the connection even if an error happens."""
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in rows]
    finally:
        conn.close()


# ── EXISTING ENDPOINTS ─────────────────────────────────────

@app.route("/api/sales", methods=["GET"])
def get_sales():
    data = run_query("""
        SELECT order_id, customer_id, product_id, seller_id,
               order_status, price, freight_value, total_amount,
               order_purchase_date_key
        FROM dw.fact_order_items
        LIMIT 100
    """)
    return jsonify(data)


@app.route("/api/products", methods=["GET"])
def get_products():
    data = run_query("""
        SELECT product_id, product_category_name,
               product_category_name_english,
               product_weight_g, product_length_cm,
               product_height_cm, product_width_cm
        FROM dw.dim_products
        LIMIT 100
    """)
    return jsonify(data)


@app.route("/api/summary", methods=["GET"])
def get_summary():
    data = run_query("""
        SELECT year, month, month_name, total_orders,
               total_items_sold, total_revenue, avg_order_value
        FROM dw.agg_sales_monthly
        ORDER BY year, month
    """)
    return jsonify(data)


@app.route("/api/top_products", methods=["GET"])
def get_top_products():
    data = run_query("""
        SELECT product_id, product_category_name_english,
               total_units_sold, total_orders, total_revenue,
               avg_price, stock_status
        FROM dw.agg_inventory
        ORDER BY total_revenue DESC
        LIMIT 20
    """)
    return jsonify(data)


@app.route("/api/daily", methods=["GET"])
def get_daily():
    data = run_query("""
        SELECT full_date, total_orders, total_items_sold,
               total_revenue, avg_order_value
        FROM dw.agg_sales_daily
        ORDER BY full_date
    """)
    for d in data:
        d["full_date"] = str(d["full_date"])
    return jsonify(data)


# ── NEW ENDPOINTS ──────────────────────────────────────────

@app.route("/api/sellers", methods=["GET"])
def get_sellers():
    data = run_query("""
        SELECT
            s.seller_id,
            s.seller_city,
            s.seller_state,
            COUNT(DISTINCT f.order_id)             AS total_orders,
            COUNT(*)                               AS total_items,
            ROUND(SUM(f.total_amount)::NUMERIC, 2) AS total_revenue
        FROM dw.fact_order_items f
        JOIN dw.dim_sellers s ON f.seller_id = s.seller_id
        GROUP BY s.seller_id, s.seller_city, s.seller_state
        ORDER BY total_revenue DESC
        LIMIT 20
    """)
    return jsonify(data)


@app.route("/api/region", methods=["GET"])
def get_region():
    data = run_query("""
        SELECT
            c.customer_state,
            COUNT(DISTINCT f.order_id)             AS total_orders,
            COUNT(*)                               AS total_items,
            ROUND(SUM(f.total_amount)::NUMERIC, 2) AS total_revenue
        FROM dw.fact_order_items f
        JOIN dw.dim_customers c ON f.customer_id = c.customer_id
        GROUP BY c.customer_state
        ORDER BY total_revenue DESC
    """)
    return jsonify(data)


# ── PAYMENTS ENDPOINTS ─────────────────────────────────────

@app.route("/api/payments", methods=["GET"])
def get_payments():
    """Payment method breakdown from agg_payment_methods.
    'not_defined' is grouped under 'other' so dashboards don't show a confusing label."""
    data = run_query("""
        SELECT
            CASE WHEN payment_type = 'not_defined' THEN 'unknown'
     ELSE payment_type END AS payment_type,
            SUM(total_transactions)               AS total_transactions,
            SUM(total_orders)                     AS total_orders,
            SUM(total_revenue)                    AS total_revenue,
            ROUND(AVG(avg_installments)::NUMERIC, 2) AS avg_installments,
            ROUND(AVG(avg_payment_value)::NUMERIC, 2) AS avg_payment_value
        FROM dw.agg_payment_methods
        GROUP BY 1
        ORDER BY total_revenue DESC
    """)
    return jsonify(data)


@app.route("/api/payments/installments", methods=["GET"])
def get_installments():
    """Distribution of payment installments"""
    data = run_query("""
        SELECT
            payment_installments,
            COUNT(*)                                    AS total_transactions,
            ROUND(SUM(payment_value)::NUMERIC, 2)       AS total_revenue,
            ROUND(AVG(payment_value)::NUMERIC, 2)       AS avg_value
        FROM dw.fact_order_payments
        GROUP BY payment_installments
        ORDER BY payment_installments
    """)
    return jsonify(data)


# ── REVIEWS ENDPOINTS ──────────────────────────────────────

@app.route("/api/reviews", methods=["GET"])
def get_reviews():
    """Monthly reviews summary from agg_reviews_monthly"""
    data = run_query("""
        SELECT
            year, month, total_reviews, avg_score,
            score_5, score_4, score_3, score_2, score_1
        FROM dw.agg_reviews_monthly
        ORDER BY year, month
    """)
    return jsonify(data)


@app.route("/api/reviews/scores", methods=["GET"])
def get_review_scores():
    """Overall score distribution"""
    data = run_query("""
        SELECT
            review_score,
            COUNT(*)                              AS total,
            ROUND(COUNT(*) * 100.0 /
                SUM(COUNT(*)) OVER(), 2)          AS percentage
        FROM dw.fact_order_reviews
        GROUP BY review_score
        ORDER BY review_score DESC
    """)
    return jsonify(data)

@app.route("/api/reviews/comments", methods=["GET"])
def get_review_comments():
    """Recent reviews with their text comments.
    NULL comments are converted to empty strings so frontend clients
    don't crash or display the literal word 'null'."""
    data = run_query("""
        SELECT
            review_id,
            order_id,
            review_score,
            COALESCE(review_comment_title, '')   AS review_comment_title,
            COALESCE(review_comment_message, '') AS review_comment_message,
            review_creation_date
        FROM dw.fact_order_reviews
        ORDER BY review_creation_date DESC
        LIMIT 100
    """)
    for d in data:
        d["review_creation_date"] = str(d["review_creation_date"])
    return jsonify(data)



if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)