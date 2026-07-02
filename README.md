<div align="center">

<img src="https://readme-typing-svg.demolab.com?font=Fira+Code&size=32&duration=3000&pause=1000&color=2E75B6&center=true&vCenter=true&width=700&lines=Retail+Data+Pipeline;Olist+E-Commerce+Analytics;End-to-End+Data+Engineering" alt="Typing SVG" />

<br/>

<img src="https://img.shields.io/badge/Python-3.9+-3776AB?style=for-the-badge&logo=python&logoColor=white"/>
<img src="https://img.shields.io/badge/PostgreSQL-18-336791?style=for-the-badge&logo=postgresql&logoColor=white"/>
<img src="https://img.shields.io/badge/Apache_Airflow-2.9.3-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white"/>
<img src="https://img.shields.io/badge/Flask-3.1-000000?style=for-the-badge&logo=flask&logoColor=white"/>
<img src="https://img.shields.io/badge/Docker-Containerized-2496ED?style=for-the-badge&logo=docker&logoColor=white"/>
<img src="https://img.shields.io/badge/Azure-Cloud_Deployed-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white"/>

<br/><br/>

> **An end-to-end data engineering platform that transforms 100K+ raw Brazilian e-commerce records into a governed star-schema warehouse, served through a REST API and a live analytics dashboard — fully automated with Apache Airflow.**

<br/>

[![GitHub repo](https://img.shields.io/badge/GitHub-EsraaSafwat12-181717?style=flat-square&logo=github)](https://github.com/EsraaSafwat12/Building-a-Retail-Data-Pipeline)
![Status](https://img.shields.io/badge/Status-Complete-brightgreen?style=flat-square)
![License](https://img.shields.io/badge/License-Educational-orange?style=flat-square)

</div>

---

## 📌 Table of Contents

| | Section |
|---|---|
| 🧾 | [About the Project](#-about-the-project) |
| 🏗️ | [Architecture](#️-architecture) |
| 📦 | [Dataset](#-dataset) |
| 🔄 | [Pipeline Stages](#-pipeline-stages) |
| 🗃️ | [Data Warehouse Model](#️-data-warehouse-model) |
| ✈️ | [Airflow Orchestration](#️-airflow-orchestration) |
| 🌐 | [REST API](#-rest-api) |
| 📊 | [Dashboard](#-dashboard) |
| 🛠️ | [Tech Stack](#️-tech-stack) |
| 🚀 | [Getting Started](#-getting-started) |
| 📁 | [Project Structure](#-project-structure) |
| 👥 | [Team](#-team) |

---

## 🧾 About the Project

**Retail Data Pipeline** is a production-grade data engineering project built on the real-world **Olist Brazilian E-Commerce** public dataset.

It simulates a complete retail analytics platform — from raw multi-source CSV ingestion, through a rigorous cleaning and transformation layer, into a fully dimensional star-schema data warehouse, and finally served through both a documented REST API and an interactive live dashboard.

```
Raw CSV data  →  Staging (clean)  →  DW Star Schema  →  Flask API  →  Dashboard
                      ↑__________________________↑
                      Automated every 5 minutes via Apache Airflow
```

### ✨ What makes it production-ready?

- 🔁 **Fully automated** — two chained Airflow DAGs run on a `*/5 * * * *` schedule with zero manual steps
- 🛡️ **Resilient** — idempotent pipeline (TRUNCATE before INSERT), no duplicate-key failures on re-runs
- ✅ **Quality-checked** — built-in data quality assertions after every load
- 🔗 **Decoupled** — API layer isolates the database from the dashboard
- ☁️ **Cloud-deployed** — hosted on Microsoft Azure

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                              │
│   9 Olist CSV files  +  inventory.csv  (data/raw_source/)       │
└──────────────────────────────┬──────────────────────────────────┘
                                │
                    ┌───────────▼───────────┐
                    │   STAGING PIPELINE     │   ← Airflow DAG 1
                    │  staging_dag.py        │     schedule: */5 * * * *
                    │                        │
                    │  Task 1: Load raw CSVs │
                    │  Task 2: Clean & transform│
                    │  Task 3: Trigger DW ──────────────────────┐
                    └───────────────────────┘                   │
                                                                │
                    ┌───────────────────────┐                   │
                    │   WAREHOUSE PIPELINE   │ ◄─── triggered ──┘
                    │  dw_dag.py             │   (TriggerDagRunOperator)
                    │                        │
                    │  Task 1: Build dims    │
                    │  Task 2: Build facts   │
                    │  Task 3: Quality checks│
                    │  Task 4: Aggregates    │
                    └───────────┬───────────┘
                                │
               ┌────────────────▼────────────────┐
               │         PostgreSQL DW            │
               │   staging schema + dw schema     │
               └────────────────┬────────────────┘
                                │
               ┌────────────────▼────────────────┐
               │         Flask REST API           │
               │           api.py                 │
               │       11 endpoints               │
               └────────────────┬────────────────┘
                                │
               ┌────────────────▼────────────────┐
               │     Interactive Dashboard        │
               │   dashboard.html + Chart.js      │
               └─────────────────────────────────┘
```

---

## 📦 Dataset

Built on the **[Olist Store](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)** Brazilian e-commerce public dataset (~100K orders, 2016–2018):

| # | Source File | Records | Description |
|---|---|---|---|
| 1 | `olist_orders_dataset.csv` | 99,441 | Order-level records — status, timestamps |
| 2 | `olist_order_items_dataset.csv` | 112,650 | Line items — product, seller, price, freight |
| 3 | `olist_order_payments_dataset.csv` | 103,886 | Payment method, installments, value |
| 4 | `olist_order_reviews_dataset.csv` | 99,224 | Customer review scores and comments |
| 5 | `olist_customers_dataset.csv` | 99,441 | Customer identity and location |
| 6 | `olist_sellers_dataset.csv` | 3,095 | Seller identity and location |
| 7 | `olist_products_dataset.csv` | 32,951 | Product attributes, category, dimensions |
| 8 | `olist_geolocation_dataset.csv` | 1M+ | Zip-code lat/long data |
| 9 | `product_category_name_translation.csv` | 71 | Portuguese → English category mapping |

---

## 🔄 Pipeline Stages

### Stage 1 — Raw Ingestion
All 9 CSV files are loaded as-is into `staging.*_raw` tables using **pandas + SQLAlchemy** via `pycharm_load_raw.py`.

### Stage 2 — Staging Clean & Transform (`SQL.sql`)
Raw tables are cleaned inside PostgreSQL into `staging.*` tables:

| Rule | Implementation |
|---|---|
| Remove empty strings | `NULLIF(TRIM(col), '')` |
| Deduplicate | `ROW_NUMBER() OVER (PARTITION BY id)` |
| Fix negative prices | `ABS(price)` |
| Compute derived columns | `total_amount = price + freight_value` |
| Add date parts | `EXTRACT(YEAR/MONTH/DAY/ISODOW FROM timestamp)` |
| Normalize payment type | `'not_defined' → 'unknown'` |
| Add English categories | `LEFT JOIN staging.product_category_name_translation` |

### Stage 3 — Data Warehouse Build (`Project (2).sql`)
Cleaned staging data is loaded into the star-schema `dw` schema (see model below).

### Stage 4 — Aggregates (`aggregate.sql`)
Pre-computed summary tables power fast dashboard queries:
- `dw.agg_sales_daily` / `dw.agg_sales_monthly`
- `dw.agg_inventory`
- `dw.agg_payment_methods`
- `dw.agg_reviews_monthly`

---

## 🗃️ Data Warehouse Model

The `dw` schema follows a **Star Schema** for fast analytical queries:

```
                        ┌─────────────────┐
                        │  dim_customers  │
                        │  (customer_id)  │
                        └────────┬────────┘
                                 │
┌──────────────┐    ┌────────────▼─────────────┐    ┌──────────────┐
│ dim_products │    │     fact_order_items       │    │  dim_sellers │
│ (product_id) │◄───│  order_id + order_item_id │───►│ (seller_id)  │
└──────────────┘    │  customer_id  product_id  │    └──────────────┘
                    │  seller_id    date_key     │
                    │  price  freight  total     │
                    └────────────┬──────────────┘
                                 │
                        ┌────────▼────────┐
                        │   dim_date      │
                        │  (date_key)     │
                        │  YYYYMMDD INT   │
                        └─────────────────┘

Additional fact tables:
  ├── fact_order_payments  (order_id, payment_type, installments, value)
  └── fact_order_reviews   (review_id, order_id, score, comments)
```

---

## ✈️ Airflow Orchestration

The pipeline runs on **Apache Airflow 2.9.3** (Dockerized) with two chained DAGs:

### `staging_pipeline` — runs every 5 minutes

```python
load_raw_to_staging  ──►  clean_transform_staging  ──►  trigger_dw_pipeline
                                                         (TriggerDagRunOperator)
```

### `dw_pipeline` — triggered automatically by staging

```python
build_dimensions  ──►  build_fact_table  ──►  quality_checks  ──►  refresh_aggregates
```

> **Why two DAGs?** Separating staging from the warehouse build guarantees the DW pipeline only starts *after* staging is fully complete — eliminating race conditions that occurred with a single monolithic DAG.

```bash
# Start Airflow
cd Airflow
docker-compose up -d
# UI: http://localhost:8080
# Trigger staging_pipeline — dw_pipeline fires automatically
```

---

## 🌐 REST API

Built with **Flask 3.1** — all endpoints read from the `dw` schema and return JSON:

| Method | Endpoint | Source Table | Returns |
|---|---|---|---|
| GET | `/api/sales` | `fact_order_items` | Latest 100 order items |
| GET | `/api/products` | `dim_products` | Product catalog |
| GET | `/api/summary` | `agg_sales_monthly` | Monthly revenue summary |
| GET | `/api/top_products` | `agg_inventory` | Top 20 products by revenue |
| GET | `/api/daily` | `agg_sales_daily` | Full daily sales history |
| GET | `/api/sellers` | `fact_order_items` + `dim_sellers` | Top 20 sellers |
| GET | `/api/region` | `fact_order_items` + `dim_customers` | Sales by state |
| GET | `/api/payments` | `agg_payment_methods` | Payment method breakdown |
| GET | `/api/payments/installments` | `fact_order_payments` | Installment distribution |
| GET | `/api/reviews` | `agg_reviews_monthly` | Monthly review summary |
| GET | `/api/reviews/scores` | `fact_order_reviews` | Score distribution (1–5) |
| GET | `/api/reviews/comments` | `fact_order_reviews` | Latest 100 comments |

```bash
python api.py        # starts on http://localhost:5000
```

---

## 📊 Dashboard

`dashboard.html` — a dark-themed, tabbed analytics dashboard powered by **Chart.js**:

| Tab | What it shows |
|---|---|
| 📈 Sales | Monthly & daily revenue trends, order volume |
| 🏆 Products | Top 20 by revenue, stock status (In Stock / Low / Out) |
| 🏪 Sellers | Top 20 sellers with city, state, and seller ID |
| 🗺️ Region | Revenue breakdown by Brazilian state |
| 💳 Payments | Credit card / boleto / voucher / debit split + installment distribution |
| ⭐ Reviews | Monthly avg score, 1–5 distribution, recent customer comments |

> Open `dashboard.html` in your browser with `api.py` running — no build step needed.

---

## 🛠️ Tech Stack

| Layer | Technology | Version |
|---|---|---|
| Language | Python | 3.9+ |
| Data Processing | pandas, SQLAlchemy | — |
| Database | PostgreSQL | 18 |
| Orchestration | Apache Airflow | 2.9.3 |
| Containerization | Docker + Docker Compose | — |
| API | Flask, Flask-CORS | 3.1.3 |
| Frontend | HTML / CSS / JavaScript, Chart.js | — |
| Cloud | Microsoft Azure | — |
| Security | python-dotenv | — |

---

## 🚀 Getting Started

### Prerequisites
- Python 3.9+
- PostgreSQL running locally
- Docker Desktop (for Airflow)

### 1. Clone
```bash
git clone https://github.com/EsraaSafwat12/Building-a-Retail-Data-Pipeline.git
cd Building-a-Retail-Data-Pipeline
```

### 2. Environment
```bash
cp .env.example .env
# Fill in your database credentials in .env
```
```env
DB_USER=postgres
DB_PASSWORD=your_password_here
DB_HOST=localhost
DB_PORT=5432
DB_NAME=Project
DATA_PATH=/path/to/data/raw_source
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Load raw data
```bash
python "pycharm_load_raw (1).py"
```

### 5. Build staging + warehouse
```bash
# Run SQL.sql then Project (2).sql then aggregate.sql in pgAdmin
# OR use the ETL script:
python etl_pipeline.py
```

### 6. (Optional) Run with Airflow
```bash
cd Airflow
docker-compose up -d
# Open http://localhost:8080 → trigger staging_pipeline
```

### 7. Start the API
```bash
python api.py
# Running on http://localhost:5000
```

### 8. Open the Dashboard
```
Open dashboard.html in your browser
```

---

## 📁 Project Structure

```
Building-a-Retail-Data-Pipeline/
│
├── 📂 Airflow/
│   ├── 📂 dags/
│   │   ├── staging_dag.py          # DAG 1: Load + Clean → triggers DW
│   │   └── dw_dag.py               # DAG 2: Dimensions + Facts + Aggregates
│   ├── 📂 plugins/
│   └── docker-compose.yml
│
├── 📂 data/
│   ├── raw_source/                  # Original Olist CSVs (gitignored)
│   ├── processed/                   # Time-partitioned extracts
│   ├── batch_outputs/               # Batch summaries + inventory
│   └── pipeline_state.json          # Incremental processing state
│
├── api.py                           # Flask REST API (11 endpoints)
├── dashboard.html                   # Analytics dashboard (Chart.js)
├── etl_pipeline.py                  # Full ETL script
├── pycharm_load_raw (1).py          # Raw CSV loader utility
│
├── SQL.sql                          # Staging schema DDL
├── Project (2).sql                  # DW schema + loading
├── aggregate.sql                    # Aggregate tables (PostgreSQL)
├── SQL_mysql.sql                    # MySQL version — staging
├── Project_mysql.sql                # MySQL version — DW
├── aggregate_mysql.sql              # MySQL version — aggregates
│
├── erd_final.pdf                    # Entity Relationship Diagram
├── final retail (1).ipynb           # EDA notebook
├── requirements.txt
├── .env.example                     # ← copy this to .env
└── .gitignore
```

---

## 🔒 Security

- **Never commit `.env`** — it's listed in `.gitignore`
- Only `.env.example` with placeholder values is tracked
- Database credentials are loaded at runtime via `python-dotenv`
- If credentials were ever pushed, rotate them immediately and scrub history with `git filter-repo`

---

## 👥 Team

<div align="center">

| # | Member | Role |
|---|---|---|
| 1 | 👩‍💻 **Esraa Safwat Mohamed** | Team Leader · Airflow · API · Dashboard |
| 2 | 👨‍💻 **Mohamed Mahmoud Ibrahim** | Staging Load · Cleaning · Transformation |
| 3 | 👨‍💻 **Kirolos Magdy Helmy** | Data Collection · Micro-Batch Simulation |
| 4 | 👩‍💻 **Yasmin Eslam Esmat** | Aggregate Tables · Azure Cloud Deployment |
| 5 | 👨‍💻 **Youssef Fetyan Mohamed** | Star Schema ERD · DW Setup · Dimensions & Fact Table |
| 6 | 👩‍💻 **Noran Ayman** | ETL Pipeline |

</div>

---

## 📄 License

This project is built for educational purposes as part of the **DEPI R4 — Data Engineering Track**.

---

<div align="center">

**⭐ If you found this project useful, consider starring the repo!**

Made with ❤️ by the Retail Data Pipeline Team · DEPI R4

</div>
