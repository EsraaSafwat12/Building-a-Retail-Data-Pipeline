# 🛒 Retail Data Pipeline — Olist E-Commerce ETL & Analytics Platform

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.9+-blue?style=for-the-badge&logo=python" alt="Python"/>
  <img src="https://img.shields.io/badge/PostgreSQL-Data%20Warehouse-336791?style=for-the-badge&logo=postgresql" alt="PostgreSQL"/>
  <img src="https://img.shields.io/badge/Apache%20Airflow-Orchestration-017CEE?style=for-the-badge&logo=apacheairflow" alt="Airflow"/>
  <img src="https://img.shields.io/badge/Flask-REST%20API-black?style=for-the-badge&logo=flask" alt="Flask"/>
  <img src="https://img.shields.io/badge/Docker-Containerized-2496ED?style=for-the-badge&logo=docker" alt="Docker"/>
</p>

<p align="center">
  <b>An end-to-end data engineering pipeline that transforms raw Brazilian e-commerce data into a governed data warehouse, powering a live analytics dashboard.</b>
</p>

---

## 📋 Table of Contents

- [About](#-about)
- [Architecture](#-architecture)
- [Dataset](#-dataset)
- [Key Components](#-key-components)
- [Data Warehouse Model](#-data-warehouse-model)
- [Orchestration with Airflow](#-orchestration-with-airflow)
- [Batch Processing & Pipeline State](#-batch-processing--pipeline-state)
- [REST API](#-rest-api)
- [Dashboard](#-dashboard)
- [Tech Stack](#-tech-stack)
- [Getting Started](#-getting-started)
- [Project Structure](#-project-structure)
- [Security Notes](#-security-notes)
- [Team Members](#-team-members)
- [License](#-license)

---

## 🧾 About

**Retail Data Pipeline** is a full-scale data engineering project built around the **Olist Brazilian E-Commerce** public dataset. It simulates a real production data platform for a retail business — ingesting raw multi-source data, cleaning and validating it, modeling it into a dimensional data warehouse, and serving it through both a REST API and a live interactive dashboard.

The project demonstrates the complete data lifecycle:

**Raw data → Staging → Cleaning → Data Warehouse (star schema) → API → Dashboard**, with **Apache Airflow** orchestrating the pipeline end-to-end and batch/incremental processing tracked via persistent pipeline state.

---

## 🏗️ Architecture

```
                     ┌────────────────────────────┐
                     │   Raw Source CSVs           │
                     │   data/raw_source/           │
                     │  (orders, customers,          │
                     │   products, sellers,           │
                     │   payments, reviews,            │
                     │   geolocation, inventory)         │
                     └──────────────┬───────────────┘
                                    │
                                    ▼
                     ┌────────────────────────────┐
                     │     ETL Pipeline             │
                     │     (etl_pipeline.py)         │
                     │  1. Load → staging schema      │
                     │  2. Clean & validate data        │
                     │  3. Build dw star schema           │
                     └──────────────┬───────────────┘
                                    │
                     ┌──────────────┴───────────────┐
                     ▼                               ▼
        ┌─────────────────────────┐     ┌─────────────────────────┐
        │   PostgreSQL Database     │     │   Apache Airflow           │
        │   • staging schema          │◀───▶│   • staging_dag              │
        │   • dw schema (facts +        │     │   • dw_dag                      │
        │     dimensions)                  │     │   (Dockerized via                 │
        └────────────┬─────────────┘     │    docker-compose)                  │
                     │                    └─────────────────────────┘
                     ▼
        ┌─────────────────────────┐
        │      Flask REST API        │
        │        (api.py)              │
        └────────────┬─────────────┘
                     │
                     ▼
        ┌─────────────────────────┐
        │  Interactive Dashboard     │
        │  (dashboard.html + Chart.js) │
        └─────────────────────────┘

           Batch/incremental outputs tracked in
           data/batch_outputs/ + pipeline_state.json
```

---

## 📦 Dataset

Built on the **Olist Store** Brazilian e-commerce public dataset, enriched with a supplementary inventory feed:

| Source File | Description |
|---|---|
| `olist_orders_dataset.csv` | Order-level records with timestamps and status |
| `olist_order_items_dataset.csv` | Line items per order (product, seller, price, freight) |
| `olist_order_payments_dataset.csv` | Payment method and installment data |
| `olist_order_reviews_dataset.csv` | Customer review scores and comments |
| `olist_customers_dataset.csv` | Customer identifiers and location |
| `olist_sellers_dataset.csv` | Seller identifiers and location |
| `olist_products_dataset.csv` | Product attributes (category, dimensions, weight) |
| `olist_geolocation_dataset.csv` | Zip-code-level lat/long data |
| `product_category_name_translation.csv` | Portuguese → English category mapping |
| `inventory.csv` | Supplementary stock-level data for inventory analytics |

---

## 🧩 Key Components

| Component | File(s) | Description |
|---|---|---|
| **ETL Engine** | `etl_pipeline.py` | Loads raw CSVs into a PostgreSQL `staging` schema, applies cleaning/validation rules, then transforms and loads into the `dw` warehouse schema. Fully logged to `pipeline.log`. |
| **Orchestration** | `Airflow/dags/staging_dag.py`, `Airflow/dags/dw_dag.py` | Two chained Airflow DAGs automating staging ingestion/cleaning and warehouse dimension/fact builds, deployed via `docker-compose.yml`. |
| **Batch & Incremental Processing** | `data/processed/`, `data/batch_outputs/`, `pipeline_state.json` | Monthly and batch-numbered processed extracts, daily/monthly aggregate summaries, and synthetic inventory batches — with pipeline state persisted for resumable, incremental runs. |
| **REST API** | `api.py` | Flask + Flask-CORS service exposing warehouse tables (`/api/sales`, `/api/products`, etc.) as JSON for the dashboard and external consumers. |
| **Analytics Dashboard** | `dashboard.html` | Dark-themed, tabbed, Chart.js-powered dashboard consuming the Flask API for live KPIs. |
| **Data Modeling** | `SQL_mysql.sql`, `Project_mysql.sql`, `aggregate_mysql.sql` | DDL for staging/warehouse tables and pre-built aggregation queries for reporting. |
| **ERD** | `erd_final.pdf` | Full Entity-Relationship Diagram of the data warehouse schema. |
| **Exploratory Analysis** | `final retail (1).ipynb` | Jupyter notebook used for EDA and pipeline logic prototyping. |
| **Loader Utility** | `pycharm_load_raw (1).py` | Standalone script for locally loading raw source data during development. |

---

## 🗃️ Data Warehouse Model

The warehouse (`dw` schema) follows a **star schema** design:

**Dimension tables:**
- `dw.dim_customers` — customer identity & location
- `dw.dim_products` — product attributes & category (English-translated)
- `dw.dim_sellers` — seller identity & location
- `dw.dim_date` — calendar/date breakdown for time-based analysis

**Fact tables:**
- `dw.fact_order_items` — the central fact table: order/customer/product/seller keys, price, freight, total amount, and purchase date key
- `dw.fact_order_payments` — payment records per order: payment type, installments, value, and purchase date key
- `dw.fact_order_reviews` — customer reviews per order: review score, comment title, comment message, creation date, and purchase date key

This model supports fast slice-and-dice analytics (sales by category, by region, by time period, by seller performance, etc.) — see `erd_final.pdf` for the complete diagram.

---

## 🔄 Orchestration with Airflow

Two DAGs manage the pipeline end-to-end:

1. **`staging_dag`** — loads all raw CSVs into `staging` tables, then applies cleaning rules (deduplication, null handling, type casting). Runs on a `*/5 * * * *` schedule. At the end of every successful run, it automatically triggers `dw_dag` using a `TriggerDagRunOperator` — no manual hand-off required.
2. **`dw_dag`** — triggered exclusively by `staging_dag` (its own `schedule_interval` is `None`); builds `dw` dimension tables, populates the three fact tables (`fact_order_items`, `fact_order_payments`, `fact_order_reviews`), runs data quality checks, and refreshes all aggregate tables.

This chained design guarantees that the warehouse build always starts **after** staging is fully complete, preventing race conditions between the two pipelines.

The Airflow stack (webserver, scheduler, metadata Postgres) is fully containerized via `Airflow/docker-compose.yml`, with `dags/`, `logs/`, `plugins/`, and the project's `data/` folder mounted as volumes.

---

## ⏱️ Batch Processing & Pipeline State

Beyond the core ETL, the pipeline supports **incremental/batch execution**:

- `pipeline_state.json` tracks the current processing window (`current_window_start`) and the next batch ID — enabling the pipeline to resume from where it left off rather than reprocessing everything.
- `data/batch_outputs/` holds generated batch artifacts: `batch_summary.csv`, `batch_inventory_details.csv`, `synthetic_inventory.csv`, `daily_summary.csv`, and `monthly_summary.csv`.
- `data/processed/` stores time-partitioned and batch-numbered extracts (e.g. `orders_2016_09.csv`, `orders_batch_0001.csv`) for auditability and reprocessing.

---

## 🌐 REST API

Built with **Flask**, the API layer exposes clean JSON endpoints backed directly by the `dw` schema, for example:

- `GET /api/sales` → order items with pricing, freight, and status
- `GET /api/products` → product catalog with categories and dimensions

CORS is enabled so the dashboard (or any external client) can consume the API directly from the browser.

---

## 📊 Dashboard

`dashboard.html` is a self-contained, dark-themed analytics dashboard built with vanilla JS + **Chart.js**, featuring:

- A configurable API endpoint input (point it at any running instance of `api.py`)
- Live connection status indicator
- Tabbed views for different KPI categories
- Interactive charts rendered directly from live API data

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| **Language** | Python 3.9+ |
| **Data Processing** | pandas, SQLAlchemy |
| **Database** | PostgreSQL (`psycopg2`) |
| **Orchestration** | Apache Airflow (Docker Compose) |
| **API** | Flask, Flask-CORS |
| **Visualization** | HTML / CSS / JavaScript, Chart.js |
| **Config Management** | python-dotenv |
| **Notebook** | Jupyter |

---

## 🚀 Getting Started

### 1. Clone the repository
```bash
git clone https://github.com/EsraaSafwat12/Building-a-Retail-Data-Pipeline.git
cd Building-a-Retail-Data-Pipeline
```

### 2. Configure environment variables
Copy the example file and fill in your own local credentials — **never commit your real `.env`**:
```bash
cp .env.example .env
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

### 4. Run the ETL pipeline
```bash
python etl_pipeline.py
```
This loads raw data into `staging`, cleans it, and builds the `dw` warehouse.

### 5. (Optional) Run the full pipeline with Airflow
```bash
cd Airflow
docker-compose up -d
```
Open the Airflow UI at `http://localhost:8080`, then trigger `staging_dag` followed by `dw_dag`.

### 6. Start the API
```bash
python api.py
```

### 7. Launch the dashboard
Open `dashboard.html` in your browser and set the API URL (e.g. `http://localhost:5000`) in the connection field.

---

## 📁 Project Structure

```
Retail Data Pipeline/
├── Airflow/
│   ├── dags/
│   │   ├── staging_dag.py
│   │   └── dw_dag.py
│   ├── plugins/
│   └── docker-compose.yml
├── data/
│   ├── raw_source/          # Original Olist CSVs + inventory.csv
│   ├── processed/           # Cleaned, time-partitioned & batch extracts
│   ├── batch_outputs/       # Batch & aggregate summaries
│   └── pipeline_state.json  # Incremental processing state
├── etl_pipeline.py           # Main ETL script (staging → clean → dw)
├── api.py                     # Flask REST API
├── dashboard.html               # Analytics dashboard (Chart.js)
├── SQL_mysql.sql                 # Core schema DDL
├── Project_mysql.sql               # Project database schema
├── aggregate_mysql.sql               # Aggregation/reporting queries
├── erd_final.pdf                       # Data warehouse ERD
├── final retail (1).ipynb                # EDA / prototyping notebook
├── pycharm_load_raw (1).py                 # Local raw-data loader utility
├── pipeline.log                              # ETL execution log
├── requirements.txt
├── .env.example
└── .gitignore
```

---

## 🔒 Security Notes

- Real database credentials must **never** be committed to the repository — only `.env.example` with placeholder values should be tracked.
- Ensure `.env` is listed in `.gitignore`.
- If real credentials were ever pushed to the repo history, rotate them immediately and consider scrubbing them from git history (e.g. with `git filter-repo` or BFG Repo-Cleaner).

---

## 👥 Team Members

| # | Name |
|---|---|
| 1 | 👩‍💻 **Esraa Safwat Mohamed** |
| 2 | 👨‍💻 **Mohamed Mahmoud Ibrahim** |
| 3 | 👨‍💻 **Kirolos Magdy Helmy** |
| 4 | 👩‍💻 **Yasmin Eslam Esmat** |
| 5 | 👨‍💻 **Youssef Fetyan Mohamed** |
| 6 | 👩‍💻 **Noran Ayman** |

---

## 📄 License

This project is built for educational purposes as part of a data engineering learning track.

<p align="center">Made with ❤️ by the Retail Data Pipeline Team</p>
