# Instacart Analytics Engineering Pipeline

> A production-grade ELT data pipeline built on the modern data stack — featuring Kimball dimensional modeling (star schema with SCD Type 1 & 2), dbt transformations across 14 models, Apache Airflow orchestration, PostgreSQL for local development, Snowflake for cloud production, GitHub Actions CI/CD, and a live Streamlit analytics dashboard.


**[Live Dashboard → instacart-analytics-pipeline.streamlit.app](https://instacart-analytics-pipeline.streamlit.app/)**

[![dbt Build](https://img.shields.io/badge/dbt%20build-86%2F86%20passing-brightgreen)](https://github.com/Duncan610/instacart-analytics-pipeline/actions)
[![CI](https://github.com/Duncan610/instacart-analytics-pipeline/actions/workflows/dbt_ci.yml/badge.svg)](https://github.com/Duncan610/instacart-analytics-pipeline/actions)
[![Snowflake](https://img.shields.io/badge/Snowflake-Production-29B5E8)](https://www.snowflake.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-Live-FF4B4B)](https://instacart-analytics-pipeline.streamlit.app/)
[![dbt](https://img.shields.io/badge/dbt-1.8+-orange)](https://www.getdbt.com/)
[![Airflow](https://img.shields.io/badge/Airflow-2.9.3-017CEE)](https://airflow.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.12-3776AB)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791)](https://www.postgresql.org/)

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Business Problem](#business-problem)
3. [Architecture](#architecture)
4. [Tech Stack & Why Each Tool](#tech-stack--why-each-tool)
5. [Data Sources](#data-sources)
6. [ELT Pattern — Not ETL](#elt-pattern--not-etl)
7. [Dimensional Modeling — Star Schema](#dimensional-modeling--star-schema)
8. [SCD Types Implemented](#scd-types-implemented)
9. [dbt Project Structure](#dbt-project-structure)
10. [Data Layers Explained](#data-layers-explained)
11. [Data Quality & Testing](#data-quality--testing)
12. [Environments — Dev vs Production](#environments--dev-vs-production)
13. [Airflow Orchestration](#airflow-orchestration)
14. [CI/CD — GitHub Actions](#cicd--github-actions)
15. [Streamlit Dashboard](#streamlit-dashboard)
16. [Setup Guide](#setup-guide)
17. [Running the Pipeline](#running-the-pipeline)
18. [Key Business Insights](#key-business-insights)
19. [Challenges & Solutions](#challenges--solutions)
20. [What I Learned](#what-i-learned)

---

## Project Overview

This project builds a **production-grade ELT analytics pipeline** on the [Instacart Online Grocery Shopping Dataset](https://www.kaggle.com/datasets/psparks/instacart-market-basket-analysis) — one of the most realistic public datasets available, with **3.4 million orders, 206,000 users, 49,688 products, and 37 million total rows** across 6 CSV files.

The pipeline follows the modern **ELT paradigm** — Extract, Load, then Transform inside the database:
- **Extract & Load** — Python ingestion script (pandas + SQLAlchemy) reads raw CSVs and loads them into PostgreSQL with audit columns
- **Transform** — dbt models clean, join, aggregate, and model the data into a production star schema
- **Serve** — Snowflake hosts the production dimensional model; Streamlit delivers a live public dashboard

The final output is a fully tested, documented, CI/CD-enabled **Kimball star schema** with 2 fact tables and 4 dimension tables, orchestrated by Airflow, and visualized in a live Streamlit dashboard connected directly to Snowflake.

---

## Business Problem

Instacart's data team needs a reliable, tested, and well-documented data model to answer critical business questions:

| Business Question | Model That Answers It |
|---|---|
| Who are our Champions vs At Risk customers? | `dim_users` + `fact_orders` |
| Which products have the highest reorder rates? | `fact_order_products` + `dim_products` |
| What are peak ordering hours by day of week? | `fact_orders` + `dim_dates` |
| How does basket size vary by customer segment? | `fact_orders` + `dim_users` |
| Which departments drive the most order volume? | `fact_order_products` + `dim_departments` |
| Which products are always added first to cart? | `fact_order_products` |

---

## Architecture

![Architecture Diagram](images/instacart_pipeline_architecture.svg)

The pipeline follows these layers:
- **Data Sources** → Kaggle CSVs ingested via Python script
- **PostgreSQL** → Local development environment (Docker)
- **dbt** → 3-layer transformation: staging → intermediate → marts + core star schema
- **Snowflake** → Production warehouse, 86/86 tests passing
- **Airflow** → Daily orchestration at 6AM UTC (9AM EAT)
- **Streamlit** → Live public dashboard connected to Snowflake
- **GitHub Actions** → CI/CD on every push to main
---

## Tech Stack & Why Each Tool

| Tool | Role | Why This Tool |
|---|---|---|
| **dbt Core 1.8+** | SQL transformation & modeling | Industry standard for analytics engineering. SQL-first, version-controlled, self-documenting, built-in testing. Brings software engineering practices to SQL. |
| **Snowflake** | Production data warehouse | Separates compute from storage. Scales to petabytes. Dominant in modern data stacks. X-SMALL warehouse with 60s auto-suspend keeps costs near zero. |
| **PostgreSQL 16** | Local development database | Free. Docker-based. SQL-compatible with Snowflake for fast local iteration without cloud costs. |
| **Apache Airflow 2.9.3** | Workflow orchestration | DAG-based scheduling with retry logic and task dependency management. Industry standard at data-mature companies. |
| **Python 3.12** | Data ingestion | Pandas chunked reading (100K rows/chunk) handles 32M-row files without memory issues. SQLAlchemy for database-agnostic connections. |
| **Streamlit** | Analytics dashboard | Python-native BI tool. Connects directly to Snowflake. Deploys publicly for free. Anyone can view the live dashboard without installing anything. |
| **Docker + Docker Compose** | Environment management | Reproducible local PostgreSQL + Airflow setup. One command starts the entire local stack. |
| **GitHub Actions** | CI/CD | Automated dbt testing on every push. Validates all models compile and build correctly before reaching production. |
| **dbt-utils** | SQL utilities | Surrogate key generation via `generate_surrogate_key()`, date spine generation for `dim_dates`. |
| **Plotly** | Dashboard charts | Interactive charts in Streamlit — area charts, bar charts, bubble charts, all connected to live Snowflake data. |

---

## Data Sources

| File | Rows | Description |
|---|---|---|
| `orders.csv` | 3,421,083 | All orders: user, day of week, hour, days since last order |
| `order_products__prior.csv` | 32,434,489 | Products in all prior orders |
| `order_products__train.csv` | 1,384,617 | Products in most recent orders (Kaggle eval set) |
| `products.csv` | 49,688 | Product catalog with aisle and department |
| `aisles.csv` | 134 | Aisle reference table |
| `departments.csv` | 21 | Department reference table |

**Total: ~37 million rows across 6 tables**

> **Note on order_products files:** Kaggle split order line items into `prior` and `train` sets for ML purposes. From an analytics standpoint they are the same entity — products ordered. `stg_order_products.sql` unions them into a single clean table using `UNION ALL`.

---

## ⚡ ELT Pattern — Not ETL

This project implements **ELT (Extract → Load → Transform)** — the modern standard for analytics engineering.

```
TRADITIONAL ETL                    MODERN ELT (this project)
─────────────                      ─────────────────────────
Extract from source                Extract from CSVs
     ↓                                  ↓
Transform in middleware            Load raw into PostgreSQL/Snowflake
     ↓                                  ↓
Load clean data                    Transform INSIDE the database with dbt
```

**Why ELT over ETL?**
- Raw data is preserved in the database — you can always rerun transformations
- dbt runs SQL inside the warehouse which is optimized for this workload
- Transformations are version-controlled, tested, and documented like software
- No proprietary middleware — just SQL that any analyst can read

---

## Dimensional Modeling — Star Schema

This project implements a **Kimball-style star schema** — the industry standard for analytical data warehousing.

### Why Star Schema?

| Normalized (OLTP) | Star Schema (OLAP — this project) |
|---|---|
| Optimized for writes | Optimized for reads |
| Many small tables, many JOINs | Few wide tables, simple JOINs |
| Used in transactional systems | Used in data warehouses |
| Hard for analysts to query | Intuitive for BI tools |

### The Star Schema Diagram

```
                    ┌─────────────────┐
                    │   dim_dates     │
                    │ ─────────────── │
                    │ date_key  PK    │
                    │ full_date       │
                    │ year            │
                    │ month_name      │
                    │ day_name        │
                    │ is_weekend      │
                    │ season          │
                    └────────┬────────┘
                             │
┌─────────────────┐  ┌───────┴────────┐  ┌──────────────────────┐
│   dim_users     │  │  fact_orders   │  │   dim_departments    │
│ ─────────────── │  │ ────────────── │  │ ──────────────────── │
│ user_key   PK   │◄─│ user_key   FK  │  │ department_key  PK  │
│ user_id         │  │ order_key  PK  │  │ department_id        │
│ customer_segment│  │ date_key   FK  │  │ department_name      │
│ loyalty_tier    │  │ order_id       │  └──────────────────────┘
│ reorder_rate    │  │ total_items    │
│ rfm_average     │  │ reorder_rate   │  ┌──────────────────────┐
│ SCD Type 1      │  │ basket_size    │  │   dim_products       │
└─────────────────┘  │ INCREMENTAL    │  │ ──────────────────── │
                     └───────┬────────┘  │ product_key     PK  │
                             │           │ product_id           │
                     ┌───────┴────────┐  │ product_name         │
                     │fact_order_     │─►│ aisle_name           │
                     │products        │  │ department_name      │
                     │ ────────────── │  │ valid_from   SCD Type 2
                     │ order_id  FK   │  │ valid_to             │
                     │ product_key FK │  │ is_current           │
                     │ is_reordered   │  └──────────────────────┘
                     │ cart_position  │
                     │ INCREMENTAL    │
                     └────────────────┘
```

### dbt Lineage Graph

![dbt Lineage Graph](images/dbtlineagegraph.png)

*The full lineage from raw sources → staging → intermediate → dimensions → facts*

---

## SCD Types Implemented

### SCD Type 1 — `dim_users` (Overwrite)

User behavioral attributes (segment, loyalty tier, reorder rate) are **recalculated fresh on every pipeline run** from `fact_orders`. Overwriting is correct here because we can always recompute the past from raw order data. Keeping history adds no analytical value.

```sql
-- Type 1: current row simply overwritten on each run
user_key | user_id | customer_segment | reorder_rate | updated_at
abc123   | 1       | Champion         | 0.73         | 2024-03-16
```

### SCD Type 2 — `dim_products` (Historical Rows)

Product attributes (name, department) could change over time. SCD Type 2 preserves full history by **adding a new row** for each change, enabling point-in-time queries.

```sql
-- Type 2: new row added when product changes, old row preserved
product_key | product_id | product_name  | dept_name  | valid_from | valid_to   | is_current
abc123      | 1          | Organic Milk  | Dairy      | 2019-01-01 | 2019-06-01 | FALSE
def456      | 1          | Organic Milk  | Beverages  | 2019-06-01 | NULL       | TRUE
```

---

## dbt Project Structure

```
dbt_project/
├── dbt_project.yml              # Project config, materialization strategy per layer
├── packages.yml                 # External packages (dbt-utils, dbt-expectations, etc.)
│
├── models/
│   ├── staging/                 # Layer 1: Source-aligned cleaning (5 models)
│   │   ├── _sources.yml         # Source definitions, freshness config, column tests
│   │   ├── stg_orders.sql       # Cleaned orders with day_name, time_bucket, is_first_order
│   │   ├── stg_products.sql     # Products joined with aisles + departments
│   │   ├── stg_order_products.sql # UNION ALL of prior + train with surrogate key
│   │   ├── stg_aisles.sql       # Reference table
│   │   └── stg_departments.sql  # Reference table
│   │
│   ├── intermediate/            # Layer 2: Business logic (1 ephemeral model)
│   │   └── int_customer_order_history.sql  # Per-user aggregations
│   │
│   └── marts/
│       ├── marketing/           # Customer analytics
│       │   └── mart_customer_360.sql    # RFM segmentation, loyalty tiers
│       ├── product/             # Product analytics
│       │   └── mart_product_performance.sql  # Reorder rates, rankings
│       └── core/                # Star schema dimensional model
│           ├── dim_dates.sql        # Calendar dimension (date_spine)
│           ├── dim_users.sql        # User dimension (SCD Type 1)
│           ├── dim_products.sql     # Product dimension (SCD Type 2)
│           ├── dim_departments.sql  # Department dimension
│           ├── fact_orders.sql      # Central fact table (INCREMENTAL)
│           ├── fact_order_products.sql  # Line item fact (INCREMENTAL)
│           └── _core_models.yml     # Tests + documentation for all core models
│
├── macros/
│   └── safe_divide.sql          # Reusable divide-by-zero protection macro
│
└── tests/
    └── assert_reorder_rate_between_0_and_1.sql  # Custom singular test
```

---

## Data Layers Explained

### Layer 1: Staging
**Purpose:** Clean and standardize raw source data. One model per source table. No joins between sources, no business logic.

**Materialization:** Views — always fresh, zero storage cost.

```sql
-- stg_orders.sql: cleaning only, no business logic
SELECT
    order_id::INT                       AS order_id,
    order_dow::INT                      AS order_day_of_week,
    CASE order_dow::INT
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
    END                                 AS order_day_name,
    CASE
        WHEN order_hour_of_day BETWEEN 5  AND 11 THEN 'Morning'
        WHEN order_hour_of_day BETWEEN 12 AND 16 THEN 'Afternoon'
        WHEN order_hour_of_day BETWEEN 17 AND 20 THEN 'Evening'
        ELSE 'Night'
    END                                 AS order_time_of_day,
    CASE WHEN days_since_prior_order IS NULL
         THEN TRUE ELSE FALSE END       AS is_first_order,
    CURRENT_TIMESTAMP                   AS loaded_at
FROM {{ source('raw', 'orders') }}
```

![stg_orders running](images/dbtaisles.png)

![stg_departments](images/dbtstgdepts.png) ![stg_products](images/dbtstgproducts.png)

### Layer 2: Intermediate
**Purpose:** Apply business logic. Join staging models and calculate per-user aggregations.

**Materialization:** Ephemeral — no table created. SQL inlined as a CTE into downstream models.

### Layer 3: Analytics Marts

![mart_customer_360 green](images/dbtmartproduct360green.png)

![mart_product_performance](images/dbtmartproductperformance.png)

### Layer 4: Core Dimensional Model

![fact_orders building](images/dbtfactorders.png)

![fact_order_products green](images/dbtfactorderproductsgreen.png)

---

## Data Quality & Testing

### Testing Pyramid

```
┌─────────────────────────────────────────┐
│   Custom Singular Tests                 │  ← Business assertions
│   assert_reorder_rate_between_0_and_1   │
├─────────────────────────────────────────┤
│   dbt Generic Tests                     │  ← Structural checks
│   unique, not_null, accepted_values,    │
│   relationships (referential integrity) │
├─────────────────────────────────────────┤
│   Source Freshness Tests                │  ← Data pipeline health
│   warn_after: 24h, error_after: 48h    │
└─────────────────────────────────────────┘
```

**Local (PostgreSQL):** `PASS=77 WARN=0 ERROR=0`

![dbt build green](images/dbtbuildrungreen.png)

**Production (Snowflake):** `PASS=86 WARN=0 ERROR=0`

![dbt prod green](images/dbtprodgreen.png)

---

## Environments — Dev vs Production

| Feature | Local Dev (PostgreSQL) | Production (Snowflake) |
|---|---|---|
| Cost | Free (Docker) | ~$0-5/month (X-SMALL, auto-suspend 60s) |
| Purpose | Development & testing | Final analytics, BI tools |
| Staging schema | `dbt_dev_staging` | `dbt_prod_staging` |
| Core schema | `dbt_dev_marts_core` | `dbt_prod_marts_core` |
| Materialization | Views (fast iteration) | Tables + Incremental |

```bash
dbt build               # → PostgreSQL (dev)
dbt build --target prod  # → Snowflake (prod)
```

### Snowflake Setup

![Tables loaded in Snowflake](images/tablesloadedintosnowflake.png)

> **Large file note:** `order_products__prior.csv` (550MB) exceeds Snowflake's 250MB limit.
> ```bash
> split -l 10000000 order_products__prior.csv prior_part_
> ```

![Split table script](images/splittablenamesscriptfororderproductsprior.png)
![Append to same table](images/appendtosametable.png)

![Snowflake tests passing](images/testpassonsnowflake.png)

---

## Orchestration — Apache Airflow

The pipeline runs automatically every day at **6 AM UTC (9 AM EAT)**:

```
run_staging → test_staging → run_intermediate → run_marts → test_marts → generate_docs
```

![Airflow containers running](images/airflowcontainers.png)

![Airflow started](images/airflowstarted.png)

![Airflow graph view](images/airflowgraphview.png)

---

## CI/CD — GitHub Actions

Every push to `main` automatically triggers:

```
Push → Spin up PostgreSQL → Install dbt → Compile → Build → ✅ Green tick
```

![GitHub Actions green tick](images/githubactionstick.png)

**Why `--exclude source:*`?** CI has no raw data — source tests are excluded. Model logic is fully validated. Source tests run in the production Airflow pipeline.

---

## Streamlit Dashboard

**Live dashboard:** [instacart-analytics-pipeline.streamlit.app](https://instacart-analytics-pipeline.streamlit.app/)

Connected directly to Snowflake production data. Refreshes every hour. No login required — anyone can view it.

### Dashboard Screenshots

![Title and Key Metrics](images/streamlittitleandkeymetrics.png)
*Key metrics: 3.4M orders, 206K customers, 9.9 avg basket size, 65% reorder rate*

![Customer Segments and Loyalty Tiers](images/streamlitcustomersegmentandloyaltytiers.png)
*RFM customer segmentation and loyalty tier distribution*

![Product Reorder Rate and Peak Ordering Hours](images/streamlitproductreorderrateandpeakorderinghours.png)
*Top 15 products by reorder rate and peak ordering hours area chart*

![Orders by Day of Week](images/ordersbydayofweek.png)
*Order volume by day — Sunday and Monday are peak days*

![Department Performance](images/streamlitdepartmentperformance.png)
*Bubble chart: department volume vs reorder rate vs unique orders*

![Customer Order Frequency](images/streamlitcustomerorderfrequency.png)
*How often customers order — majority are monthly or infrequent buyers*

### Running the Dashboard Locally

```bash
cd streamlit
streamlit run dashboard.py
# Opens at http://localhost:8501
```

---

## Setup Guide

### Prerequisites
- Python 3.11+
- Docker Desktop
- Git + VS Code
- Snowflake free trial account
- Kaggle dataset downloaded

### Step 1: Clone and set up

```bash
git clone https://github.com/Duncan610/instacart-analytics-pipeline.git
cd instacart-analytics-pipeline
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Step 2: Start PostgreSQL

```bash
docker compose up -d postgres
```

### Step 3: Create .env file

```bash
cp .env.example .env
# Edit .env with your credentials
```

### Step 4: Load raw data

```bash
python ingestion/load_to_postgres.py --data-dir /path/to/your/csvs
```

### Step 5: Configure dbt profiles

Create `~/.dbt/profiles.yml`:

```yaml
instacart_pipeline:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: analytics
      password: analytics
      dbname: instacart
      schema: dbt_dev
      threads: 4
    prod:
      type: snowflake
      account: YOUR_ACCOUNT_ID
      user: YOUR_USERNAME
      password: YOUR_PASSWORD
      role: TRANSFORMER
      database: TRANSFORM_DB
      warehouse: INSTACART_WH
      schema: dbt_prod
      threads: 4
```

### Step 6: Run the pipeline

```bash
cd dbt_project
dbt debug
dbt deps
dbt build
dbt docs generate && dbt docs serve
```

### Step 7: Deploy to Snowflake

```bash
dbt debug --target prod
dbt build --target prod
```

### Step 8: Start Airflow

```bash
docker compose up -d
# Open http://localhost:8080 → admin/admin
```

### Step 9: Run the Dashboard

```bash
cd streamlit
streamlit run dashboard.py
```

---

## Running the Pipeline

```bash
source venv/bin/activate
docker compose up -d postgres
cd dbt_project

dbt run --select dim_products          # single model
dbt run --select staging               # entire layer
dbt build                              # full build + tests
dbt build --full-refresh               # rebuild incrementals
dbt build --target prod                # deploy to Snowflake
dbt docs generate && dbt docs serve    # documentation
```

---

## Key Business Insights

**Customer Segmentation:**
```sql
SELECT customer_segment, COUNT(*) AS customers,
       ROUND(AVG(reorder_rate), 3) AS avg_reorder_rate
FROM dbt_prod_marts_marketing.mart_customer_360
GROUP BY customer_segment ORDER BY customers DESC;
```

**Top Reordered Products:**
```sql
SELECT p.product_name, p.department_name,
       ROUND(AVG(fop.is_reordered::INT), 3) AS reorder_rate
FROM dbt_prod_marts_core.fact_order_products fop
JOIN dbt_prod_marts_core.dim_products p ON fop.product_key = p.product_key
WHERE p.is_current = TRUE
GROUP BY p.product_name, p.department_name
ORDER BY reorder_rate DESC LIMIT 20;
```

**Peak Ordering Times:**
```sql
SELECT order_day_name, order_hour_of_day, COUNT(*) AS order_count
FROM dbt_prod_marts_core.fact_orders
GROUP BY order_day_name, order_hour_of_day
ORDER BY order_count DESC LIMIT 10;
```

---

## Challenges & Solutions

| Challenge | Root Cause | Solution |
|---|---|---|
| `INSERT 0 0` on incremental model | Empty shell table from failed prior run | `dbt build --full-refresh` |
| `MODE() WITHIN GROUP` error on Snowflake | PostgreSQL-specific syntax | Replaced with `MAX()` — cross-database compatible |
| `_loaded_at` column missing in Snowflake | Snowflake UI upload strips audit columns | Used `CURRENT_TIMESTAMP` as fallback |
| 550MB file exceeds Snowflake 250MB limit | Snowflake UI restriction | Split CSV with Linux `split` command |
| CI/CD cross-database reference error | `database: RAW_DB` invalid for PostgreSQL | Removed database override from sources.yml |
| `accepted_values` deprecation warning | dbt 1.11 changed test syntax | Nested values under `arguments` key |
| Airflow webserver timeout | Insufficient RAM for 4 gunicorn workers | Reduced to 1 worker, increased master timeout |
| Streamlit Cloud missing packages | No `requirements.txt` in app folder | Created `streamlit/requirements.txt` |

---

## What I Learned

**Analytics Engineering**
- Kimball star schema design from scratch
- SCD Type 1 vs Type 2 — choosing correctly for each dimension
- Layered dbt architecture: staging → intermediate → marts → core
- Incremental models and full-refresh strategies

**Modern Data Stack**
- ELT over ETL — why the industry shifted
- Multi-environment dbt (PostgreSQL dev → Snowflake prod)
- Cross-database SQL compatibility (PostgreSQL vs Snowflake syntax)
- Surrogate keys vs natural keys

**Software Engineering for Data**
- CI/CD with GitHub Actions — automated testing on every push
- Docker Compose for reproducible environments
- Data quality testing pyramid
- Public dashboard deployment with Streamlit Cloud

**Production Debugging**
- Reading compiled SQL to diagnose dbt failures
- Debugging incremental model state issues
- Docker container resource management
- Snowflake role-based access control setup

---

## License

MIT — see LICENSE for details.

---

*Built by Duncan Otieno | [LinkedIn](https://linkedin.com/in/duncan-otieno) | [GitHub](https://github.com/Duncan610)*

> ⭐ If you found this project useful for your own learning, please consider starring the repository!
