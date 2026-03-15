# 🛒 Instacart Analytics Engineering Pipeline

> A production-grade ELT data pipeline built on the modern data stack — featuring Kimball dimensional modeling (star schema with SCD Type 1 & 2), dbt transformations across 14 models, Apache Airflow orchestration, PostgreSQL for local development, Snowflake for cloud production, and GitHub Actions CI/CD.


[![dbt Build](https://img.shields.io/badge/dbt%20build-86%2F86%20passing-brightgreen)](https://github.com/Duncan610/instacart-analytics-pipeline/actions)
[![CI](https://github.com/Duncan610/instacart-analytics-pipeline/actions/workflows/dbt_ci.yml/badge.svg)](https://github.com/Duncan610/instacart-analytics-pipeline/actions)
[![Snowflake](https://img.shields.io/badge/Snowflake-Production-29B5E8)](https://www.snowflake.com/)
[![dbt](https://img.shields.io/badge/dbt-1.8+-orange)](https://www.getdbt.com/)
[![Airflow](https://img.shields.io/badge/Airflow-2.9.3-017CEE)](https://airflow.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.12-3776AB)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791)](https://www.postgresql.org/)

---

## 📋 Table of Contents

1. [Project Overview](#-project-overview)
2. [Business Problem](#-business-problem)
3. [Architecture](#-architecture)
4. [Tech Stack & Why Each Tool](#-tech-stack--why-each-tool)
5. [Data Sources](#-data-sources)
6. [ELT Pattern — Not ETL](#-elt-pattern--not-etl)
7. [Dimensional Modeling — Star Schema](#-dimensional-modeling--star-schema)
8. [SCD Types Implemented](#-scd-types-implemented)
9. [dbt Project Structure](#-dbt-project-structure)
10. [Data Layers Explained](#-data-layers-explained)
11. [Data Quality & Testing](#-data-quality--testing)
12. [Environments — Dev vs Production](#-environments--dev-vs-production)
13. [Airflow Orchestration](#-airflow-orchestration)
14. [CI/CD — GitHub Actions](#-cicd--github-actions)
15. [Setup Guide](#-setup-guide)
16. [Running the Pipeline](#-running-the-pipeline)
17. [Key Business Insights](#-key-business-insights)
18. [Challenges & Solutions](#-challenges--solutions)
19. [What I Learned](#-what-i-learned)

---

## 🎯 Project Overview

This project builds a **production-grade ELT analytics pipeline** on the [Instacart Online Grocery Shopping Dataset](https://www.kaggle.com/datasets/psparks/instacart-market-basket-analysis) — one of the most realistic public datasets available, with **3.4 million orders, 206,000 users, 49,688 products, and 37 million total rows** across 6 CSV files.

The pipeline follows the modern **ELT paradigm** — Extract, Load, then Transform inside the database:
- **Extract & Load** — Python ingestion script (pandas + SQLAlchemy) reads raw CSVs and loads them into PostgreSQL with audit columns
- **Transform** — dbt models clean, join, aggregate, and model the data into a production star schema
- **Serve** — Snowflake hosts the production dimensional model for BI tools and analytics

The final output is a fully tested, documented, CI/CD-enabled **Kimball star schema** with 2 fact tables and 4 dimension tables answering real business questions about customer behavior, product performance, and order patterns.

---

## 💼 Business Problem

Instacart's data team needs a reliable, tested, and well-documented data model to answer critical business questions:

| Business Question | Model That Answers It |
|---|---|
| Who are our Champions vs At Risk customers? | `dim_users` + `fact_orders` |
| Which products have the highest reorder rates? | `fact_order_products` + `dim_products` |
| What are peak ordering hours by day of week? | `fact_orders` + `dim_dates` |
| How does basket size vary by customer segment? | `fact_orders` + `dim_users` |
| Which departments drive the most order volume? | `fact_order_products` + `dim_departments` |
| Which products are always added first to cart (habitual staples)? | `fact_order_products` |

---

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    DATA SOURCES                           │
│    Kaggle Instacart Dataset — 6 CSV files, 37M+ rows     │
└─────────────────────┬────────────────────────────────────┘
                      │
         Python Ingestion Script
         pandas + SQLAlchemy
         Chunked loading (100K rows/batch)
         Audit columns: _loaded_at, _source_file
                      │
          ┌───────────▼──────────────┐
          │   LOCAL DEVELOPMENT       │
          │   PostgreSQL 16 (Docker)  │  ← raw.* schema
          └───────────┬──────────────┘
                      │
              dbt Transformations
              14 models, 73+ tests
                      │
     ┌────────────────┼────────────────┐
     │                │                │
  Staging         Intermediate       Marts
  Layer           Layer              Layer
  (5 views)       (1 ephemeral)      (dims + facts)
     │                │                │
     └────────────────┴────────────────┘
                      │
          ┌───────────▼──────────────┐
          │   PRODUCTION              │
          │   Snowflake               │
          │   TRANSFORM_DB            │  ← dbt_prod_* schemas
          │   86/86 tests passing     │
          └───────────┬──────────────┘
                      │
          ┌───────────▼──────────────┐
          │   ORCHESTRATION           │
          │   Apache Airflow 2.9.3    │
          │   Daily 6AM UTC (9AM EAT) │
          │   Docker Compose          │
          └───────────┬──────────────┘
                      │
          ┌───────────▼──────────────┐
          │   CI/CD                   │
          │   GitHub Actions          │
          │   Runs on every push      │
          │   Green tick ✅           │
          └──────────────────────────┘
```

---

## 🛠️ Tech Stack & Why Each Tool

| Tool | Role | Why This Tool |
|---|---|---|
| **dbt Core 1.8+** | SQL transformation & modeling | Industry standard for analytics engineering. SQL-first, version-controlled, self-documenting, built-in testing. Brings software engineering practices to SQL. |
| **Snowflake** | Production data warehouse | Separates compute from storage. Scales to petabytes. Dominant in modern data stacks. X-SMALL warehouse with 60s auto-suspend keeps costs near zero. |
| **PostgreSQL 16** | Local development database | Free. Docker-based. SQL-compatible with Snowflake for fast local iteration without cloud costs. |
| **Apache Airflow 2.9.3** | Workflow orchestration | DAG-based scheduling with retry logic, Slack alerting, and task dependency management. Industry standard at data-mature companies. |
| **Python 3.12** | Data ingestion | Pandas chunked reading (100K rows/chunk) handles 32M-row files without memory issues. SQLAlchemy for database-agnostic connections. |
| **Docker + Docker Compose** | Environment management | Reproducible local PostgreSQL + Airflow setup. One command starts the entire local stack. |
| **GitHub Actions** | CI/CD | Automated dbt testing on every push. Validates all models compile and build correctly before reaching production. |
| **dbt-utils** | SQL utilities | Surrogate key generation via `generate_surrogate_key()`, date spine generation for `dim_dates`. |

---

## 📊 Data Sources

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

## 🏛️ Dimensional Modeling — Star Schema

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
                     │ ────────────── │  │ valid_from      ←─── SCD Type 2
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

## 🔄 SCD Types Implemented

### SCD Type 1 — `dim_users` (Overwrite)

User behavioral attributes (segment, loyalty tier, reorder rate) are **recalculated fresh on every pipeline run** from `fact_orders`. Overwriting is correct here because we can always recompute the past from raw order data. Keeping history adds no analytical value.

```sql
-- Type 1: current row simply overwritten on each run
user_key | user_id | customer_segment | reorder_rate | updated_at
abc123   | 1       | Champion         | 0.73         | 2024-03-16  ← today's values, always current
```

### SCD Type 2 — `dim_products` (Historical Rows)

Product attributes (name, department) could change over time. SCD Type 2 preserves full history by **adding a new row** for each change, enabling point-in-time queries.

```sql
-- Type 2: new row added when product changes, old row preserved
product_key | product_id | product_name  | dept_name  | valid_from | valid_to   | is_current
abc123      | 1          | Organic Milk  | Dairy      | 2019-01-01 | 2019-06-01 | FALSE  ← historical
def456      | 1          | Organic Milk  | Beverages  | 2019-06-01 | NULL       | TRUE   ← current
```

Query current products only:
```sql
SELECT * FROM dim_products WHERE is_current = TRUE;
```

Query what department a product was in on a specific past date:
```sql
SELECT * FROM dim_products
WHERE product_id = 1
  AND valid_from <= '2019-03-01'
  AND (valid_to > '2019-03-01' OR valid_to IS NULL);
```

---

## 📁 dbt Project Structure

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

## 🗂️ Data Layers Explained

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
        -- ...
    END                                 AS order_day_name,
    CASE
        WHEN order_hour_of_day BETWEEN 5  AND 11 THEN 'Morning'
        WHEN order_hour_of_day BETWEEN 12 AND 16 THEN 'Afternoon'
        WHEN order_hour_of_day BETWEEN 17 AND 20 THEN 'Evening'
        ELSE 'Night'
    END                                 AS order_time_of_day,
    CASE WHEN days_since_prior_order IS NULL THEN TRUE ELSE FALSE END AS is_first_order,
    CURRENT_TIMESTAMP                   AS loaded_at
FROM {{ source('raw', 'orders') }}
```

![stg_orders running](images/dbtaisles.png)
*Staging models building successfully*

![stg_departments](images/dbtstgdepts.png) ![stg_products](images/dbtstgproducts.png)

### Layer 2: Intermediate
**Purpose:** Apply business logic. Join staging models and calculate per-user aggregations.

**Materialization:** Ephemeral — no table created. SQL inlined as a CTE into downstream models. Saves storage while keeping logic modular.

`int_customer_order_history.sql` calculates: total orders, avg days between orders, avg basket size, reorder rate, pct items added early to cart — all per user.

### Layer 3: Analytics Marts
**Purpose:** Subject-area specific aggregations for business teams.

`mart_customer_360.sql` — Uses `NTILE(4)` window functions to calculate RFM (Recency, Frequency, Monetary) scores and assigns customer segments:

![mart_customer_360 green](images/dbtmartproduct360green.png)

`mart_product_performance.sql` — Product reorder rates, cart position analysis, popularity rankings within departments:

![mart_product_performance](images/dbtmartproductperformance.png)

### Layer 4: Core Dimensional Model
**Purpose:** The production star schema. Fact and dimension tables for BI tools.

**Materialization:** Tables (fast query performance) + Incremental (only new rows processed on each daily run).

![fact_orders building](images/dbtfactorders.png)
*fact_orders processing 3.4M rows*

![fact_order_products green](images/dbtfactorderproductsgreen.png)
*fact_order_products — 33.8M rows*

---

## ✅ Data Quality & Testing

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

### Test Results

**Local (PostgreSQL):** `PASS=77 WARN=0 ERROR=0`

![dbt build green](images/dbtbuildrungreen.png)

**Production (Snowflake):** `PASS=86 WARN=0 ERROR=0`

![dbt prod green](images/dbtprodgreen.png)

### Example Tests

```yaml
# Generic tests in _core_models.yml
- name: fact_orders
  columns:
    - name: order_id
      tests: [unique, not_null]           # every order has a unique ID
    - name: user_key
      tests:
        - relationships:
            to: ref('dim_users')
            field: user_key               # referential integrity to dim_users
    - name: total_items
      tests: [not_null]                   # every order has at least 0 items

# Custom singular test
-- Returns rows that FAIL (dbt expects 0 rows = PASS)
SELECT user_id, reorder_rate
FROM {{ ref('mart_customer_360') }}
WHERE reorder_rate < 0 OR reorder_rate > 1
```

---

## 🌍 Environments — Dev vs Production

| Feature | Local Dev (PostgreSQL) | Production (Snowflake) |
|---|---|---|
| Cost | Free (Docker) | ~$0-5/month (X-SMALL, auto-suspend 60s) |
| Purpose | Development & testing | Final analytics, BI tools |
| Staging schema | `dbt_dev_staging` | `dbt_prod_staging` |
| Core schema | `dbt_dev_marts_core` | `dbt_prod_marts_core` |
| Materialization | Views (fast iteration) | Tables + Incremental |
| Auth | Username/password | Username/password (role: TRANSFORMER) |

```bash
dbt build              # → PostgreSQL (dev)
dbt build --target prod # → Snowflake (prod)
```

### Snowflake Setup

Tables loaded into Snowflake RAW_DB:

![Tables loaded in Snowflake](images/tablesloadedintosnowflake.png)

> **Note on large files:** `order_products__prior.csv` (550MB) exceeds Snowflake's 250MB upload limit. Solution: split the file into parts using the Linux `split` command, upload each part, and append to the same table.

![Split table script](images/splittablenamesscriptfororderproductsprior.png)
![Append to same table](images/appendtosametable.png)

Production tests passing on Snowflake:

![Snowflake tests passing](images/testpassonsnowflake.png)

---

## ⚙️ Orchestration — Apache Airflow

The pipeline runs automatically every day at **6 AM UTC (9 AM EAT)**:

```
run_staging → test_staging → run_intermediate → run_marts → test_marts → generate_docs
```

Each task has: 2 retries, 5-minute retry delay.

### Airflow Running

![Airflow containers running](images/airflowcontainers.png)
*All 4 Docker containers: postgres, airflow-init, airflow-webserver, airflow-scheduler*

![Airflow started](images/airflowstarted.png)
*Airflow UI at localhost:8080*

![Airflow graph view](images/airflowgraphview.png)
*DAG graph view showing task dependencies*

### Starting the Full Stack

```bash
docker compose up -d
# Starts: instacart-postgres, airflow-init, airflow-webserver, airflow-scheduler
```

Open `http://localhost:8080` → login `admin/admin` → enable `daily_instacart_pipeline` → Trigger DAG.

---

## 🚀 CI/CD — GitHub Actions

Every push to `main` automatically triggers the CI pipeline:

```
Push to main
     ↓
Spin up Ubuntu + PostgreSQL container
     ↓
Install dbt-postgres
     ↓
Create CI profiles.yml (points to CI postgres)
     ↓
Create raw schema + empty tables in CI database
     ↓
dbt deps (install packages)
     ↓
dbt compile (syntax check all 14 models)
     ↓
dbt build --exclude source:* (build + test all models)
     ↓
✅ Green tick
```

![GitHub Actions green tick](images/githubactionstick.png)

### Key CI/CD Design Decisions

**Why `--exclude source:*`?**
The CI database has no raw data. Source tests require actual data to pass. By excluding source tests in CI, we still validate all model SQL compiles and builds correctly — which is the purpose of CI. Source tests run in the full production pipeline via Airflow.

**Why a separate CI database?**
CI uses `instacart_ci` as the database name — completely isolated from dev and prod. This prevents CI runs from interfering with local development or production data.

---

## 🖥️ Setup Guide

### Prerequisites
- Python 3.11+
- Docker Desktop (running)
- Git
- VS Code
- Snowflake free trial account (snowflake.com)
- Kaggle dataset downloaded

### Step 1: Clone and set up environment

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
docker ps   # verify instacart-postgres is running
```

### Step 3: Create .env file

```bash
# Create .env in project root — never committed to GitHub
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=instacart
POSTGRES_USER=your_username
POSTGRES_PASSWORD=your_password
```

### Step 4: Load raw data

```bash
python ingestion/load_to_postgres.py
# Default path: /home/otieno/Downloads/instacart
# To specify a different path:
python ingestion/load_to_postgres.py --data-dir /your/path/to/csvs
# Takes 10-20 minutes for all 6 files (37M+ rows total)
```

### Step 5: Configure dbt profiles

Create `~/.dbt/profiles.yml` (outside project — never committed to GitHub):

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
      account: YOUR_ACCOUNT_ID   # e.g. atgffjl-qeb15691
      user: YOUR_USERNAME
      password: YOUR_PASSWORD
      role: TRANSFORMER
      database: TRANSFORM_DB
      warehouse: INSTACART_WH
      schema: dbt_prod
      threads: 4
```

### Step 6: Run the full pipeline locally

```bash
cd dbt_project
dbt debug            # verify connection
dbt deps             # install packages
dbt build            # run all 14 models + 77 tests
dbt docs generate    # generate documentation
dbt docs serve       # view at http://localhost:8080
```

### Step 7: Snowflake setup

In Snowflake UI → Worksheets, run:

```sql
CREATE WAREHOUSE INSTACART_WH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

CREATE DATABASE RAW_DB;
CREATE DATABASE TRANSFORM_DB;

CREATE ROLE TRANSFORMER;
USE ROLE ACCOUNTADMIN;
GRANT USAGE ON WAREHOUSE INSTACART_WH TO ROLE TRANSFORMER;
GRANT ALL PRIVILEGES ON DATABASE RAW_DB TO ROLE TRANSFORMER;
GRANT ALL PRIVILEGES ON DATABASE TRANSFORM_DB TO ROLE TRANSFORMER;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE RAW_DB TO ROLE TRANSFORMER;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE TRANSFORM_DB TO ROLE TRANSFORMER;
GRANT ALL PRIVILEGES ON FUTURE SCHEMAS IN DATABASE TRANSFORM_DB TO ROLE TRANSFORMER;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN DATABASE TRANSFORM_DB TO ROLE TRANSFORMER;
GRANT ROLE TRANSFORMER TO USER YOUR_USERNAME;
```

Load raw CSVs via Snowflake UI → Upload local files → RAW_DB → RAW schema.

> **Large file note:** `order_products__prior.csv` (550MB) exceeds the 250MB limit. Split it first:
> ```bash
> split -l 10000000 order_products__prior.csv prior_part_
> ```
> Upload each part and append to `order_products_prior` table.

### Step 8: Deploy to Snowflake production

```bash
cd dbt_project
dbt debug --target prod    # verify Snowflake connection
dbt build --target prod    # deploy all 14 models to Snowflake
```

### Step 9: Start Airflow

```bash
# From project root
docker compose up -d
# Wait 3 minutes, then open http://localhost:8080
# Login: admin / admin
# Enable daily_instacart_pipeline DAG → Trigger DAG
```

---

## ▶️ Running the Pipeline

```bash
# Start your session (every time you open a new terminal)
source venv/bin/activate
cd instacart-analytics-pipeline
docker compose up -d postgres
cd dbt_project

# Run individual model
dbt run --select dim_products

# Run entire layer
dbt run --select staging
dbt run --select marts

# Full build with tests
dbt build

# Force rebuild incremental models from scratch
dbt build --full-refresh

# Production deployment
dbt build --target prod

# Generate and view documentation
dbt docs generate
dbt docs serve
```

---

## 💡 Key Business Insights

Run these queries in pgAdmin or Snowflake to see real results:

**Customer Segmentation Distribution:**
```sql
SELECT customer_segment, COUNT(*) AS customers,
       ROUND(AVG(reorder_rate), 3) AS avg_reorder_rate,
       ROUND(AVG(avg_basket_size), 1) AS avg_basket_size
FROM dbt_prod_marts_marketing.mart_customer_360
GROUP BY customer_segment
ORDER BY customers DESC;
```

**Top Reordered Products:**
```sql
SELECT p.product_name, p.department_name,
       COUNT(*) AS times_ordered,
       ROUND(AVG(fop.is_reordered::INT), 3) AS reorder_rate
FROM dbt_prod_marts_core.fact_order_products fop
JOIN dbt_prod_marts_core.dim_products p ON fop.product_key = p.product_key
WHERE p.is_current = TRUE
GROUP BY p.product_name, p.department_name
ORDER BY reorder_rate DESC
LIMIT 20;
```

**Peak Ordering Times:**
```sql
SELECT d.day_name, fo.order_hour_of_day,
       COUNT(*) AS order_count
FROM dbt_prod_marts_core.fact_orders fo
JOIN dbt_prod_marts_core.dim_dates d ON fo.date_key = d.day_of_week_number
GROUP BY d.day_name, fo.order_hour_of_day
ORDER BY order_count DESC
LIMIT 10;
```

---

## 🔧 Challenges & Solutions

Throughout this build, several real-world production problems were encountered and solved:

| Challenge | Root Cause | Solution |
|---|---|---|
| `INSERT 0 0` on incremental model | Table existed as empty shell from failed prior run | `dbt build --full-refresh` to rebuild from scratch |
| `MODE() WITHIN GROUP` error on Snowflake | PostgreSQL-specific syntax not supported in Snowflake | Replaced with `MAX()` — cross-database compatible |
| `_loaded_at` column not found in Snowflake | Snowflake UI upload strips audit columns vs Python script which adds them | Used `CURRENT_TIMESTAMP` as fallback in `stg_orders.sql` |
| 550MB file exceeds Snowflake 250MB upload limit | Snowflake UI file size restriction | Split CSV with Linux `split` command, uploaded in parts |
| CI/CD cross-database reference error | `database: RAW_DB` in sources.yml not valid for PostgreSQL | Removed database override from sources.yml; CI creates its own raw schema |
| `accepted_values` test deprecation warning | dbt 1.11 changed test syntax — `arguments` key now required | Updated all test configs to nest values under `arguments` |

---

## 🎓 What I Learned

**Analytics Engineering Fundamentals**
- Designing and implementing a Kimball star schema from scratch
- Choosing the right SCD type for each dimension and articulating why
- The difference between fact tables (events/measures) and dimension tables (context/filters)
- Layered dbt architecture: staging → intermediate → marts → core

**Modern Data Stack**
- ELT vs ETL — why ELT dominates modern analytics engineering
- Multi-environment dbt profiles (local PostgreSQL dev → Snowflake prod)
- Incremental models — processing only new data on each pipeline run
- Surrogate keys vs natural keys in dimensional modeling

**Software Engineering for Data**
- Git branching and version control for data projects
- CI/CD with GitHub Actions — automated testing on every push
- Docker Compose for reproducible development environments
- Data quality testing pyramid (generic → singular tests)
- Cross-database compatibility between PostgreSQL and Snowflake

**Production Debugging**
- Reading dbt error messages and compiled SQL to diagnose failures
- Handling cross-database SQL syntax differences
- Debugging incremental model state issues with `--full-refresh`
- Resolving Docker container dependency and startup order issues

---

## 📄 License

MIT — see LICENSE for details.

---

*Built by Duncan Otieno | [LinkedIn](https://linkedin.com/in/duncan-otieno) | [GitHub](https://github.com/Duncan610)*

> ⭐ If you found this project useful for your own learning, please consider starring the repository!
