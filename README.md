# Ecommerce Olist Data Pipeline

An end-to-end batch + streaming data engineering pipeline built on Azure Databricks, dbt, and Power BI — using the Olist Brazilian ecommerce dataset plus a self-built synthetic streaming order feed.

![Architecture Diagram](docs/architecture-diagram.png)
<img width="916" height="385" alt="bilde" src="https://github.com/user-attachments/assets/6fa83bcd-86f6-4b4c-8f23-82f6bada3596" />


This project demonstrates a full medallion architecture pipeline — raw ingestion, transformation, testing, and visualization — with both a historical batch dataset and a live-simulated streaming leg unified into a single analytics layer, deployed via CI/CD.

## Table of Contents

- [Tech Stack](#tech-stack)
- [Architecture](#architecture)
- [The Streaming Component](#the-streaming-component)
- [CI/CD Pipeline](#cicd-pipeline)
- [Data Quality & Testing](#data-quality--testing)
- [Dashboards & Key Findings](#dashboards--key-findings)
- [Challenges & How They Were Solved](#challenges--how-they-were-solved)
- [Repo Structure](#repo-structure)
- [Setup: Running This Yourself](#setup-running-this-yourself)

## Tech Stack

| Layer | Tool |
|---|---|
| Cloud platform | Azure |
| Storage | Azure Data Lake Storage Gen2 |
| Data platform | Databricks (Lakeflow Declarative Pipelines, Unity Catalog) |
| Streaming ingestion | Databricks Auto Loader (continuous mode) |
| Transformation | dbt Core |
| Orchestration / CI-CD | GitHub Actions (Databricks Asset Bundles) |
| Visualization | Power BI |

## Architecture

The pipeline follows a **bronze → silver → gold** medallion architecture:

**Bronze** — raw ingestion, no transformation
- 6 batch tables (customers, orders, order_items, order_payments, products, product_category) loaded via `spark.read` + `@dp.materialized_view`
- 1 streaming table, `new_orders`, loaded via `spark.readStream` with Auto Loader (`cloudFiles`) in **continuous mode**, simulating live order arrivals

**Silver** — cleaned, typed, and joined
- 8 tables built via Lakeflow `@dp.table`: category, customer, order_items (with seller_id), order_payments, orders, product, sellers, new_orders

**Gold** — dimensional star schema, built with dbt
- Dimensions: `dim_customers`, `dim_products`, `dim_sellers`, `dim_date` (a generated calendar spine)
- Facts: `fact_orders`, `fact_order_items`, `fact_payments`
- Built on 8 dbt staging models sitting on top of a `sources.yml` declaring the silver schema

## The Streaming Component

Rather than a purely batch pipeline, this project includes a self-built synthetic streaming feed (`new_orders`) ingested continuously via Auto Loader. The gold-layer fact tables (`fact_orders`, `fact_order_items`) union the batch and streaming legs together, tagged with a `source_type` column (`batch` / `streaming`), so downstream consumers — including the Power BI dashboards — see one unified view of order activity regardless of where it originated.

## CI/CD Pipeline

Defined in `.github/workflows/deploy.yml`, triggered on every push to `main`:

1. **Deploy job** — deploys the Databricks Asset Bundle (`databricks.yml`), which defines and updates the Lakeflow pipeline, authenticating as a service principal via OAuth M2M.
2. **dbt job** — runs after deploy succeeds: installs `dbt-databricks`, generates `profiles.yml` on the fly using OAuth M2M credentials, then runs `dbt deps`, `dbt run`, and `dbt test` against the gold schema.

No manual steps are required after a push — both jobs run fully automated, and a green pipeline means the bundle is deployed **and** the gold layer has rebuilt and passed its tests.

## Data Quality & Testing

The gold layer is covered by dbt tests defined in `models/marts/schema.yml`:

- `unique` and `not_null` constraints on primary keys
- `relationships` tests validating every fact-to-dimension foreign key
- `accepted_values` tests on categorical columns
- `dbt_utils.accepted_range` tests on numeric columns
- A custom test, `tests/assert_delivery_after_purchase.sql`, asserting that no order's delivery date precedes its purchase date

All tests pass as part of the CI/CD `dbt test` step on every push.

## Dashboards & Key Findings

Three Power BI report pages, connected live to the gold schema via the Databricks SQL Warehouse:

### Overview
![Overview Page](docs/screenshot-overview.png)
<img width="982" height="552" alt="bilde" src="https://github.com/user-attachments/assets/0eec12f5-7767-4486-ae15-975774e124de" />

Revenue trend, total orders, average order value, and order volume by payment method.

### Top Products & Categories
![Top Products Page](docs/screenshot-top-products.png)
<img width="1037" height="711" alt="bilde" src="https://github.com/user-attachments/assets/4d5db4a5-63de-4075-b5b0-b05848422c7c" />

Revenue and unit volume by product category, with a product-level detail table. **Finding:** Bed Bath Table is the #1 category by units sold but only #3 by revenue — Health Beauty earns more overall despite lower volume, suggesting a higher average price point per item.

### Delivery Performance
![Delivery Performance Page](docs/screenshot-delivery.png)
<img width="1111" height="728" alt="bilde" src="https://github.com/user-attachments/assets/dede093e-c9fa-40de-8b67-c7b5ffbca53e" />

On-time delivery rate, average delivery time, and average delay (for late orders), broken down by month and by customer state. **Findings:**
- Overall on-time delivery rate: **91.92%**
- On-time rate dips to ~80-83% around February-March and November, likely tied to post-holiday order backlogs and Black Friday demand, versus ~95-98% the rest of the year
- Remote northern Brazilian states (RR, AP, AM) see roughly 3x longer average delivery times than São Paulo (SP), consistent with real Brazilian logistics geography

## Challenges & How They Were Solved

Building this pipeline surfaced a few real data engineering problems worth calling out — not just building the happy path, but debugging it:

**Datetime vs. date join mismatch.** `fact_orders.purchase_date` was stored as a `Date/time` type with a time-of-day component, while `dim_date.date_day` was a plain date. Since relationships match on exact value equality, this meant almost no orders joined to the date dimension — 99.9% of revenue sat in an unmatched "blank" bucket. Fixed by casting `purchase_date` to `DATE` in the dbt model.

**A date dimension that couldn't keep up with live data.** `dim_date`'s calendar spine was originally a fixed range ending in 2019, but the streaming leg generates orders with the current system date — meaning the spine fell further behind every day. Fixed by changing the dbt date spine's end bound from a hardcoded date to `date_add(current_date(), 365)`, so it self-extends on every rebuild rather than needing manual updates.

**A silent DAX bug inflating an on-time delivery KPI to 100%.** An "on-time deliveries" measure had been copy-pasted from a different measure and was missing its actual date comparison logic, so it was silently counting all delivered orders as on-time. Caught by cross-checking it against an independent "late orders" test measure that returned a nonzero count — proving the two couldn't both be right. Fixed the DAX; the real on-time rate is 91.92%.

## Repo Structure

```
.
├── .github/workflows/                     # CI/CD: bundle deploy + dbt run/test
├── dbt_ecommerce_olist/                    # dbt project (gold layer)
│   ├── analyses/
│   ├── macros/
│   ├── models/                             # staging models + gold marts (dims/facts) + schema.yml
│   ├── seeds/
│   ├── snapshots/
│   ├── tests/                              # custom dbt tests (e.g. assert_delivery_after_purchase.sql)
│   ├── dbt_project.yml
│   ├── packages.yml                        # dbt_utils dependency
│   └── package-lock.yml
├── ecommerce_pipeline/
│   └── transformations/                    # Lakeflow Declarative Pipeline code (bronze + silver)
│       ├── bronze/                         # raw ingestion (one script per source table + new_orders streaming)
│       │   ├── category.py
│       │   ├── customers.py
│       │   ├── new_orders.py               # streaming ingestion via Auto Loader
│       │   ├── order_items.py
│       │   ├── order_payments.py
│       │   ├── orders.py
│       │   ├── products.py
│       │   └── sellers.py
│       └── silver/                         # cleaned/typed/joined tables (same file layout as bronze)
├── databricks.yml                          # Databricks Asset Bundle definition
└── README.md
```

## Setup: Running This Yourself

### Prerequisites

- An Azure subscription
- A Databricks workspace with Unity Catalog enabled
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) installed and configured
- [dbt Core](https://docs.getdbt.com/docs/core/installation) with the `dbt-databricks` adapter
- Power BI Desktop
- A GitHub account (for CI/CD via GitHub Actions)

### 1. Set up Azure storage

1. Create an Azure Data Lake Storage Gen2 account.
2. Create a container (e.g. `sourcedata`) with one folder per source table: `customers/`, `orders/`, `order_items/`, `order_payments/`, `products/`, `product_category/`, and `new_orders/` for the streaming leg.
3. Upload the [Olist Brazilian ecommerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) CSVs into their respective folders.

### 2. Set up Databricks and Unity Catalog

1. Create a Unity Catalog catalog (e.g. `ecommerce_olist`) with `bronze`, `silver`, and `gold` schemas.
2. Create an external location pointing at your ADLS container, and grant `READ FILES` on it.
3. Create a SQL Warehouse for querying the gold layer (used later by Power BI).
4. Create a service principal for CI/CD, and grant it: `USE CATALOG`, `READ FILES` on the external location, `CREATE MATERIALIZED VIEW`, `SELECT`/`MODIFY`/`CREATE TABLE` on the gold schema, and **"Can use"** on the SQL Warehouse.
5. Generate an OAuth M2M client ID and secret for the service principal.

### 3. Configure the repo

1. Clone this repository.
2. Update `databricks.yml` with your workspace URL, catalog name, and any environment-specific settings.
3. In your GitHub repo settings, add these secrets: `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`.

### 4. Deploy the bronze + silver pipeline

Push to `main` — GitHub Actions will automatically deploy the Databricks Asset Bundle. Note: deploying the bundle updates the *pipeline definition* but does not automatically run it. To actually populate the tables, trigger a run manually:

```bash
databricks bundle run ecommerce_pipeline -t dev
```

(or trigger it from the Databricks UI under Jobs & Pipelines).

### 5. Build the gold layer with dbt

The CI/CD `dbt` job runs this automatically after every successful deploy. To run it locally instead:

```bash
pip install dbt-databricks
dbt deps
dbt run
dbt test
```

You'll need a local `profiles.yml` pointing at your SQL Warehouse's HTTP path, using either a personal access token or OAuth.

### 6. Connect Power BI

1. Open Power BI Desktop.
2. **Get Data** → search for **Azure Databricks**.
3. Enter your SQL Warehouse's server hostname and HTTP path.
4. Authenticate (Azure AD or a personal access token).
5. Select the tables under your `gold` schema and load them in.
6. Rebuild the relationships between fact and dimension tables as described in the [Architecture](#architecture) section above.

