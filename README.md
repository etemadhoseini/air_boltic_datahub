# Air Boltic — Analytics Engineering Project (dbt + Databricks)

This repository implements a production‑grade analytics stack for the hypothetical **Air Boltic** marketplace, using **S3 → Databricks → dbt → Looker**. It is the deliverable for the *Analytics Engineer Home Task* and showcases canonical modeling patterns (sources → staging → curated → marts), strong testing, and practical CI/CD.

> The original task brief is included in the repo for reference (see `Analytics Engineer Home Task_ Air Boltic.pdf`).

---

## Contents

- [Air Boltic — Analytics Engineering Project (dbt + Databricks)](#air-boltic--analytics-engineering-project-dbt--databricks)
  - [Contents](#contents)
  - [Architecture \& layers](#architecture--layers)
  - [Data loading (`data/`)](#data-loading-data)
  - [Modeling](#modeling)
    - [Sources](#sources)
    - [Staging](#staging)
    - [Curated (Star Schema)](#curated-star-schema)
    - [Mart (One Big Table)](#mart-one-big-table)
  - [Testing \& quality gates](#testing--quality-gates)
  - [Incremental strategy \& backfills](#incremental-strategy--backfills)
  - [CI/CD](#cicd)
    - [CI (pull requests)](#ci-pull-requests)
  - [Project structure](#project-structure)
  - [How to run locally](#how-to-run-locally)
    - [1) Install dependencies](#1-install-dependencies)
    - [2) Configure credentials](#2-configure-credentials)
    - [3) Load sample data](#3-load-sample-data)
    - [4) Build models](#4-build-models)
    - [5) Backfills (as needed)](#5-backfills-as-needed)
  - [Design decisions](#design-decisions)
  - [Future work](#future-work)
  - [Acknowledgements](#acknowledgements)

---

## Architecture & layers

```
S3 (raw files) → Databricks (Delta) → dbt models → Looker (explore)
                        │
                        ├─ sources    (raw schemas w/ freshness SLOs)
                        ├─ staging    (contracts, renames, types, light cleaning)
                        ├─ curated    (star: dims + fact, incremental fact)
                        └─ marts      (one big table for BI)
```

- **Data governance**: contracts enforce schemas/types; tests guard assumptions.
- **Performance**: incremental strategies on operationally changing tables; clustering by date for large facts/marts.
- **Developer ergonomics**: macros, tags, slim CI, artifact deferral in CD.

---

## Data loading (`data/`)

The `data/` folder contains a small Python utility that:
1) Creates **Unity Catalog** catalogs/schemas required by the project.  
2) Uploads the initial sample data (JSON/CSV) into Delta tables under the **raw** catalog.  
3) Can be re‑run idempotently to refresh raw test datasets during development.

> Run it once before building dbt models so the `source()` references resolve. The script expects Databricks credentials (PAT) and workspace URL via environment variables.

---

## Modeling

### Sources

Raw tables are declared with **freshness** and light constraints in:
- **demand**: `customer`, `customer_group`  
- **supply**: `airplane`, `airplane_model`  
- **operations**: `trip`, `order`

We preserve natural keys and attach freshness SLOs (warn/error) on the most time‑sensitive feeds.

### Staging

Goals: **normalize names**, **cast types**, **light cleaning** (trim, case normalize), and introduce small semantics. Highlights:

- **Contracts**: `contract.enforced: true` on staging models.
- **Macros**: `nullif_blank`, `standardize_phone`, `parse_ts` keep SQL tidy.
- **Normalization**: emails lowercased, phone digits‑only, categorical fields uppercased, epoch millis → timestamps.
- **Operational models** (`stg_trip`, `stg_order`) are **incremental** and add an **`updated_at`** column (`current_timestamp()`) to drive downstream incrementality.

### Curated (Star Schema)

Two **dimensions** and one **fact**:

- **`dim_airplane`**: attributes by airplane (manufacturer, model, capacity, engine_type, ranges), joined with canonical `airplane_model` via `model_key`.
- **`dim_customer`**: customer + group enrichment (name, group name/type/registry).
- **`fct_orders`** *(incremental MERGE on `order_id`)*: joins orders with trip attributes.  
  - Carries: order status, price, seat, origin/destination, `trip_start_ts`, `trip_end_ts`, `order_trip_start_ts`, `order_trip_start_date`.  
  - Adds **`trip_duration_minutes`**.  
  - Maintains **`updated_at = greatest(order.updated_at, trip.updated_at)`** for late‑arriving updates.  
  - **Null‑safe updates** on incremental runs: overwrite with non‑null values only (`coalesce(new, old)` pattern).

### Mart (One Big Table)

- **`mart_obt_orders`**: a denormalized “one big table” for BI and ad‑hoc exploration.  
  - Joins `fct_orders` with `dim_customer` and `dim_airplane`.  
  - Uses a simple incremental MERGE keyed on `order_id`, driven by the fact’s `updated_at` with a configurable small lookback.

---

## Testing & quality gates

We combine native dbt tests with community packages to maximize signal:

- **Schema tests**: `not_null`, `unique`, `relationships`, `accepted_values` (enums uppercased).  
- **Business assertions** (via `dbt-expectations`): non‑negative prices, `trip_end_ts > trip_start_ts`, email regex, value ranges.  
- **Composite key** checks (via `dbt_utils.unique_combination_of_columns`).  
- **PII tagging** on emails/phones for awareness.  
- **Audits** (via `audit_helper.compare_relations`) ensure parity between key layers (e.g., `stg` vs `fact`, `fact` vs `mart`).  
- **Source drift** guards (singular test comparing source vs staging counts).  
- **Snapshots**  for slowly changing attributes (e.g., airplane model specs, customer details).

All tests run in **CI**; a lean subset (contracts + a few smoke checks) runs during **CD** post‑deploy.

---

## Incremental strategy & backfills

- **Staging (operations)**: incremental MERGE with a **rolling lookback** window on trip start time; supports **bounded backfills** via vars:
  - `ops_lookback_days` (default 3)  
  - `ops_backfill_start`, `ops_backfill_end` (ISO timestamps)
- **Curated fact**: incremental MERGE on `order_id`:
  - Rolling window on **`updated_at`** with `fact_lookback_hours` (default 24) or bounded backfill with `fact_backfill_start`/`fact_backfill_end`.
- **Mart**: simple incremental MERGE on `order_id` with `mart_lookback_hours` (default 24).

This pattern gracefully captures **late‑arriving** data while keeping runs fast.

---

## CI/CD

### CI (pull requests)

- **Slim CI** builds only **changed models + their neighbors** and runs the test suite; failing tests are persisted to the warehouse (`--store-failures`).  
- CI also builds docs to catch contract or docs issues early.

- A **Databricks Workflow** promotes code through **Staging → approval → Production**.  
- **Staging**: builds user‑facing surfaces (e.g., `tag:deploy`), runs smoke tests, and publishes **artifacts** (`manifest.json`, `run_results.json`) to a stable UC Volume path.  
- **Production**: builds with **deferred** state pointing at staging artifacts for speed/safety; runs a tiny smoke suite and refreshes docs/lineage.  
- **Rollback**: re‑run at a previous commit; use **Delta Time Travel** for data rollback where needed.

> See `cd_plan.md` for the concise delivery plan and the Databricks Workflow layout. Complementary helpers like `databricks-ci-janitor.yml` can clean transient assets on schedule.

---

## Project structure
```
air_boltic_datahub/
│
├── air_boltic_analytics/
│   ├── dbt_project.yml
│   ├── packages.yml
│   ├── profiles.yml
│   ├── requirements.ci.txt
│   ├── analyses/
│   ├── macros/
│   ├── models/
│   │   ├── exposures.yml
│   │   ├── overview.md
│   │   ├── curated/
│   │   ├── marts/
│   │   ├── sources/
│   │   └── staging/
│   ├── seeds/
│   ├── snapshots/
│   ├── target/
│   └── tests/
├── data/
│   ├── aeroplane_model.json
│   ├── Air Boltic data.xlsx
│   └── load_data_to_databricks.py
├── .env.example
├── .gitignore
└── Analytics Engineer Home Task_ Air Boltic.pdf
```
---

## How to run locally

### 1) Install dependencies
```bash
pip install -r requirements.txt
dbt deps
```

### 2) Configure credentials
Set a dbt **profile** for Databricks and required env vars (e.g., `RAW_CATALOG` for raw).

### 3) Load sample data
```bash
python data/load_raw_to_databricks.py
```

### 4) Build models
```bash
# All layers
dbt build

# Only staging
dbt build --select staging

# Curated fact (24h lookback on updated_at)
dbt build --select fct_orders

# Mart (joins fact + dims)
dbt build --select mart_obt_orders
```

### 5) Backfills (as needed)
```bash
# Staging operations (bounded window)
dbt build --select stg_operations__trip+   --vars '{ops_backfill_start: "2025-06-01 00:00:00", ops_backfill_end: "2025-06-10 00:00:00"}'

# Curated fact (bounded window)
dbt build --select fct_orders   --vars '{fact_backfill_start: "2025-06-01 00:00:00", fact_backfill_end: "2025-06-10 00:00:00"}'
```

---

## Design decisions

- **Star schema** in curated: keeps conformed dimensions reusable and facts lean; plays nicely with BI tools (Looker Explores/semantic layers).  
- **OBT in mart**: pragmatic denormalization for analyst speed and dashboard simplicity.  
- **Contracts everywhere**: defend the interface; fail fast on drift.  
- **`updated_at` in staging**: a clear, reliable driver for downstream incrementality.  
- **Null‑safe merges**: coalesce pattern avoids erasing attributes when upstream fields are missing (common with late-arriving trips or slowly filled attributes).  
- **Audit surfaces**: `audit_helper` compares keep us honest across layers.  
- **CI/CD simplicity**: slim CI + deferral‑based CD yields fast, boring releases.
- **SCD‑2 Snapshots** or dimensional slowly‑changing patterns where analysis benefits.
- **Seeded reference vocabularies** for statuses/engine types and enforce via relationships.

---

## Future work

- Add **data quality metrics** (DQ dashboards) sourced from test artifacts.   
- **Row‑level lineage** to help debug late arrivals (e.g., augment facts with source load ids).  
- Expand **marts** (e.g., regional performance, aircraft utilization, cohort OBTs).  
- Optional **column‑level constraints** (Delta `NOT NULL`, `CHECK`) on stable curated tables.

---

## Acknowledgements

- *Task brief:* included as `Analytics Engineer Home Task_ Air Boltic.pdf` (see repo root).  
- *CD concept:* see `cd_plan.md` for the concise delivery flow; Databricks Workflow defers prod builds to staging artifacts for safety.
- If external assistance/tools were used while preparing this repository, the details and rationale can be documented here for transparency (per the task’s guidance).

---

*Author: Seyed Amir Hosein Etemad Hoseini> – etemadhoseini@gmail.com*  
*Date: 08.09.2025*
