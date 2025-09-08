{% docs __overview__ %}

# Air Boltic dbt Project — Overview

This repository models Air Boltic’s data in **three layers** — *sources → staging → curated → marts* — with strict schemas, type safety, and pragmatic incrementality on Databricks.

---

## What’s in here

### Sources (raw landing)
- **demand**: `customer`, `customer_group`
- **supply**: `airplane`, `airplane_model`
- **operations**: `trip`, `order`

> Freshness SLOs live on sources. Raw tables typically land as strings and are typed in staging.

---

## Staging layer (`models/staging/*`)

Purpose: **standardize and sanitize** raw data.
- **Contracts**: `contract.enforced: true` on models.
- **Conventions**: snake_case column names, trimmed strings, lowercased emails, normalized phone numbers, uppercase enums.
- **Typing**: cast numerics/decimals/timestamps (e.g., epoch millis → `timestamp`).
- **Light semantics**: construct keys like `model_key`, normalize manufacturer/model.

**Incremental staging (operations only)**  
- `stg_trip` & `stg_order` use `materialized='incremental'` + `merge`.
- Rolling lookback controlled by `ops_lookback_days` (default 3) or **bounded backfills** via `ops_backfill_start` / `ops_backfill_end`.
- Both add **`updated_at`** (current run timestamp) to support downstream incrementals.

**Views in staging (demand & supply)**  
- Demand and supply staging models are views with contracts and column tests.

**Utility macros (in `macros/cleaning.sql`)**
- `nullif_blank()`, `standardize_phone()`, `to_bool()`, `parse_ts()`, `surrogate_key()`.

---

## Curated layer (`models/curated/*`)

### Dimensions
- **`dim_airplane`**: Type‑1 dimension joining `stg_airplane` with `stg_airplane_model` by `model_key` (attributes: manufacturer, model, max_seats, engine_type, etc.).
- **`dim_customer`**: Type‑1 dimension joining `stg_customer` with `stg_customer_group` (group attributes, registry number, etc.).

### Fact
- **`fct_orders`** *(incremental MERGE on `order_id`)*:  
  Joins `stg_order` + `stg_trip`; includes trip fields (origin/destination, start/end) and `trip_duration_minutes`.  
  **Incremental driver**: `updated_at = greatest(order.updated_at, trip.updated_at)`.
  - Rolling lookback: `fact_lookback_hours` (default 24).
  - Optional bounded backfill: `fact_backfill_start` / `fact_backfill_end`.
  - **Null‑safe updates**: during incremental runs, updates coalesce `COALESCE(new, old)` so non‑null new values overwrite, but NULLs don’t erase existing values.

---

## Mart layer (`models/marts/*`)

- **`mart_obt_orders`**: “One Big Table” combining `fct_orders` + `dim_customer` + `dim_airplane`.  
  Simple incremental MERGE on `order_id` driven by the fact’s `updated_at` with `mart_lookback_hours` (default 24).

---

## Naming & conventions

- **Models**: `stg_*`, `dim_*`, `fct_*`, `mart_*`.
- **Columns**: snake_case; natural keys retained (`*_id`).  
- **Enums**: uppercase; tested via `accepted_values` (warnings for out‑of‑set values).
- **Contracts**: enforced across staging/curated; marts sync schema with `on_schema_change='sync_all_columns'` by default.

---

## Tests

- **Uniqueness/Not‑null** on business keys (e.g., `customer_id`, `airplane_id`, `order_id`, `trip_id`).  
- **Relationships**: e.g., `stg_order.trip_id → stg_trip.trip_id`, `fct_orders.customer_id → dim_customer.customer_id`.  
- **Accepted values**: order status, engine type, etc.
- **PII tagging**: emails/phones carry `tags: [pii]` in staging/demand where applicable.

---

## Incremental & backfill playbook

**Operations staging**
```bash
# Default 3‑day lookback
dbt build --select staging:operations

# Wider window
dbt build --select staging:operations --vars '{ops_lookback_days: 7}'

# Bounded backfill
dbt build --select stg_operations__trip+ --vars '{ops_backfill_start: "2025-06-01 00:00:00", ops_backfill_end: "2025-06-10 00:00:00"}'
```

**Curated fact**
```bash
# Regular run (24h lookback on updated_at)
dbt build --select fct_orders

# Wider window
dbt build --select fct_orders --vars '{fact_lookback_hours: 72}'

# Bounded backfill
dbt build --select fct_orders --vars '{fact_backfill_start: "2025-06-01 00:00:00", fact_backfill_end: "2025-06-10 00:00:00"}'
```

**Mart OBT**
```bash
dbt build --select mart_obt_orders
dbt build --select mart_obt_orders --vars '{mart_lookback_hours: 72}'
```

---

{% enddocs %}