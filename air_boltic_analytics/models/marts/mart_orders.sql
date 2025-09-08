{#
  One Big Table mart combining fct_orders + dim_customer + dim_airplane.
  Simple incremental strategy:
    - MERGE on order_id
    - Process rows where fct_orders.updated_at is within a small lookback
      of the current table's MAX(updated_at).
    - Configure lookback via var('mart_lookback_hours', 24).

  Usage examples:
    dbt build --select marts.mart_obt_orders
    dbt build --select marts.mart_obt_orders --vars '{mart_lookback_hours: 72}'
#}
{{ 
  config(
      materialized='incremental',
      incremental_strategy='merge',
      unique_key='order_id',
      on_schema_change='sync_all_columns',
      cluster_by=['order_trip_start_date'],
      tags=['mart','obt'],
      post_hook=[
        "OPTIMIZE {{ this }} ZORDER BY (order_trip_start_date)",
        "ANALYZE TABLE {{ this }} COMPUTE STATISTICS"
      ]
  ) 
}}

with f as (
  select * from {{ ref('fct_orders') }}
  {% if is_incremental() %}
  where updated_at >= coalesce(
    (select max(updated_at) from {{ this }}) - interval {{ var('mart_lookback_hours', 24) }} hours,
    timestamp('1970-01-01 00:00:00')
  )
  {% endif %}
),
dc as (
  select * from {{ ref('dim_customer') }}
),
da as (
  select * from {{ ref('dim_airplane') }}
),
joined as (
  select
    f.order_id,
    f.customer_id,
    f.trip_id,
    f.airplane_id,
    dc.customer_name,
    dc.customer_group_id,
    dc.customer_group_name,
    dc.customer_group_type,
    dc.customer_group_registry_number,
    da.manufacturer,
    da.model,
    da.max_seats,
    da.engine_type,
    da.max_distance,
    da.max_weight,
    da.model_key,
    f.order_status,
    f.price_eur,
    f.seat_no,
    f.origin_city,
    f.destination_city,
    f.trip_start_ts,
    f.trip_end_ts,
    f.order_trip_start_ts,
    f.order_trip_start_date,
    f.trip_duration_minutes,
    f.updated_at
  from f
  left join dc on dc.customer_id = f.customer_id
  left join da on da.airplane_id = f.airplane_id
)

select * from joined
