{# 
  Incremental fact combining orders + trip attributes.
  - Uses updated_at from staging to capture late-arriving/updated rows.
  - Rolling lookback (hours): var('fact_lookback_hours', 24)
  - Optional bounded backfill: var('fact_backfill_start'), var('fact_backfill_end')
  - Update-only-if-new-not-null: COALESCE(new, old) in incremental branch.
#}
{{ 
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='order_id',
        on_schema_change='fail',
        cluster_by=['order_trip_start_date'],
        tags=['curated','fact'],
        post_hook=[
          "OPTIMIZE {{ this }} ZORDER BY (order_trip_start_date)",
          "ANALYZE TABLE {{ this }} COMPUTE STATISTICS"
        ]
    ) 
}}

with orders as (
  select * from {{ ref('stg_order') }}
),
trips as (
  select * from {{ ref('stg_trip') }}
),
joined as (
  select
    o.order_id,
    o.customer_id,
    o.trip_id,
    t.airplane_id,
    o.order_status,
    o.price_eur,
    o.seat_no,
    t.origin_city,
    t.destination_city,
    t.trip_start_ts,
    t.trip_end_ts,
    o.order_trip_start_ts,
    o.order_trip_start_date,
    case 
      when t.trip_start_ts is not null and t.trip_end_ts is not null 
      then (unix_timestamp(t.trip_end_ts) - unix_timestamp(t.trip_start_ts)) / 60.0
    end as trip_duration_minutes,
    greatest(
      coalesce(o.updated_at, timestamp('1970-01-01 00:00:00')),
      coalesce(t.updated_at,  timestamp('1970-01-01 00:00:00'))
    ) as updated_at
  from orders o
  left join trips t using (trip_id)
)

{% if is_incremental() %}

, existing as (
  select * from {{ this }}
)

, resolved as (
  -- In incremental runs, only overwrite with non-null incoming values
  select
    j.order_id,
    coalesce(j.customer_id,          e.customer_id)          as customer_id,
    coalesce(j.trip_id,              e.trip_id)              as trip_id,
    coalesce(j.airplane_id,          e.airplane_id)          as airplane_id,

    coalesce(j.order_status,         e.order_status)         as order_status,
    coalesce(j.price_eur,            e.price_eur)            as price_eur,
    coalesce(j.seat_no,              e.seat_no)              as seat_no,

    coalesce(j.origin_city,          e.origin_city)          as origin_city,
    coalesce(j.destination_city,     e.destination_city)     as destination_city,
    coalesce(j.trip_start_ts,        e.trip_start_ts)        as trip_start_ts,
    coalesce(j.trip_end_ts,          e.trip_end_ts)          as trip_end_ts,
    coalesce(j.order_trip_start_ts,  e.order_trip_start_ts)  as order_trip_start_ts,
    coalesce(j.order_trip_start_date,e.order_trip_start_date)as order_trip_start_date,

    coalesce(j.trip_duration_minutes, e.trip_duration_minutes) as trip_duration_minutes,

    j.updated_at
  from joined j
  left join existing e
    on e.order_id = j.order_id
)

{% else %}

, resolved as (
  select
    j.order_id,
    j.customer_id,
    j.trip_id,
    j.airplane_id,
    j.order_status,
    j.price_eur,
    j.seat_no,
    j.origin_city,
    j.destination_city,
    j.trip_start_ts,
    j.trip_end_ts,
    j.order_trip_start_ts,
    j.order_trip_start_date,
    j.trip_duration_minutes,
    j.updated_at
  from joined j
)

{% endif %}

, max_loaded as (
  {% if is_incremental() %}
    select max(updated_at) as max_updated_at from {{ this }}
  {% else %}
    select cast(null as timestamp) as max_updated_at
  {% endif %}
)

select *
from resolved
{% if is_incremental() %}
  {% if var('fact_backfill_start', none) is not none and var('fact_backfill_end', none) is not none %}
    where updated_at >= to_timestamp('{{ var('fact_backfill_start') }}')
      and updated_at <  to_timestamp('{{ var('fact_backfill_end') }}')
  {% else %}
    where updated_at >= coalesce(
      (select max_updated_at - interval {{ var('fact_lookback_hours', 24) }} hours from max_loaded),
      timestamp('1970-01-01 00:00:00')
    )
  {% endif %}
{% endif %}
