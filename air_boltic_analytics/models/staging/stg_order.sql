{# 
  Incremental with rolling lookback driven by the TRIP start time (joined from stg_operations__trip).
  - Ensures late-arriving orders for known trips are captured.
  - Bounded backfill supported via ops_backfill_start / ops_backfill_end (same vars as trips).
  - Includes rows with NULL order_trip_start_ts to avoid dropping orders before their trip arrives.
#}
{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id',
    on_schema_change='fail',
    tags=['staging','operations'],
    cluster_by=['order_trip_start_date'],
    contract={'enforced': true}
  )
}}

with src as (
  select * from {{ source('operations','order') }}
),
trip as (
  select 
    trip_id, 
    trip_start_ts 
  from {{ ref('stg_trip') }}
),
orders_typed as (
  select
    cast(trim(order_id) as integer)                                                         as order_id,
    cast(trim(customer_id) as integer)                                                      as customer_id,
    cast(trim(trip_id) as integer)                                                          as trip_id,
    initcap(trim(status))                                                                   as order_status,
    cast({{ nullif_blank('price_eur') }} as decimal(18,2))                                  as price_eur,
    cast(upper(trim(seat_no)) as string)                                                    as seat_no
  from src
),
joined as (
  select
    o.order_id,
    o.customer_id,
    o.trip_id,
    o.order_status,
    o.price_eur,
    o.seat_no,
    t.trip_start_ts                                                     as order_trip_start_ts,
    to_date(t.trip_start_ts)                                            as order_trip_start_date,
    current_timestamp()                                                 as updated_at
  from orders_typed o
  left join trip t on o.trip_id = t.trip_id
),
max_loaded as (
  {% if is_incremental() %}
    select max(order_trip_start_ts) as max_ts from {{ this }}
  {% else %}
    select cast(null as timestamp) as max_ts
  {% endif %}
)

select *
from joined
{% if is_incremental() %}
  {% if var('ops_backfill_start', none) is not none and var('ops_backfill_end', none) is not none %}
    where (order_trip_start_ts >= to_timestamp('{{ var('ops_backfill_start') }}')
       and order_trip_start_ts <  to_timestamp('{{ var('ops_backfill_end') }}'))
       or order_trip_start_ts is null
  {% else %}
    where (order_trip_start_ts >= coalesce((select max_ts from max_loaded) - interval {{ var('ops_lookback_days', 3) }} days,
                                           to_timestamp('1970-01-01 00:00:00')))
       or order_trip_start_ts is null
  {% endif %}
{% endif %}
