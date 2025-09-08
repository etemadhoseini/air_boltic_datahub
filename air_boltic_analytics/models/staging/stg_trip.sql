{# 
  Incremental strategy with rolling lookback + optional bounded backfill.
  - Default: reprocess last N days based on trip_start_ts (N = var('ops_lookback_days', 3)).
  - Bounded backfill: pass ops_backfill_start / ops_backfill_end as ISO timestamps.
    e.g. --vars '{ops_backfill_start: "2025-06-01 00:00:00", ops_backfill_end: "2025-06-10 00:00:00"}'
#}
{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='trip_id',
    on_schema_change='fail',
    tags=['staging','operations'],
    cluster_by=['trip_start_date'],
    contract={'enforced': true}
  )
}}

with src as (
  select * from {{ source('operations','trip') }}
), 
typed as (
  select
    cast(trim(trip_id) as integer)                                        as trip_id,
    initcap(trim(origin_city))                                            as origin_city,
    initcap(trim(destination_city))                                       as destination_city,
    cast(trim(airplane_id) as integer)                                    as airplane_id,
    case
      when try_cast(start_timestamp as bigint) is not null
        then from_unixtime(cast(start_timestamp as bigint) / 1000)
      else to_timestamp(start_timestamp)
    end as trip_start_ts,
    case
      when try_cast(end_timestamp as bigint) is not null
        then from_unixtime(cast(end_timestamp as bigint) / 1000)
      else to_timestamp(end_timestamp)
    end as trip_end_ts
  from src
),
final as (
  select
    trip_id,
    origin_city,
    destination_city,
    airplane_id,
    trip_start_ts,
    trip_end_ts,
    to_date(trip_start_ts)                                                as trip_start_date,
    current_timestamp()                                                   as updated_at
  from typed
),
max_loaded as (
  {% if is_incremental() %}
    select max(trip_start_ts) as max_ts from {{ this }}
  {% else %}
    select cast(null as timestamp) as max_ts
  {% endif %}
)

select *
from final
{% if is_incremental() %}
  {% if var('ops_backfill_start', none) is not none and var('ops_backfill_end', none) is not none %}
    where trip_start_ts >= to_timestamp('{{ var('ops_backfill_start') }}')
      and trip_start_ts <  to_timestamp('{{ var('ops_backfill_end') }}')
  {% else %}
    where trip_start_ts >= coalesce((select max_ts from max_loaded) - interval {{ var('ops_lookback_days', 3) }} days,
                                    to_timestamp('1970-01-01 00:00:00'))
  {% endif %}
{% endif %}
