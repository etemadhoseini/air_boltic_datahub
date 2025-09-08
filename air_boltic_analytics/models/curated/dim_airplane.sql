{{ 
    config(
        materialized='table',
        on_schema_change='fail',
        tags=['curated','dim']
    ) 
}}

with stg_airplane as (
  select * from {{ ref('stg_airplane') }}
),
stg_airplane_model as (
  select * from {{ ref('stg_airplane_model') }}
)
select
  a.airplane_id,
  a.manufacturer,
  a.model,
  m.max_seats,
  m.engine_type,
  m.max_distance,
  m.max_weight,
  a.model_key
from stg_airplane a
left join stg_airplane_model m using (model_key)
