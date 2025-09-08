{{ 
    config(
        materialized='table',
        on_schema_change='fail',
        tags=['curated','dim']
    ) 
}}

with stg_customer as (
  select * from {{ ref('stg_customer') }}
),
stg_customer_group as (
  select * from {{ ref('stg_customer_group') }}
)

select
  c.customer_id,
  c.customer_group_id,
  g.customer_group_name,
  g.customer_group_type,
  g.registry_number as customer_group_registry_number,
  c.customer_name
from stg_customer c
left join stg_customer_group g using (customer_group_id)
