{{ 
    config(
        tags=['staging','supply'], 
        contract={'enforced': true}
        ) 
}}

with src as (
  select * from {{ source('supply','airplane') }}
)
select
  cast(airplane_id as integer)                                                    as airplane_id,
  cast(upper(trim(manufacturer)) as string)                                       as manufacturer,
  cast(upper(trim(airplane_model)) as string)                                     as model,

  -- Deterministic key to match to stg_airplane_model
  md5(concat_ws('||', upper(trim(manufacturer)), upper(trim(airplane_model))))    as model_key
from src
