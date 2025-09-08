{{ 
    config(
        tags=['staging','supply'], 
        contract={'enforced': true}
        ) 
}}

with src as (
  select * from {{ source('supply','airplane_model') }}
)
select
  cast(upper(trim(manufacturer)) as string)                               as manufacturer,
  cast(upper(trim(model)) as string)                                      as model,
  md5(concat_ws('||', upper(trim(manufacturer)), upper(trim(model))))     as model_key,
  cast({{ nullif_blank('max_seats') }} as integer)                        as max_seats,
  cast(upper(trim(engine_type)) as string)                                as engine_type,
  cast({{ nullif_blank('max_distance') }} as integer)                     as max_distance,
  cast({{ nullif_blank('max_weight') }} as integer)                       as max_weight
from src
