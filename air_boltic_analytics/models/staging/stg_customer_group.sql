{{
    config(
        tags=['staging','demand'],
        contract={'enforced': true}
    )
}}

with src as (
    select * from {{ source('demand', 'customer_group') }}
)

select
    cast(id as integer)                                   as customer_group_id,
    cast({{ nullif_blank('name') }} as string)            as customer_group_name,
    cast({{ nullif_blank('type') }} as string)            as customer_group_type,
    cast({{ nullif_blank('registry_number') }} as string) as registry_number
from src
