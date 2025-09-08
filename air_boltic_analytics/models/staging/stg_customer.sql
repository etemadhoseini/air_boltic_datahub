{{
    config(
        tags=['staging','demand'],
        contract={'enforced': true}
    )
}}


with src as (
    select * from {{ source('demand','customer') }}
)

select
    cast(customer_id as integer) as customer_id,
    cast(regexp_replace(customer_group_id, '\\.0$', '') as integer) as customer_group_id,
    cast({{ nullif_blank('name') }} as string) as customer_name,
    cast(lower(trim(email)) as string) as email,
    cast({{ standardize_phone('phone_number') }} as string) as phone_number
from src