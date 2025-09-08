-- Fail if staging lost rows vs. source
with src as (select count(*) as c from {{ source('operations','order') }}),
stg as (select count(*) as c from {{ ref('stg_order') }})
select stg.c as stg_count, src.c as src_count
from stg
cross join src
where stg.c < src.c

