{% snapshot snap_customer %}
    {{ config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='check',
        check_cols=['customer_name','customer_group_id','email','phone_number']
    ) }}
    select * from {{ ref('stg_customer') }}
{% endsnapshot %}
