{% snapshot snap_airplane_model %}
    {{ config(
        target_schema='snapshots',
        unique_key='model_key',
        strategy='check',
        check_cols=['max_seats','engine_type','max_distance','max_weight']
    ) }}
    select * from {{ ref('stg_airplane_model') }}
{% endsnapshot %}
