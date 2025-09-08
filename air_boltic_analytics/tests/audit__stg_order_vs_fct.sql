{{ audit_helper.compare_relations(
     a_relation=ref('stg_order'),
     b_relation=ref('fct_orders'),
     primary_key='order_id',
     summarize=false,
     exclude_columns=['updated_at']
) }}
