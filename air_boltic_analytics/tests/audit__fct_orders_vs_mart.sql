-- Show mismatches between fct_orders and mart_obt_orders on the shared grain (order_id)
{{ audit_helper.compare_relations(
     a_relation=ref('fct_orders'),
     b_relation=ref('mart_orders'),
     primary_key='order_id',
     summarize=false,
     exclude_columns=['updated_at']
) }}