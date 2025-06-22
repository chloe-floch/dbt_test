SELECT count(distinct order_id) as total_distinct_orders2
FROM {{ ref('test_model') }}