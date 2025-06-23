SELECT count(distinct order_id) as total_distinct_orders
FROM {{ ref('test_model') }}