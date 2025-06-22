SELECT
  SUM(total_orders) AS total_orders
FROM {{ ref('daily_order_report') }}