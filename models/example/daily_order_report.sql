{{
  config(
    materialized = 'table',
    )
}}

SELECT 
  DATE(order_date) AS order_date,
  COUNT(DISTINCT order_id) AS total_orders,
  COUNT(DISTINCT user_name) AS total_customers,
  COUNT(DISTINCT
    CASE WHEN order_status = 'shipped'
    THEN order_id
    END) AS total_shipped_orders 
FROM {{ source('dataset_fil_rouge', 'order') }}
GROUP BY order_date