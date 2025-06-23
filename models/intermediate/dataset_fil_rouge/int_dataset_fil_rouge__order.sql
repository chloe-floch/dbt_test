WITH 
  order_item_grouped_by_order AS (
    SELECT
      order_id, 
      SUM(item_quantity) AS total_items,
      COUNT(DISTINCT(product_id)) AS total_distinct_items,
      SUM(unit_price * item_quantity + shipping_cost) AS total_order_amount
    FROM {{ ref('stg_dataset_fil_rouge_order_item') }}
    GROUP BY order_id
  ),
  feedback_grouped_by_order AS (
    SELECT
      order_id,
      ROUND(AVG(feedback_score),1) AS avg_feedback_score
    FROM {{ ref('stg_dataset_fil_rouge_feedback') }}
    GROUP BY order_id
  )

SELECT 
  o.order_id,
  o.order_status,
  o.order_created_at,
  o.order_approved_at,
  oi.total_items,
  oi.total_distinct_items,
  oi.total_order_amount,
  f.avg_feedback_score
FROM {{ ref('stg_dataset_fil_rouge_order') }} AS o  
LEFT JOIN order_item_grouped_by_order AS oi 
  USING(order_id)
LEFT JOIN feedback_grouped_by_order AS f
  USING(order_id)