WITH 
  orders_summary AS (
    SELECT
      o.user_id,
      SUM((oi.unit_price * oi.item_quantity + oi.shipping_cost)) AS total_order_spent,
      SUM(oi.item_quantity) AS total_items,
      COUNT(DISTINCT(product_id)) AS total_distinct_items,
      COUNT(DISTINCT o.order_id) AS total_orders
    FROM {{ ref('stg_dataset_fil_rouge_order_item') }} oi
    INNER JOIN {{ ref('stg_dataset_fil_rouge_order') }} o
      USING(order_id)
    GROUP BY 
      o.user_id
  ),

  product_summary AS (
    SELECT
      o.user_id,
      oi.product_id,
      SUM(oi.item_quantity) AS quantity_ordered,
      ROW_NUMBER() OVER (
      PARTITION BY o.user_id
      ORDER BY SUM(item_quantity) DESC
      ) AS rank 
    FROM {{ ref('stg_dataset_fil_rouge_order') }} o
    LEFT JOIN {{ ref('stg_dataset_fil_rouge_order_item') }} oi
      USING (order_id)
    GROUP BY 
      o.user_id,
      oi.product_id
  )

  SELECT 
    os.user_id,
    os.total_order_spent,
    os.total_distinct_items,
    os.total_orders,
    ps.product_id AS favorite_product,
    ps.quantity_ordered
  FROM orders_summary os
  LEFT JOIN product_summary ps
    ON os.user_id = ps.user_id AND rank=1
  ORDER BY os.total_order_spent DESC 
  
