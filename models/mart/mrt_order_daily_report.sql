SELECT
  DATE(o.order_created_at) AS order_date,
  o.user_state,
  am.account_manager,
  o.order_status,
  COALESCE(COUNT(DISTINCT o.order_id), 0) AS total_orders,
  COALESCE(AVG(o.total_items), 0) AS avg_items_per_order,
  COALESCE(AVG(o.avg_feedback_score), 0) AS avg_feedback_score,
  COALESCE(AVG(o.total_order_amount), 0) AS avg_amount_spent_per_order,
  
FROM {{ ref('int_dataset_fil_rouge__order') }} o
LEFT JOIN {{ ref('stg_google_sheets_account_manager_region_mapping') }} am
  ON am.state = o.user_state
GROUP BY 
  order_date,
  o.user_state,
  am.account_manager,
  o.order_status
ORDER BY 
  am.account_manager,
  order_date