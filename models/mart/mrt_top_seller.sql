SELECT
  am.account_manager,
  SUM(o.total_order_amount) as total_amount,
  AVG(o.avg_feedback_score) as avg_feedback_score

FROM {{ ref('int_dataset_fil_rouge__order') }} o
LEFT JOIN {{ ref('stg_google_sheets_account_manager_region_mapping') }} am
  ON am.state = o.user_state
GROUP BY
  account_manager
ORDER BY
  total_amount DESC,
  avg_feedback_score