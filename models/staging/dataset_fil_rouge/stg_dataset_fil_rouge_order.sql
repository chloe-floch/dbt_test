SELECT
  order_id,
  user_name as user_id,
  order_status,
  DATETIME(order_date, "Europe/Paris") AS order_created_at,
  DATETIME(order_approved_date, "Europe/Paris") AS order_approved_at,
  DATETIME(pickup_date, "Europe/Paris") AS picked_up_at,
  DATETIME(delivered_date, "Europe/Paris") AS delivered_at,
  DATETIME(estimated_time_delivery, "Europe/Paris") AS esttimated_time_delivery,
FROM {{ source('dataset_fil_rouge', 'order') }}