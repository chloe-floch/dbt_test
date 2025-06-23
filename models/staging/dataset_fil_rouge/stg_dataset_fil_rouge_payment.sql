SELECT 
  CONCAT(order_id, '-' , payment_sequential) AS payment_id, 
  order_id,
  payment_sequential,
  payment_type,
  payment_installments,
  payment_value
FROM {{ source('dataset_fil_rouge', 'payment') }}