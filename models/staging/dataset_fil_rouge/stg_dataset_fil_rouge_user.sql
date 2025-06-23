SELECT  
  user_name AS user_id,
  customer_zip_code as user_zip_code,
  customer_city as user_city,
  customer_state as user_state
FROM {{ source('dataset_fil_rouge', 'user') }} 