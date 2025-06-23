SELECT 
  seller_id,
  seller_zip_code,
  seller_city,
  seller_state
FROM {{ source('dataset_fil_rouge', 'seller') }} 