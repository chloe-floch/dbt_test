SELECT 
  product_id,
  COALESCE(product_category, 'other') AS product_category,
  CAST(product_name_lenght AS int) AS product_name_lenght,
  CAST(product_description_lenght AS int) AS product_description_lenght,
  CAST(product_photos_qty AS int) AS product_photos_qty,
  CAST(product_weight_g AS int) AS product_weight_g,
  CAST(product_length_cm AS int) AS product_length_cm,
  CAST(product_height_cm AS int) AS product_height_cm,
  CAST(product_width_cm AS int) AS product_width_cm,
FROM {{ source('dataset_fil_rouge', 'product') }}