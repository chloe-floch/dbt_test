SELECT  
  state,
  account_manager
FROM {{ source('google_sheets', 'account_manager_region_mapping') }}