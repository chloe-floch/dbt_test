SELECT 
  CONCAT(feedback_id, '-' , order_id) as feedback_id,
  order_id,
  feedback_score,
  DATETIME(feedback_form_sent_date, "Europe/Paris") AS feedback_form_sent_at,
  DATETIME(feedback_answer_date, "Europe/Paris") AS feedback_answered_at
FROM {{ source('dataset_fil_rouge', 'feedback') }} 