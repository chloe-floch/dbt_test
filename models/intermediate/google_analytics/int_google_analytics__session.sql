WITH session_data AS (
SELECT 
  user_pseudo_id,
  ga_session_id,
  CONCAT(ga_session_id,' - ', user_pseudo_id) AS unique_session_id,
  MIN(TIMESTAMP_MICROS(event_timestamp)) AS session_start_time, 
  MAX(TIMESTAMP_MICROS(event_timestamp)) AS session_end_time,  
  MAX(browser) AS browser_used,
  MAX(medium) AS traffic_medium, 
  MAX(source) AS traffic_source,
  MAX(name) AS traffic_name, 
  COUNT(*) AS event_count,
  SUM(
    CASE 
      WHEN event_name = 'page_view' THEN 1
      ELSE 0
      END
  ) AS pages_viewed
FROM {{ ref('stg_google_analytics_event_flattened') }}
GROUP BY 
  ga_session_id, 
  user_pseudo_id
)

SELECT
  user_pseudo_id,
  ga_session_id,
  unique_session_id,
  session_start_time,
  session_end_time,
  TIMESTAMP_DIFF(session_end_time, session_start_time, second) AS session_duration_seconds, 
  browser_used,
  traffic_medium,
  traffic_source,
  traffic_name,
  event_count,
  pages_viewed
FROM session_data