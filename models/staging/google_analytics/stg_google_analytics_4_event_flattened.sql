SELECT
  event_name,
  event_date,
  event_timestamp,
  user_pseudo_id,
  user_first_touch_timestamp,

  (SELECT ep.value.int_value
   FROM UNNEST(event_params) AS ep
   WHERE ep.key = 'ga_session_id') AS ga_session_id,

  (SELECT ep.value.string_value
   FROM UNNEST(event_params) AS ep
   WHERE ep.key = 'page_title') AS page_title,

  (SELECT ep.value.string_value
   FROM UNNEST(event_params) AS ep
   WHERE ep.key = 'browser') AS browser,

  traffic_source.medium,
  traffic_source.source,
  traffic_source.name

FROM {{ source('google_analytics_4', 'events_20210131')}}