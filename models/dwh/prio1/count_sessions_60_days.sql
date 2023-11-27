{{ config(schema=var('marketplace_reports')) }}




SELECT
  attribution_session_id,
  visitor_id,
  date,
  first_event_timestamp as sd_entry_time,
  COUNT(*) OVER ( PARTITION BY visitor_id ORDER BY unix_timestamp(first_event_timestamp) RANGE BETWEEN 60 * 24 * 3600 PRECEDING AND CURRENT ROW) - 1 as n_sessions
  -- converted 60 days into seconds
FROM {{ ref('fact_session') }}
WHERE 1=1
  {% if is_incremental() %}
    AND date BETWEEN '{{ var ('start-date') }}' - INTERVAL '60' DAYS AND '{{ var ('end-date') }}'
  {% endif %}
  AND date >= '2020-01-01'
