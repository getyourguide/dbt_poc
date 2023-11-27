{{ config(schema=var('dwh')) }}




-- Logic explained: https://docs.google.com/document/d/1s1kFjRH9_cnwhcNWrcdMLldk7jYTf1NLITccrK0f_Ec/edit#
-- Investigation notebook: https://dbc-d10db17d-b6c4.cloud.databricks.com/?o=4592942032988138#notebook/2604571396281703

WITH booking_events
AS (
  SELECT events.DATE
    , events.attribution_session_id AS session_id
    , events.event_properties.TIMESTAMP
    , events.event_properties.uuid
    , user.visitor_id
    , activity.tour_id
    , activity.booking_hash_code
  FROM {{ source('default', 'events') }} AS events
    lateral VIEW explode(booking.cart.cart_items.activities) AS activity
  WHERE events.DATE BETWEEN "{{ var ('based-on-events-start-date') }}" AND "{{ var ('end-date') }}"
    AND events.event_name = "BookAction"
  )
  , bookings
AS (
  SELECT *
  FROM {{ source('dwh', 'fact_booking') }}
  WHERE DATE(date_of_creation) BETWEEN "{{ var ('based-on-events-start-date') }}" - interval 3 days AND "{{ var ('end-date') }}"
  )
  , sessions
AS (
  SELECT *
  FROM {{ ref('fact_session') }}
  WHERE DATE BETWEEN "{{ var ('based-on-events-start-date') }}" AND "{{ var ('end-date') }}"
  )
  , session_adps
AS (
  SELECT *
  FROM {{ ref('fact_session_adp') }}
  WHERE DATE BETWEEN "{{ var ('based-on-events-start-date') }}" AND "{{ var ('end-date') }}"
  )
SELECT DISTINCT bookings.booking_id
  , booking_events.DATE
  , booking_events.session_id
  , booking_events.visitor_id
  , sessions.touchpoint_id
  , last(session_adps.session_adp_id) OVER (
    PARTITION BY bookings.booking_id ORDER BY booking_events.TIMESTAMP ASC
    ) AS session_adp_id
  , last(session_adps.session_search_id) OVER (
    PARTITION BY bookings.booking_id ORDER BY booking_events.TIMESTAMP ASC
    ) AS session_search_id
FROM booking_events
JOIN bookings
  ON booking_events.booking_hash_code = bookings.hash_code
JOIN sessions
  ON booking_events.DATE = sessions.DATE
    AND booking_events.session_id = sessions.session_id
LEFT JOIN session_adps
  ON sessions.session_id = session_adps.session_id
    AND booking_events.tour_id = session_adps.tour_id
    AND booking_events.TIMESTAMP >= session_adps.visit_started_timestamp