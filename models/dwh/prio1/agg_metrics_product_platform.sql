{{ config(schema=var('reports')) }}




-- the first and last date to be regenerated
WITH report AS (
  SELECT
    {% if is_incremental() %}
      "{{ var ('based-on-events-start-date') }}" AS start_date
    {% endif %}
    {% if not is_incremental() %}
      "2020-01-01" AS start_date
    {% endif %}
    , "{{ var ('end-date') }}" AS end_date
)

-- the list of dates to be regenerated (one row per date)
, report_dates AS (
  SELECT
    dim_date.date_id AS date
  FROM
    {{ source('dwh', 'dim_date') }} AS dim_date
    CROSS JOIN report ON dim_date.date_id BETWEEN report.start_date AND report.end_date
)

-- the first and last date for querying the data
-- (this is a wider timeframe because of the WoW comparisons)
, config AS (
  SELECT
    DATE_SUB(report.start_date, 6) AS start_date
    , report.end_date AS end_date
  FROM
    report
)

, session_metrics AS (
  SELECT
    report_dates.date
    , platform.platform_name AS platform
    , COUNT(DISTINCT CASE WHEN session.date = report_dates.date THEN session.visitor_id END) AS visitors
    , COUNT(DISTINCT CASE WHEN session.date BETWEEN DATE_SUB(report_dates.date, 6) AND report_dates.date THEN session.visitor_id END) AS visitors_last_7_days
    , COUNT(DISTINCT CASE WHEN session.date = report_dates.date AND session.is_bounce THEN session.visitor_id END) AS bouncers
    , COUNT(DISTINCT CASE WHEN session.date BETWEEN DATE_SUB(report_dates.date, 6) AND report_dates.date AND session.is_bounce THEN session.visitor_id END) AS bouncers_last_7_days
    , COUNT(DISTINCT CASE WHEN session.date = report_dates.date AND session_adp.session_id IS NOT NULL THEN session.visitor_id END) AS quoters
    , COUNT(DISTINCT CASE WHEN session.date BETWEEN DATE_SUB(report_dates.date, 6) AND report_dates.date AND session_adp.session_id IS NOT NULL THEN session.visitor_id END) AS quoters_last_7_days
    , COUNT(DISTINCT CASE WHEN session.date = report_dates.date AND session_adp.number_check_availability > 0 THEN session.visitor_id END) AS visitors_checked_availability
    , COUNT(DISTINCT CASE WHEN session.date BETWEEN DATE_SUB(report_dates.date, 6) AND report_dates.date AND session_adp.number_check_availability > 0 THEN session.visitor_id END) AS visitors_checked_availability_last_7_days
    , COUNT(DISTINCT CASE WHEN session.date = report_dates.date AND session_adp.has_add_to_cart THEN session.visitor_id END) AS visitors_added_to_cart
    , COUNT(DISTINCT CASE WHEN session.date BETWEEN DATE_SUB(report_dates.date, 6) AND report_dates.date AND session_adp.has_add_to_cart THEN session.visitor_id END) AS visitors_added_to_cart_last_7_days
    , COUNT(DISTINCT CASE WHEN session.date = report_dates.date AND session_adp.has_checkout THEN session.visitor_id END) AS visitors_checkouts
    , COUNT(DISTINCT CASE WHEN session.date BETWEEN DATE_SUB(report_dates.date, 6) AND report_dates.date AND session_adp.has_checkout THEN session.visitor_id END) AS visitors_checkouts_last_7_days
    , COUNT(DISTINCT CASE WHEN session.date = report_dates.date AND session.number_wishlist_interactions > 0 THEN session.visitor_id END) AS visitors_added_to_wishlist
    , COUNT(DISTINCT CASE WHEN session.date BETWEEN DATE_SUB(report_dates.date, 6) AND report_dates.date AND session.number_wishlist_interactions > 0 THEN session.visitor_id END) AS visitors_added_to_wishlist_last_7_days
    , COUNT(DISTINCT CASE WHEN session.date = report_dates.date THEN session.session_id END) as sessions
    , COUNT(DISTINCT CASE WHEN session.date BETWEEN DATE_SUB(report_dates.date, 6) AND report_dates.date THEN session.session_id END) as sessions_last_7_days
    , COUNT(DISTINCT CASE WHEN session.date = report_dates.date AND session.is_bounce THEN session.session_id END) as sessions_bounced
    , COUNT(DISTINCT CASE WHEN session.date BETWEEN DATE_SUB(report_dates.date, 6) AND report_dates.date AND session.is_bounce THEN session.session_id END) as sessions_bounced_last_7_days
  FROM
    {{ ref('fact_session') }} AS session
    LEFT JOIN {{ ref('fact_session_adp') }} AS session_adp ON session.date = session_adp.date AND session.session_id = session_adp.session_id
    LEFT JOIN {{ source('dwh', 'dim_platform') }} AS platform ON session.platform_id = platform.platform_id
    CROSS JOIN config
    CROSS JOIN report_dates
  WHERE
    session.date BETWEEN config.start_date AND config.end_date
    AND session.is_discovery
  GROUP BY 1, 2
)

, booking_metrics AS (
  SELECT
    report_dates.date
    , CASE
        WHEN device.device_display IN ('Desktop', 'Tablet') THEN 'desktop'
        WHEN device.device_display IN ('Android', 'iOS') THEN 'app'
        ELSE 'mobile'
      END AS platform
    , COUNT(DISTINCT CASE WHEN DATE(booking.date_of_checkout) = report_dates.date THEN booking.customer_id END) AS customers
    , COUNT(DISTINCT CASE WHEN DATE(booking.date_of_checkout) BETWEEN DATE_SUB(report_dates.date, 6) AND report_dates.date THEN booking.customer_id END) AS customers_last_7_days
    , COUNT(DISTINCT CASE WHEN DATE(booking.date_of_checkout) = report_dates.date THEN booking.booking_id END) AS bookings
    , COUNT(DISTINCT CASE WHEN DATE(booking.date_of_checkout) BETWEEN DATE_SUB(report_dates.date, 6) AND report_dates.date THEN booking.booking_id END) AS bookings_last_7_days
    , COUNT(DISTINCT CASE WHEN DATE(booking.date_of_checkout) = report_dates.date AND booking_trip.booking_in_trip_number > 1 THEN booking.booking_id END) AS bookings_trip_repeat
    , COUNT(DISTINCT CASE WHEN DATE(booking.date_of_checkout) BETWEEN DATE_SUB(report_dates.date, 6) AND report_dates.date AND booking_trip.booking_in_trip_number > 1 THEN booking.booking_id END) AS bookings_trip_repeat_last_7_days
    , SUM(CASE WHEN DATE(booking.date_of_checkout) = report_dates.date THEN booking.gmv END) AS gmv
    , SUM(CASE WHEN DATE(booking.date_of_checkout) BETWEEN DATE_SUB(report_dates.date, 6) AND report_dates.date THEN booking.gmv END) AS gmv_last_7_days
    , SUM(CASE WHEN DATE(booking.date_of_checkout) = report_dates.date THEN booking.nr END) AS nr
    , SUM(CASE WHEN DATE(booking.date_of_checkout) BETWEEN DATE_SUB(report_dates.date, 6) AND report_dates.date THEN booking.nr END) AS nr_last_7_days
    , COUNT(DISTINCT CASE WHEN DATE(booking.date_of_checkout) = report_dates.date THEN booking.shopping_cart_id END) AS transactions
    , COUNT(DISTINCT CASE WHEN DATE(booking.date_of_checkout) BETWEEN DATE_SUB(report_dates.date, 6) AND report_dates.date THEN booking.shopping_cart_id END) AS transactions_last_7_days
  FROM
    {{ source('dwh', 'fact_booking') }} AS booking
    JOIN {{ source('dwh', 'dim_device') }} AS device ON booking.device_id = device.device_id
    LEFT JOIN {{ ref('fact_booking_trip') }} AS booking_trip ON booking.booking_id = booking_trip.booking_id
    CROSS JOIN config
    CROSS JOIN report_dates
  WHERE
    DATE(booking.date_of_checkout) BETWEEN config.start_date AND config.end_date
    AND booking.status_id IN (1, 2)
  GROUP BY 1, 2
)

, app_installs AS (
  SELECT
    report_dates.date
    , 'app' AS platform
    , COUNT(DISTINCT CASE WHEN events.date = report_dates.date THEN user.visitor_id END) AS visitors_app_installs
    , COUNT(DISTINCT CASE WHEN events.date BETWEEN DATE_SUB(report_dates.date, 6) AND report_dates.date THEN user.visitor_id END) AS visitors_app_installs_last_7_days
  FROM
    {{ source('default', 'events') }} AS events
    CROSS JOIN config
    CROSS JOIN report_dates
  WHERE
    events.event_name = 'AppInstall'
    AND events.date BETWEEN config.start_date AND config.end_date
  GROUP BY 1, 2
)

SELECT
  *
FROM
  session_metrics
  LEFT JOIN booking_metrics USING (date, platform)
  LEFT JOIN app_installs USING (date, platform)