{{ config(schema=var('reports')) }}




WITH date_config AS (
       SELECT
         {% if is_incremental() %}
         -- -15 days here is used to enable calculating last 7 & 14 days with windows
         -- and then in the last part of the query we limit the period to quarter start
         DATE(DATE_TRUNC('QUARTER', '{{ var ('start-date') }}')) - INTERVAL 13 days AS start_date
         {% endif %}
         , a.date AS end_date
         {% if is_incremental() %}
         , DATE(DATE_TRUNC('QUARTER', '{{ var ('start-date') }}')) AS quarter_start_date
         {% endif %}
       FROM (SELECT
                {% if is_incremental() %}
                '{{ var ('end-date') }}'
                {% endif %}
                {% if target.name == 'dev' and not is_incremental() %}
                CURRENT_DATE()
                {% endif %}
                {% if target.name != 'dev' and not is_incremental() %}
                '2020-03-31'
                {% endif %}
                AS date
       ) a
     ),

dates AS (
    SELECT DISTINCT
        *
        , DATE(DATE_TRUNC('QUARTER', date_id)) AS quarter
        , ARRAY_MIN(ARRAY(DATE(date_id) - INTERVAL 13 days, DATE(DATE_TRUNC('QUARTER', date_id)))) AS join_start_date
        , DATE(date_id) - INTERVAL 13 days AS start_14_days
        , DATE(date_id) - INTERVAL 6 days AS start_7_days
    FROM {{ source('default', 'dim_date') }}
    CROSS JOIN date_config ON date_id BETWEEN start_date AND end_date
    WHERE 1=1
    {% if target.name == 'dev' and not is_incremental() %}
    LIMIT 1
    {% endif %}
),

agg_trip_customers AS (
   SELECT
       date_id as date
     , CASE
        WHEN booking_trip.booking_in_trip_number = 1 AND booking_trip.trip_number = 1 THEN 'Acquisition'
        WHEN booking_trip.booking_in_trip_number = 1 AND booking_trip.trip_number > 1 THEN 'Next Trip Acquisition'
        WHEN booking_trip.booking_in_trip_number > 1                                  THEN 'Trip Repeat'
       END trip_type
     , COUNT(DISTINCT CASE WHEN TO_DATE(booking_trip.booking_checkout_date) = date_id THEN customer_id END)                             AS customers
     , COUNT(DISTINCT CASE WHEN TO_DATE(booking_trip.booking_checkout_date) BETWEEN start_7_days AND date_id THEN customer_id END)      AS customers_last_7_days
     , COUNT(DISTINCT CASE WHEN TO_DATE(booking_trip.booking_checkout_date) BETWEEN start_14_days AND date_id THEN customer_id END)     AS customers_last_14_days
     , COUNT(DISTINCT CASE WHEN TO_DATE(booking_trip.booking_checkout_date) BETWEEN quarter AND date_id THEN customer_id ELSE NULL END) AS customers_qtd

   FROM dates
  INNER JOIN {{ ref('fact_booking_trip') }} AS booking_trip ON TO_DATE(booking_trip.booking_checkout_date) BETWEEN join_start_date and end_date
  -- WHERE bookings.status_id IN (1, 2) -- covered in the fact_booking_trip transformation
  GROUP BY 1,2
)

, summary AS (
SELECT date
     , SUM(CASE trip_type WHEN 'Acquisition' THEN customers              END) AS trip_customers_acq
     , SUM(CASE trip_type WHEN 'Acquisition' THEN customers_last_7_days  END) AS trip_customers_acq_last_7_days
     , SUM(CASE trip_type WHEN 'Acquisition' THEN customers_last_14_days END) AS trip_customers_acq_last_14_days
     , SUM(CASE trip_type WHEN 'Acquisition' THEN customers_qtd          END) AS trip_customers_acq_qtd

     , SUM(CASE trip_type WHEN 'Next Trip Acquisition' THEN customers              END) AS trip_customers_next_trip_acq
     , SUM(CASE trip_type WHEN 'Next Trip Acquisition' THEN customers_last_7_days  END) AS trip_customers_next_trip_acq_last_7_days
     , SUM(CASE trip_type WHEN 'Next Trip Acquisition' THEN customers_last_14_days END) AS trip_customers_next_trip_acq_last_14_days
     , SUM(CASE trip_type WHEN 'Next Trip Acquisition' THEN customers_qtd          END) AS trip_customers_next_trip_acq_qtd

     , SUM(CASE trip_type WHEN 'Trip Repeat' THEN customers              END) AS trip_customers_trip_repeat
     , SUM(CASE trip_type WHEN 'Trip Repeat' THEN customers_last_7_days  END) AS trip_customers_trip_repeat_last_7_days
     , SUM(CASE trip_type WHEN 'Trip Repeat' THEN customers_last_14_days END) AS trip_customers_trip_repeat_last_14_days
     , SUM(CASE trip_type WHEN 'Trip Repeat' THEN customers_qtd          END) AS trip_customers_trip_repeat_qtd
  FROM agg_trip_customers
 GROUP BY 1
)

 SELECT
        summary.*
   FROM summary
  WHERE summary.date >= (SELECT quarter_start_date FROM date_config)
