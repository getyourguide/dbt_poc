{{ config(schema=var('default')) }}



WITH under8_dtc_cancellation_rate AS
(
  SELECT date_trunc('week',date_of_checkout) AS checkout_week
         ,'under8' AS dtc_bucket
         ,SUM(CASE WHEN status_id = 2 THEN nr END) / SUM(nr) AS cancellation_rate
  FROM {{ source('dwh', 'fact_booking') }}
  WHERE status_id IN (1,2)
  AND   date_of_checkout >= date_sub(date_trunc('week',CURRENT_DATE),14)
  AND   date_of_checkout < date_sub(date_trunc('week',CURRENT_DATE),7)
  AND   datediff(DATE (to_utc_timestamp(to_timestamp(date_of_travel_utc),'Europe/Berlin')),DATE (to_utc_timestamp(date_of_checkout,'Europe/Berlin'))) < 8
  GROUP BY 1, 2
),

customer_first_checkout AS (
SELECT DISTINCT customer_id,
       first_date_of_checkout
FROM {{ source('dwh', 'dim_transaction_cohort') }}
)
,

booking_cancellation_cte(
    select distinct fbcr.booking_id
    ,FIRST_VALUE(COALESCE(fbcr.booking_cancelation_reason_id,0)) over (partition by fbcr.booking_id order by fbcr.date_of_cancelation desc) as booking_cancelation_reason_id
    from {{ source('dwh', 'fact_booking_cancelation_request') }} fbcr
)
,
booking_base AS
(
  SELECT DATE (fact_booking.date_of_checkout) AS checkout_date
         , DATE (to_timestamp (fact_booking.date_of_travel_utc)) AS date_of_travel_berlin_time
         , DATE (fact_booking.date_of_travel) AS travel_date
         , fact_booking.date_of_travel_utc
         , datediff(DATE (to_utc_timestamp (to_timestamp (fact_booking.date_of_travel_utc),'Europe/Berlin')),DATE (to_utc_timestamp (fact_booking.date_of_checkout,'Europe/Berlin'))) AS days_to_conduction
         , DATE (fact_booking.date_of_cancelation) AS date_of_cancelation
         , fact_booking.customer_id
         , tc.first_date_of_checkout AS customer_first_checkout_date
         , fact_booking.booking_id AS booking_id
         , fact_booking.status_id
         , fact_booking.customer_country_id AS source_market_country_id
         , fact_booking.shopping_cart_id
         , fact_booking.borders
         , co.country_name AS source_market_country_name
         , co.country_group AS country_group_source
         , tour.location_id AS destination_location_id
         , lo.city_id AS destination_city_id
         , lo.city_name AS destination_city_name
         , purchase_type.purchase_type_name AS purchase_type
         , fact_booking.nr AS nr
         , fact_booking.gmv AS gmv
         , fact_booking.tour_id AS tour_id
         , fact_booking.gmv_supplier AS gmv_supplier
         , fact_booking.reseller_share AS reseller_share
         , fact_booking.reseller_id
         , fact_booking.supplier_share
         , fact_booking.gmv / nullif(fact_booking.gmv_total,0) AS booking_weight
         , fact_booking.total_tax AS total_tax
         , fact_booking.selling_tax_eur
         , fact_booking.net_tax_eur
         , fact_booking.tour_option_id
         , shopping_cart.purchase_type_id
         , shopping_cart.billing_id
         , COUNT(fact_booking.booking_id) OVER (PARTITION BY fact_booking.shopping_cart_id) AS shopping_cart_bookings
         , COUNT(fact_booking.booking_id) OVER (PARTITION BY fact_booking.customer_id,DATE (fact_booking.date_of_checkout)) AS daily_customer_bookings
         , COUNT(fact_booking.booking_id) OVER (PARTITION BY fact_booking.customer_id,DATE (date_trunc ('month',fact_booking.date_of_checkout))) AS monthly_customer_bookings
         , COUNT(fact_booking.booking_id) OVER (PARTITION BY fact_booking.customer_id,DATE (date_trunc ('quarter',fact_booking.date_of_checkout))) AS quarterly_customer_bookings
         , COUNT(fact_booking.booking_id) OVER (PARTITION BY fact_booking.customer_id,DATE (date_trunc ('yearly',fact_booking.date_of_checkout))) AS yearly_customer_bookings
         , b.channel AS reseller_channel
         , COALESCE(user_history.is_gyg_supplier, FALSE) AS is_inventory_relevant
         , lo.sales_area
         , fact_booking.category AS cancellation_category
         , fact_booking.sub_category as cancellation_sub_category
         , CASE
             WHEN DATE (fact_booking.date_of_cancelation) > DATE (fact_booking.date_of_travel) AND fact_booking.status_id = 2 THEN 'Yes'
             ELSE 'No'
           END AS is_cancelled_after_travel
         , CASE
             WHEN datediff (DATE (to_utc_timestamp (to_timestamp (fact_booking.date_of_travel_utc),'Europe/Berlin')),DATE (to_utc_timestamp (fact_booking.date_of_checkout,'Europe/Berlin'))) <= 7 THEN 'under8'
             WHEN datediff (DATE (to_utc_timestamp (to_timestamp (fact_booking.date_of_travel_utc),'Europe/Berlin')),DATE (to_utc_timestamp (fact_booking.date_of_checkout,'Europe/Berlin'))) >= 8 AND datediff (DATE (to_utc_timestamp (to_timestamp (fact_booking.date_of_travel_utc),'Europe/Berlin')),DATE (to_utc_timestamp (fact_booking.date_of_checkout,'Europe/Berlin'))) <= 28 THEN '8_to_28'
           ELSE 'over28'
         END AS dtc_bucket
         , CASE WHEN (fact_booking.status_id=2 AND ((bcc.booking_cancelation_reason_id IN (385, 386, 387, 390)) OR (bcc.booking_cancelation_reason_id IN (384, 388, 389, 391, 392)))) THEN 'YES'
          END as is_supplier_force_majeure
         , COALESCE(shopping_cart.is_rnpl, FALSE) AS is_rnpl
  FROM {{ source('dwh', 'fact_booking') }} AS fact_booking
    LEFT JOIN {{ source('dwh', 'dim_tour') }} AS tour ON fact_booking.tour_id = tour.tour_id
    LEFT JOIN {{ source('dwh', 'dim_user_history') }} AS user_history ON fact_booking.supplier_id = user_history.user_id AND fact_booking.date_of_checkout BETWEEN user_history.update_timestamp AND user_history.update_timestamp_next
    LEFT JOIN {{ source('dwh', 'dim_location') }} AS lo ON tour.location_id = lo.location_id
    LEFT JOIN {{ source('dwh', 'fact_shopping_cart') }} AS shopping_cart ON fact_booking.shopping_cart_id = shopping_cart.shopping_cart_id
    LEFT JOIN {{ source('dwh', 'dim_country') }} AS co ON fact_booking.customer_country_id = co.country_id
    LEFT JOIN {{ source('dwh', 'dim_purchase_type') }} AS purchase_type ON shopping_cart.purchase_type_id = purchase_type.purchase_type_id
    LEFT JOIN {{ source('dwh', 'dim_reseller') }} b ON fact_booking.reseller_id = b.reseller_id
    LEFT JOIN customer_first_checkout AS tc ON fact_booking.customer_id = tc.customer_id
    LEFT JOIN booking_cancellation_cte bcc on fact_booking.booking_id = bcc.booking_id
  WHERE fact_booking.status_id IN (1,2)
  AND   fact_booking.date_of_checkout >= '2017-01-01'
)
SELECT booking_base.*
       , CASE
           WHEN booking_base.status_id = 1 AND booking_base.travel_date > DATE (DATE_SUB (CURRENT_DATE(),1)) AND booking_base.days_to_conduction <= 7 THEN under8_cx_rate.cancellation_rate
           WHEN booking_base.status_id = 1 AND booking_base.travel_date > DATE (DATE_SUB (CURRENT_DATE(),1)) AND booking_base.days_to_conduction >= 8 AND booking_base.days_to_conduction <= 28 THEN 0.15
           WHEN booking_base.status_id = 1 AND booking_base.travel_date > DATE (DATE_SUB (CURRENT_DATE(),1)) AND booking_base.days_to_conduction > 28 THEN 0.28
           ELSE NULL -- for the bookings without cancellation_rate
         END AS forcasted_cancellation_rate
FROM booking_base
LEFT JOIN under8_dtc_cancellation_rate under8_cx_rate ON booking_base.dtc_bucket = under8_cx_rate.dtc_bucket
