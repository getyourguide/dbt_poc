{{ config(schema=var('reports')) }}



WITH dates AS (
  SELECT base.date_id AS date
       , base.yoy_date_id AS yoy_date
       , next.yoy_date_id AS yo2y_date
       , y3.yoy_date_id AS yo3y_date
    FROM {{ source('public', 'dim_date_deprecated') }} base
    LEFT JOIN {{ source('public', 'dim_date_deprecated') }} next ON base.yoy_date_id = next.date_id
    LEFT JOIN {{ source('public', 'dim_date_deprecated') }} y3 ON next.yoy_date_id = y3.date_id
   WHERE base.date_id BETWEEN '2018-12-15' AND '2023-12-31'
   -- moved down the starting date to get same values as before when we did window functions
   -- without joining to all dims further down in the query
)
, metrics_drill_down_prep AS (
  SELECT date
       , channel
       , country_group_source
       , borders
       , is_acquisition
       , ad_spend
       , realtime_ad_spend -- includes reseller & coupon cost
       , reseller_costs
       , coupon_costs
       , bookings
       , customers
       , gmv
       , nr
       , transactions
       
  FROM {{ ref('agg_metrics_drill_down') }}
-- ORDER BY channel, country_group_source, borders, is_acquisition, date
)

, drill_down_dims AS (
  SELECT DISTINCT country_group_source
       , borders
       , channel
       , is_acquisition
    FROM metrics_drill_down_prep
)

, dims as (
  SELECT DISTINCT dates.date
       , yoy_date
       , yo2y_date
       , yo3y_date
       , mm.country_group_source
       , mm.borders
       , mm.channel
       , mm.is_acquisition
    FROM drill_down_dims mm
   CROSS JOIN dates
)

, metrics_drill_down AS (
  SELECT dims.date
       , dims.channel
       , dims.country_group_source
       , dims.borders
       , dims.is_acquisition
       , dims.yoy_date
       , dims.yo2y_date
       , dims.yo3y_date
       , ad_spend
       , SUM(ad_spend) OVER(w7)  AS ad_spend_last_7_days
       , SUM(ad_spend) OVER(w14) AS ad_spend_last_14_days
       , realtime_ad_spend -- includes reseller & coupon cost
       , SUM(realtime_ad_spend) OVER(w7)  AS realtime_ad_spend_last_7_days
       , SUM(realtime_ad_spend) OVER(w14) AS realtime_ad_spend_last_14_days
       , reseller_costs
       , SUM(reseller_costs) OVER(w7)  AS reseller_costs_last_7_days
       , SUM(reseller_costs) OVER(w14) AS reseller_costs_last_14_days
       , coupon_costs
       , SUM(coupon_costs) OVER(w7)  AS coupon_costs_last_7_days
       , SUM(coupon_costs) OVER(w14) AS coupon_costs_last_14_days
       , bookings
       , SUM(bookings) OVER(w7)  AS bookings_last_7_days
       , SUM(bookings) OVER(w14) AS bookings_last_14_days
       , customers
       , SUM(customers) OVER(w7)  AS customers_last_7_days
       , SUM(customers) OVER(w14) AS customers_last_14_days
       , gmv
       , SUM(gmv) OVER(w7)  AS gmv_last_7_days
       , SUM(gmv) OVER(w14) AS gmv_last_14_days
       , nr
       , SUM(nr) OVER(w7)  AS nr_last_7_days
       , SUM(nr) OVER(w14) AS nr_last_14_days
       , transactions
       , SUM(transactions) OVER(w7)  AS transactions_last_7_days
       , SUM(transactions) OVER(w14) AS transactions_last_14_days
       
       , SUM(ad_spend) OVER(wow)          AS ad_spend_last_week
       , SUM(realtime_ad_spend) OVER(wow) AS realtime_ad_spend_last_week
       , SUM(reseller_costs) OVER(wow)    AS reseller_costs_last_week
       , SUM(coupon_costs) OVER(wow)      AS coupon_costs_last_week
       , SUM(bookings) OVER(wow)          AS bookings_last_week
       , SUM(customers) OVER(wow)         AS customers_last_week
       , SUM(gmv) OVER(wow)               AS gmv_last_week
       , SUM(nr) OVER(wow)                AS nr_last_week
       , SUM(transactions) OVER(wow)      AS transactions_last_week
  -- we are joining here to ensure correct results for window functions as for some dimension value combinations we might not have all dates
  FROM dims
  LEFT JOIN metrics_drill_down_prep AS base 
         ON dims.date = base.date
        AND COALESCE(dims.country_group_source, 'key') = COALESCE(base.country_group_source, 'key')
        AND COALESCE(dims.borders, 'key') = COALESCE(base.borders, 'key')
        AND COALESCE(dims.channel, 'key') = COALESCE(base.channel, 'key')
        AND COALESCE(CAST(dims.is_acquisition AS string), 'key') = COALESCE(CAST(base.is_acquisition AS string), 'key')
 WHERE dims.date < CURRENT_DATE -- without this we will generate future values for window functions
  
WINDOW w7  AS ( PARTITION BY dims.channel, dims.country_group_source, dims.borders, dims.is_acquisition ORDER BY dims.date ROWS BETWEEN  6 PRECEDING AND CURRENT ROW)
     , w14 AS ( PARTITION BY dims.channel, dims.country_group_source, dims.borders, dims.is_acquisition ORDER BY dims.date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW)
     , wow AS ( PARTITION BY dims.channel, dims.country_group_source, dims.borders, dims.is_acquisition ORDER BY dims.date ROWS BETWEEN  7 PRECEDING AND 7 PRECEDING)
     -- opted not to use LAG to enable copy past of the code zstipanicev
 ORDER BY channel, country_group_source, borders, is_acquisition, date
)

SELECT COALESCE(dims.date, ly.date, py.date) AS report_date
     , dims.country_group_source
     , dims.borders
     , dims.channel
     , dims.is_acquisition
   
     , base.ad_spend
     , base.realtime_ad_spend
     , base.reseller_costs
     , base.coupon_costs
     , base.bookings
     , base.customers
     , base.gmv
     , base.nr
     , base.transactions
     
     , base.ad_spend_last_week
     , base.realtime_ad_spend_last_week
     , base.reseller_costs_last_week
     , base.coupon_costs_last_week
     , base.bookings_last_week
     , base.customers_last_week
     , base.gmv_last_week
     , base.nr_last_week
     , base.transactions_last_week
     
     , base.ad_spend_last_7_days
     , base.ad_spend_last_14_days
     , base.realtime_ad_spend_last_7_days
     , base.realtime_ad_spend_last_14_days
     , base.reseller_costs_last_7_days
     , base.reseller_costs_last_14_days
     , base.coupon_costs_last_7_days
     , base.coupon_costs_last_14_days
     , base.bookings_last_7_days
     , base.bookings_last_14_days
     , base.customers_last_7_days
     , base.customers_last_14_days
     , base.gmv_last_7_days
     , base.gmv_last_14_days
     , base.nr_last_7_days
     , base.nr_last_14_days
     , base.transactions_last_7_days
     , base.transactions_last_14_days

   
     , ly.ad_spend          AS ad_spend_last_year
     , ly.realtime_ad_spend AS realtime_ad_spend_last_year
     , ly.reseller_costs    AS reseller_costs_last_year
     , ly.coupon_costs      AS coupon_costs_last_year
     , ly.bookings          AS bookings_last_year
     , ly.customers         AS customers_last_year
     , ly.gmv               AS gmv_last_year
     , ly.nr                AS nr_last_year
     , ly.transactions      AS transactions_last_year
     
     , ly.ad_spend_last_week          AS ad_spend_last_week_last_year
     , ly.realtime_ad_spend_last_week AS realtime_ad_spend_last_week_last_year
     , ly.reseller_costs_last_week    AS reseller_costs_last_week_last_year
     , ly.coupon_costs_last_week      AS coupon_costs_last_week_last_year
     , ly.bookings_last_week          AS bookings_last_week_last_year
     , ly.customers_last_week         AS customers_last_week_last_year
     , ly.gmv_last_week               AS gmv_last_week_last_year
     , ly.nr_last_week                AS nr_last_week_last_year
     , ly.transactions_last_week      AS transactions_last_week_last_year

     , ly.ad_spend_last_7_days           AS ad_spend_last_7_days_last_year
     , ly.ad_spend_last_14_days          AS ad_spend_last_14_days_last_year
     , ly.realtime_ad_spend_last_7_days  AS realtime_ad_spend_last_7_days_last_year
     , ly.realtime_ad_spend_last_14_days AS realtime_ad_spend_last_14_days_last_year
     , ly.reseller_costs_last_7_days     AS reseller_costs_last_7_days_last_year
     , ly.reseller_costs_last_14_days    AS reseller_costs_last_14_days_last_year
     , ly.coupon_costs_last_7_days       AS coupon_costs_last_7_days_last_year
     , ly.coupon_costs_last_14_days      AS coupon_costs_last_14_days_last_year
     , ly.bookings_last_7_days           AS bookings_last_7_days_last_year
     , ly.bookings_last_14_days          AS bookings_last_14_days_last_year
     , ly.customers_last_7_days          AS customers_last_7_days_last_year
     , ly.customers_last_14_days         AS customers_last_14_days_last_year
     , ly.gmv_last_7_days                AS gmv_last_7_days_last_year
     , ly.gmv_last_14_days               AS gmv_last_14_days_last_year
     , ly.nr_last_7_days                 AS nr_last_7_days_last_year
     , ly.nr_last_14_days                AS nr_last_14_days_last_year
     , ly.transactions_last_7_days       AS transactions_last_7_days_last_year
     , ly.transactions_last_14_days      AS transactions_last_14_days_last_year


     , py.ad_spend          AS ad_spend_penultimate_year
     , py.realtime_ad_spend AS realtime_ad_spend_penultimate_year
     , py.reseller_costs    AS reseller_costs_penultimate_year
     , py.coupon_costs      AS coupon_costs_penultimate_year
     , py.bookings          AS bookings_penultimate_year
     , py.customers         AS customers_penultimate_year
     , py.gmv               AS gmv_penultimate_year
     , py.nr                AS nr_penultimate_year
     , py.transactions      AS transactions_penultimate_year

     , py.ad_spend_last_7_days           AS ad_spend_last_7_days_penultimate_year
     , py.ad_spend_last_14_days          AS ad_spend_last_14_days_penultimate_year
     , py.realtime_ad_spend_last_7_days  AS realtime_ad_spend_last_7_days_penultimate_year
     , py.realtime_ad_spend_last_14_days AS realtime_ad_spend_last_14_days_penultimate_year
     , py.reseller_costs_last_7_days     AS reseller_costs_last_7_days_penultimate_year
     , py.reseller_costs_last_14_days    AS reseller_costs_last_14_days_penultimate_year
     , py.coupon_costs_last_7_days       AS coupon_costs_last_7_days_penultimate_year
     , py.coupon_costs_last_14_days      AS coupon_costs_last_14_days_penultimate_year
     , py.bookings_last_7_days           AS bookings_last_7_days_penultimate_year
     , py.bookings_last_14_days          AS bookings_last_14_days_penultimate_year
     , py.customers_last_7_days          AS customers_last_7_days_penultimate_year
     , py.customers_last_14_days         AS customers_last_14_days_penultimate_year
     , py.gmv_last_7_days                AS gmv_last_7_days_penultimate_year
     , py.gmv_last_14_days               AS gmv_last_14_days_penultimate_year
     , py.nr_last_7_days                 AS nr_last_7_days_penultimate_year
     , py.nr_last_14_days                AS nr_last_14_days_penultimate_year
     , py.transactions_last_7_days       AS transactions_last_7_days_penultimate_year
     , py.transactions_last_14_days      AS transactions_last_14_days_penultimate_year

   
     , y3.ad_spend          AS ad_spend_3_years_ago
     , y3.realtime_ad_spend AS realtime_ad_spend_3_years_ago
     , y3.reseller_costs    AS reseller_costs_3_years_ago
     , y3.coupon_costs      AS coupon_costs_3_years_ago
     , y3.bookings          AS bookings_3_years_ago
     , y3.customers         AS customers_3_years_ago
     , y3.gmv               AS gmv_3_years_ago
     , y3.nr                AS nr_3_years_ago
     , y3.transactions      AS transactions_3_years_ago

     , y3.ad_spend_last_7_days           AS ad_spend_last_7_days_3_years_ago
     , y3.ad_spend_last_14_days          AS ad_spend_last_14_days_3_years_ago
     , y3.realtime_ad_spend_last_7_days  AS realtime_ad_spend_last_7_days_3_years_ago
     , y3.realtime_ad_spend_last_14_days AS realtime_ad_spend_last_14_days_3_years_ago
     , y3.reseller_costs_last_7_days     AS reseller_costs_last_7_days_3_years_ago
     , y3.reseller_costs_last_14_days    AS reseller_costs_last_14_days_3_years_ago
     , y3.coupon_costs_last_7_days       AS coupon_costs_last_7_days_3_years_ago
     , y3.coupon_costs_last_14_days      AS coupon_costs_last_14_days_3_years_ago
     , y3.bookings_last_7_days           AS bookings_last_7_days_3_years_ago
     , y3.bookings_last_14_days          AS bookings_last_14_days_3_years_ago
     , y3.customers_last_7_days          AS customers_last_7_days_3_years_ago
     , y3.customers_last_14_days         AS customers_last_14_days_3_years_ago
     , y3.gmv_last_7_days                AS gmv_last_7_days_3_years_ago
     , y3.gmv_last_14_days               AS gmv_last_14_days_3_years_ago
     , y3.nr_last_7_days                 AS nr_last_7_days_3_years_ago
     , y3.nr_last_14_days                AS nr_last_14_days_3_years_ago
     , y3.transactions_last_7_days       AS transactions_last_7_days_3_years_ago
     , y3.transactions_last_14_days      AS transactions_last_14_days_3_years_ago
  FROM dims
  LEFT JOIN metrics_drill_down AS base 
         ON dims.date = base.date
        AND COALESCE(dims.country_group_source, 'key') = COALESCE(base.country_group_source, 'key')
        AND COALESCE(dims.borders, 'key') = COALESCE(base.borders, 'key')
        AND COALESCE(dims.channel, 'key') = COALESCE(base.channel, 'key')
        AND COALESCE(CAST(dims.is_acquisition AS string), 'key') = COALESCE(CAST(base.is_acquisition AS string), 'key')
  LEFT JOIN metrics_drill_down AS ly
         ON dims.yoy_date = ly.date
        AND COALESCE(dims.country_group_source, 'key') = COALESCE(ly.country_group_source, 'key')
        AND COALESCE(dims.borders, 'key') = COALESCE(ly.borders, 'key')
        AND COALESCE(dims.channel, 'key') = COALESCE(ly.channel, 'key')
        AND COALESCE(CAST(dims.is_acquisition AS string), 'key') = COALESCE(CAST(ly.is_acquisition AS string), 'key')
  LEFT JOIN metrics_drill_down AS py
         ON dims.yo2y_date = py.date
        AND COALESCE(dims.country_group_source, 'key') = COALESCE(py.country_group_source, 'key')
        AND COALESCE(dims.borders, 'key') = COALESCE(py.borders, 'key')
        AND COALESCE(dims.channel, 'key') = COALESCE(py.channel, 'key')
        AND COALESCE(CAST(dims.is_acquisition AS string), 'key') = COALESCE(CAST(py.is_acquisition AS string), 'key')
  LEFT JOIN metrics_drill_down AS y3
         ON dims.yo3y_date = y3.date
        AND COALESCE(dims.country_group_source, 'key') = COALESCE(y3.country_group_source, 'key')
        AND COALESCE(dims.borders, 'key') = COALESCE(y3.borders, 'key')
        AND COALESCE(dims.channel, 'key') = COALESCE(y3.channel, 'key')
        AND COALESCE(CAST(dims.is_acquisition AS string), 'key') = COALESCE(CAST(y3.is_acquisition AS string), 'key')
  WHERE dims.date >= '2019-01-01'
