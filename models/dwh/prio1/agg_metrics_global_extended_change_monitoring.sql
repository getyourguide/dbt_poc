{{ config(schema=var('reports')) }}





with mge_cur AS (
SELECT CAST(date AS string) AS pk
     , snapshot_date AS snapshot_date_cur
     ----
     , BOOKINGS AS BOOKINGS_cur
     , BOOKINGS_LAST_14_DAYS AS BOOKINGS_LAST_14_DAYS_cur
     , bookings_last_year AS bookings_last_year_cur
     , bookings_trip_repeat_last_week AS bookings_trip_repeat_last_week_cur
     , contribution_margin_checkout_last_year AS contribution_margin_checkout_last_year_cur
     , CUSTOMERS AS CUSTOMERS_cur
     , customers_last_year AS customers_last_year_cur
     , FIXED_AD_SPEND AS FIXED_AD_SPEND_cur
     , GMV AS GMV_cur
     , gmv_last_year AS gmv_last_year_cur
     , NR AS NR_cur
     , NR_COMPONENTS AS NR_COMPONENTS_cur
     , nr_components_last_year AS nr_components_last_year_cur
     , nr_last_year AS nr_last_year_cur
     , NR_NON_PAID_CHANNELS AS NR_NON_PAID_CHANNELS_cur
     , nr_non_paid_channels_last_year AS nr_non_paid_channels_last_year_cur
     , nr_paid_search_last_year AS nr_paid_search_last_year_cur
     , NR_PERFORMANCE_MARKETING AS NR_PERFORMANCE_MARKETING_cur
     , nr_performance_marketing_last_year AS nr_performance_marketing_last_year_cur
     , NR_STR AS NR_STR_cur
     , nr_str_last_year AS nr_str_last_year_cur
     , nr_trip_repeat_last_year AS nr_trip_repeat_last_year_cur
     , QUOTERS AS QUOTERS_cur
     , quoters_last_year AS quoters_last_year_cur
     , ROAS_PAID_SEARCH AS ROAS_PAID_SEARCH_cur
     , roas_paid_search_last_year AS roas_paid_search_last_year_cur
     , ROAS_PERFORMANCE_MARKETING AS ROAS_PERFORMANCE_MARKETING_cur
     , roas_performance_marketing_last_year AS roas_performance_marketing_last_year_cur
     , ROAS_REAL_TIME AS ROAS_REAL_TIME_cur
     , roas_real_time_last_year AS roas_real_time_last_year_cur
     , TRANSACTIONS AS TRANSACTIONS_cur
     , transactions_last_year AS transactions_last_year_cur
     , trip_customers_acq AS trip_customers_acq_cur
     , trip_customers_next_trip_acq AS trip_customers_next_trip_acq_cur
     , trip_customers_trip_repeat AS trip_customers_trip_repeat_cur
     , VISITORS AS VISITORS_cur
     , visitors_last_year AS visitors_last_year_cur
  FROM {{ ref('agg_metrics_global_extended_snapshot') }}
 WHERE snapshot_date = CURRENT_DATE
   AND date(date) <= date_sub(CURRENT_DATE,5) -- last date in the previous snapshot + ROAS/adspend is expected to change for the last 3 days
)
, mge_prv AS (
SELECT CAST(date AS string) AS pk
     , snapshot_date AS snapshot_date_prv
     ---
     , BOOKINGS AS BOOKINGS_prv
     , BOOKINGS_LAST_14_DAYS AS BOOKINGS_LAST_14_DAYS_prv
     , bookings_last_year AS bookings_last_year_prv
     , bookings_trip_repeat_last_week AS bookings_trip_repeat_last_week_prv
     , contribution_margin_checkout_last_year AS contribution_margin_checkout_last_year_prv
     , CUSTOMERS AS CUSTOMERS_prv
     , customers_last_year AS customers_last_year_prv
     , FIXED_AD_SPEND AS FIXED_AD_SPEND_prv
     , GMV AS GMV_prv
     , gmv_last_year AS gmv_last_year_prv
     , NR AS NR_prv
     , NR_COMPONENTS AS NR_COMPONENTS_prv
     , nr_components_last_year AS nr_components_last_year_prv
     , nr_last_year AS nr_last_year_prv
     , NR_NON_PAID_CHANNELS AS NR_NON_PAID_CHANNELS_prv
     , nr_non_paid_channels_last_year AS nr_non_paid_channels_last_year_prv
     , nr_paid_search_last_year AS nr_paid_search_last_year_prv
     , NR_PERFORMANCE_MARKETING AS NR_PERFORMANCE_MARKETING_prv
     , nr_performance_marketing_last_year AS nr_performance_marketing_last_year_prv
     , NR_STR AS NR_STR_prv
     , nr_str_last_year AS nr_str_last_year_prv
     , nr_trip_repeat_last_year AS nr_trip_repeat_last_year_prv
     , QUOTERS AS QUOTERS_prv
     , quoters_last_year AS quoters_last_year_prv
     , ROAS_PAID_SEARCH AS ROAS_PAID_SEARCH_prv
     , roas_paid_search_last_year AS roas_paid_search_last_year_prv
     , ROAS_PERFORMANCE_MARKETING AS ROAS_PERFORMANCE_MARKETING_prv
     , roas_performance_marketing_last_year AS roas_performance_marketing_last_year_prv
     , ROAS_REAL_TIME AS ROAS_REAL_TIME_prv
     , roas_real_time_last_year AS roas_real_time_last_year_prv
     , TRANSACTIONS AS TRANSACTIONS_prv
     , transactions_last_year AS transactions_last_year_prv
     , trip_customers_acq AS trip_customers_acq_prv
     , trip_customers_next_trip_acq AS trip_customers_next_trip_acq_prv
     , trip_customers_trip_repeat AS trip_customers_trip_repeat_prv
     , VISITORS AS VISITORS_prv
     , visitors_last_year AS visitors_last_year_prv
  FROM {{ ref('agg_metrics_global_extended_snapshot') }}
 WHERE snapshot_date = date_sub(CURRENT_DATE, 1)
   AND date(date) <= date_sub(CURRENT_DATE,5)
)

, mge_BOOKINGS AS (
SELECT 'reports.agg_metrics_global_extended' AS table_name
     , 'date'  AS pk
     , pk     AS pk_value
     , 'BOOKINGS'          AS column_name
     , BOOKINGS_cur AS current_value_num
     , BOOKINGS_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(BOOKINGS_cur,0) - COALESCE(BOOKINGS_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mge_cur c
  FULL JOIN mge_prv p USING (pk)
 WHERE (ABS(COALESCE(c.BOOKINGS_cur, 0) / NULLIF(p.BOOKINGS_prv, 0)-1) * 100) > 0.1
)
, mge_BOOKINGS_LAST_14_DAYS AS (
SELECT 'reports.agg_metrics_global_extended' AS table_name
     , 'date'  AS pk
     , pk     AS pk_value
     , 'BOOKINGS_LAST_14_DAYS'          AS column_name
     , BOOKINGS_LAST_14_DAYS_cur AS current_value_num
     , BOOKINGS_LAST_14_DAYS_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(BOOKINGS_LAST_14_DAYS_cur,0) - COALESCE(BOOKINGS_LAST_14_DAYS_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mge_cur c
  FULL JOIN mge_prv p USING (pk)
 WHERE (ABS(COALESCE(c.BOOKINGS_LAST_14_DAYS_cur, 0) / NULLIF(p.BOOKINGS_LAST_14_DAYS_prv, 0)-1) * 100) > 0.1
)
, mge_bookings_last_year AS (
SELECT 'reports.agg_metrics_global_extended' AS table_name
     , 'date'  AS pk
     , pk     AS pk_value
     , 'BOOKINGS_LAST_YEAR'          AS column_name
     , bookings_last_year_cur AS current_value_num
     , bookings_last_year_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(bookings_last_year_cur,0) - COALESCE(bookings_last_year_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mge_cur c
  FULL JOIN mge_prv p USING (pk)
 WHERE (ABS(COALESCE(c.bookings_last_year_cur, 0) / NULLIF(p.bookings_last_year_prv, 0)-1) * 100) > 0.1
)
, mge_bookings_trip_repeat_last_week AS (
SELECT 'reports.agg_metrics_global_extended' AS table_name
     , 'date'  AS pk
     , pk     AS pk_value
     , 'BOOKINGS_TRIP_REPEAT_LAST_WEEK'          AS column_name
     , bookings_trip_repeat_last_week_cur AS current_value_num
     , bookings_trip_repeat_last_week_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(bookings_trip_repeat_last_week_cur,0) - COALESCE(bookings_trip_repeat_last_week_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mge_cur c
  FULL JOIN mge_prv p USING (pk)
 WHERE (ABS(COALESCE(c.bookings_trip_repeat_last_week_cur, 0) / NULLIF(p.bookings_trip_repeat_last_week_prv, 0)-1) * 100) > 0.1
)
, mge_contribution_margin_checkout_last_year AS (
SELECT 'reports.agg_metrics_global_extended' AS table_name
     , 'date'  AS pk
     , pk     AS pk_value
     , 'CONTRIBUTION_MARGIN_CHECKOUT_LAST_YEAR'          AS column_name
     , contribution_margin_checkout_last_year_cur AS current_value_num
     , contribution_margin_checkout_last_year_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(contribution_margin_checkout_last_year_cur,0) - COALESCE(contribution_margin_checkout_last_year_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mge_cur c
  FULL JOIN mge_prv p USING (pk)
 WHERE (ABS(COALESCE(c.contribution_margin_checkout_last_year_cur, 0) / NULLIF(p.contribution_margin_checkout_last_year_prv, 0)-1) * 100) > 0.1
)
, mge_CUSTOMERS AS (
SELECT 'reports.agg_metrics_global_extended' AS table_name
     , 'date'  AS pk
     , pk     AS pk_value
     , 'CUSTOMERS'          AS column_name
     , CUSTOMERS_cur AS current_value_num
     , CUSTOMERS_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(CUSTOMERS_cur,0) - COALESCE(CUSTOMERS_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mge_cur c
  FULL JOIN mge_prv p USING (pk)
 WHERE (ABS(COALESCE(c.CUSTOMERS_cur, 0) / NULLIF(p.CUSTOMERS_prv, 0)-1) * 100) > 0.1
)
, mge_customers_last_year AS (
SELECT 'reports.agg_metrics_global_extended' AS table_name
     , 'date'  AS pk
     , pk     AS pk_value
     , 'CUSTOMERS_LAST_YEAR'          AS column_name
     , customers_last_year_cur AS current_value_num
     , customers_last_year_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(customers_last_year_cur,0) - COALESCE(customers_last_year_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mge_cur c
  FULL JOIN mge_prv p USING (pk)
 WHERE (ABS(COALESCE(c.customers_last_year_cur, 0) / NULLIF(p.customers_last_year_prv, 0)-1) * 100) > 0.1
)
, mge_FIXED_AD_SPEND AS (
SELECT 'reports.agg_metrics_global_extended' AS table_name
     , 'date'  AS pk
     , pk     AS pk_value
     , 'FIXED_AD_SPEND'          AS column_name
     , FIXED_AD_SPEND_cur AS current_value_num
     , FIXED_AD_SPEND_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(FIXED_AD_SPEND_cur,0) - COALESCE(FIXED_AD_SPEND_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mge_cur c
  FULL JOIN mge_prv p USING (pk)
 WHERE (ABS(COALESCE(c.FIXED_AD_SPEND_cur, 0) / NULLIF(p.FIXED_AD_SPEND_prv, 0)-1) * 100) > 0.1
)
, mge_GMV AS (
SELECT 'reports.agg_metrics_global_extended' AS table_name
     , 'date'  AS pk
     , pk     AS pk_value
     , 'GMV'          AS column_name
     , GMV_cur AS current_value_num
     , GMV_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(GMV_cur,0) - COALESCE(GMV_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mge_cur c
  FULL JOIN mge_prv p USING (pk)
 WHERE (ABS(COALESCE(c.GMV_cur, 0) / NULLIF(p.GMV_prv, 0)-1) * 100) > 0.1
)
, mge_gmv_last_year AS (
SELECT 'reports.agg_metrics_global_extended' AS table_name
     , 'date'  AS pk
     , pk     AS pk_value
     , 'GMV_LAST_YEAR'          AS column_name
     , gmv_last_year_cur AS current_value_num
     , gmv_last_year_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(gmv_last_year_cur,0) - COALESCE(gmv_last_year_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mge_cur c
  FULL JOIN mge_prv p USING (pk)
 WHERE (ABS(COALESCE(c.gmv_last_year_cur, 0) / NULLIF(p.gmv_last_year_prv, 0)-1) * 100) > 0.1
)
, mge_NR AS (
SELECT 'reports.agg_metrics_global_extended' AS table_name
     , 'date'  AS pk
     , pk     AS pk_value
     , 'NR'          AS column_name
     , NR_cur AS current_value_num
     , NR_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(NR_cur,0) - COALESCE(NR_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mge_cur c
  FULL JOIN mge_prv p USING (pk)
 WHERE (ABS(COALESCE(c.NR_cur, 0) / NULLIF(p.NR_prv, 0)-1) * 100) > 0.1
)
, mge_NR_COMPONENTS AS (
SELECT 'reports.agg_metrics_global_extended' AS table_name
     , 'date'  AS pk
     , pk     AS pk_value
     , 'NR_COMPONENTS'          AS column_name
     , NR_COMPONENTS_cur AS current_value_num
     , NR_COMPONENTS_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(NR_COMPONENTS_cur,0) - COALESCE(NR_COMPONENTS_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mge_cur c
  FULL JOIN mge_prv p USING (pk)
 WHERE (ABS(COALESCE(c.NR_COMPONENTS_cur, 0) / NULLIF(p.NR_COMPONENTS_prv, 0)-1) * 100) > 0.1
)
, mge_nr_components_last_year AS (
SELECT 'reports.agg_metrics_global_extended' AS table_name
     , 'date'  AS pk
     , pk     AS pk_value
     , 'NR_COMPONENTS_LAST_YEAR'          AS column_name
     , nr_components_last_year_cur AS current_value_num
     , nr_components_last_year_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(nr_components_last_year_cur,0) - COALESCE(nr_components_last_year_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mge_cur c
  FULL JOIN mge_prv p USING (pk)
 WHERE (ABS(COALESCE(c.nr_components_last_year_cur, 0) / NULLIF(p.nr_components_last_year_prv, 0)-1) * 100) > 0.1
)
, mge_nr_last_year AS (
SELECT 'reports.agg_metrics_global_extended' AS table_name
     , 'date'  AS pk
     , pk     AS pk_value
     , 'NR_LAST_YEAR'          AS column_name
     , nr_last_year_cur AS current_value_num
     , nr_last_year_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(nr_last_year_cur,0) - COALESCE(nr_last_year_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mge_cur c
  FULL JOIN mge_prv p USING (pk)
 WHERE (ABS(COALESCE(c.nr_last_year_cur, 0) / NULLIF(p.nr_last_year_prv, 0)-1) * 100) > 0.1
)
, mge_NR_NON_PAID_CHANNELS AS (
SELECT 'reports.agg_metrics_global_extended' AS table_name
     , 'date'  AS pk
     , pk     AS pk_value
     , 'NR_NON_PAID_CHANNELS'          AS column_name
     , NR_NON_PAID_CHANNELS_cur AS current_value_num
     , NR_NON_PAID_CHANNELS_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(NR_NON_PAID_CHANNELS_cur,0) - COALESCE(NR_NON_PAID_CHANNELS_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mge_cur c
  FULL JOIN mge_prv p USING (pk)
 WHERE (ABS(COALESCE(c.NR_NON_PAID_CHANNELS_cur, 0) / NULLIF(p.NR_NON_PAID_CHANNELS_prv, 0)-1) * 100) > 0.1
)
, mge_nr_non_paid_channels_last_year AS (
SELECT 'reports.agg_metrics_global_extended' AS table_name
     , 'date'  AS pk
     , pk     AS pk_value
     , 'NR_NON_PAID_CHANNELS_LAST_YEAR'          AS column_name
     , nr_non_paid_channels_last_year_cur AS current_value_num
     , nr_non_paid_channels_last_year_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(nr_non_paid_channels_last_year_cur,0) - COALESCE(nr_non_paid_channels_last_year_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mge_cur c
  FULL JOIN mge_prv p USING (pk)
 WHERE (ABS(COALESCE(c.nr_non_paid_channels_last_year_cur, 0) / NULLIF(p.nr_non_paid_channels_last_year_prv, 0)-1) * 100) > 0.1
)
, mge_nr_paid_search_last_year AS (
SELECT 'reports.agg_metrics_global_extended' AS table_name
     , 'date'  AS pk
     , pk     AS pk_value
     , 'NR_PAID_SEARCH_LAST_YEAR'          AS column_name
     , nr_paid_search_last_year_cur AS current_value_num
     , nr_paid_search_last_year_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(nr_paid_search_last_year_cur,0) - COALESCE(nr_paid_search_last_year_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mge_cur c
  FULL JOIN mge_prv p USING (pk)
 WHERE (ABS(COALESCE(c.nr_paid_search_last_year_cur, 0) / NULLIF(p.nr_paid_search_last_year_prv, 0)-1) * 100) > 0.1
)
, mge_NR_PERFORMANCE_MARKETING AS (
SELECT 'reports.agg_metrics_global_extended' AS table_name
     , 'date'  AS pk
     , pk     AS pk_value
     , 'NR_PERFORMANCE_MARKETING'          AS column_name
     , NR_PERFORMANCE_MARKETING_cur AS current_value_num
     , NR_PERFORMANCE_MARKETING_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(NR_PERFORMANCE_MARKETING_cur,0) - COALESCE(NR_PERFORMANCE_MARKETING_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mge_cur c
  FULL JOIN mge_prv p USING (pk)
 WHERE (ABS(COALESCE(c.NR_PERFORMANCE_MARKETING_cur, 0) / NULLIF(p.NR_PERFORMANCE_MARKETING_prv, 0)-1) * 100) > 0.1
)
, mge_nr_performance_marketing_last_year AS (
SELECT 'reports.agg_metrics_global_extended' AS table_name
     , 'date'  AS pk
     , pk     AS pk_value
     , 'NR_PERFORMANCE_MARKETING_LAST_YEAR'          AS column_name
     , nr_performance_marketing_last_year_cur AS current_value_num
     , nr_performance_marketing_last_year_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(nr_performance_marketing_last_year_cur,0) - COALESCE(nr_performance_marketing_last_year_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mge_cur c
  FULL JOIN mge_prv p USING (pk)
 WHERE (ABS(COALESCE(c.nr_performance_marketing_last_year_cur, 0) / NULLIF(p.nr_performance_marketing_last_year_prv, 0)-1) * 100) > 0.1
)
, mge_NR_STR AS (
SELECT 'reports.agg_metrics_global_extended' AS table_name
     , 'date'  AS pk
     , pk     AS pk_value
     , 'NR_STR'          AS column_name
     , NR_STR_cur AS current_value_num
     , NR_STR_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(NR_STR_cur,0) - COALESCE(NR_STR_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mge_cur c
  FULL JOIN mge_prv p USING (pk)
 WHERE (ABS(COALESCE(c.NR_STR_cur, 0) / NULLIF(p.NR_STR_prv, 0)-1) * 100) > 0.1
)
, mge_nr_str_last_year AS (
SELECT 'reports.agg_metrics_global_extended' AS table_name
     , 'date'  AS pk
     , pk     AS pk_value
     , 'NR_STR_LAST_YEAR'          AS column_name
     , nr_str_last_year_cur AS current_value_num
     , nr_str_last_year_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(nr_str_last_year_cur,0) - COALESCE(nr_str_last_year_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mge_cur c
  FULL JOIN mge_prv p USING (pk)
 WHERE (ABS(COALESCE(c.nr_str_last_year_cur, 0) / NULLIF(p.nr_str_last_year_prv, 0)-1) * 100) > 0.1
)
, mge_nr_trip_repeat_last_year AS (
SELECT 'reports.agg_metrics_global_extended' AS table_name
     , 'date'  AS pk
     , pk     AS pk_value
     , 'NR_TRIP_REPEAT_LAST_YEAR'          AS column_name
     , nr_trip_repeat_last_year_cur AS current_value_num
     , nr_trip_repeat_last_year_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(nr_trip_repeat_last_year_cur,0) - COALESCE(nr_trip_repeat_last_year_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mge_cur c
  FULL JOIN mge_prv p USING (pk)
 WHERE (ABS(COALESCE(c.nr_trip_repeat_last_year_cur, 0) / NULLIF(p.nr_trip_repeat_last_year_prv, 0)-1) * 100) > 0.1
)
, mge_QUOTERS AS (
SELECT 'reports.agg_metrics_global_extended' AS table_name
     , 'date'  AS pk
     , pk     AS pk_value
     , 'QUOTERS'          AS column_name
     , QUOTERS_cur AS current_value_num
     , QUOTERS_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(QUOTERS_cur,0) - COALESCE(QUOTERS_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mge_cur c
  FULL JOIN mge_prv p USING (pk)
 WHERE (ABS(COALESCE(c.QUOTERS_cur, 0) / NULLIF(p.QUOTERS_prv, 0)-1) * 100) > 0.1
)
, mge_quoters_last_year AS (
SELECT 'reports.agg_metrics_global_extended' AS table_name
     , 'date'  AS pk
     , pk     AS pk_value
     , 'QUOTERS_LAST_YEAR'          AS column_name
     , quoters_last_year_cur AS current_value_num
     , quoters_last_year_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(quoters_last_year_cur,0) - COALESCE(quoters_last_year_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mge_cur c
  FULL JOIN mge_prv p USING (pk)
 WHERE (ABS(COALESCE(c.quoters_last_year_cur, 0) / NULLIF(p.quoters_last_year_prv, 0)-1) * 100) > 0.1
)
, mge_ROAS_PAID_SEARCH AS (
SELECT 'reports.agg_metrics_global_extended' AS table_name
     , 'date'  AS pk
     , pk     AS pk_value
     , 'ROAS_PAID_SEARCH'          AS column_name
     , ROAS_PAID_SEARCH_cur AS current_value_num
     , ROAS_PAID_SEARCH_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(ROAS_PAID_SEARCH_cur,0) - COALESCE(ROAS_PAID_SEARCH_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mge_cur c
  FULL JOIN mge_prv p USING (pk)
 WHERE (ABS(COALESCE(c.ROAS_PAID_SEARCH_cur, 0) / NULLIF(p.ROAS_PAID_SEARCH_prv, 0)-1) * 100) > 0.1
)
, mge_roas_paid_search_last_year AS (
SELECT 'reports.agg_metrics_global_extended' AS table_name
     , 'date'  AS pk
     , pk     AS pk_value
     , 'ROAS_PAID_SEARCH_LAST_YEAR'          AS column_name
     , roas_paid_search_last_year_cur AS current_value_num
     , roas_paid_search_last_year_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(roas_paid_search_last_year_cur,0) - COALESCE(roas_paid_search_last_year_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mge_cur c
  FULL JOIN mge_prv p USING (pk)
 WHERE (ABS(COALESCE(c.roas_paid_search_last_year_cur, 0) / NULLIF(p.roas_paid_search_last_year_prv, 0)-1) * 100) > 0.1
)
, mge_ROAS_PERFORMANCE_MARKETING AS (
SELECT 'reports.agg_metrics_global_extended' AS table_name
     , 'date'  AS pk
     , pk     AS pk_value
     , 'ROAS_PERFORMANCE_MARKETING'          AS column_name
     , ROAS_PERFORMANCE_MARKETING_cur AS current_value_num
     , ROAS_PERFORMANCE_MARKETING_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(ROAS_PERFORMANCE_MARKETING_cur,0) - COALESCE(ROAS_PERFORMANCE_MARKETING_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mge_cur c
  FULL JOIN mge_prv p USING (pk)
 WHERE (ABS(COALESCE(c.ROAS_PERFORMANCE_MARKETING_cur, 0) / NULLIF(p.ROAS_PERFORMANCE_MARKETING_prv, 0)-1) * 100) > 0.1
)
, mge_roas_performance_marketing_last_year AS (
SELECT 'reports.agg_metrics_global_extended' AS table_name
     , 'date'  AS pk
     , pk     AS pk_value
     , 'ROAS_PERFORMANCE_MARKETING_LAST_YEAR'          AS column_name
     , roas_performance_marketing_last_year_cur AS current_value_num
     , roas_performance_marketing_last_year_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(roas_performance_marketing_last_year_cur,0) - COALESCE(roas_performance_marketing_last_year_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mge_cur c
  FULL JOIN mge_prv p USING (pk)
 WHERE (ABS(COALESCE(c.roas_performance_marketing_last_year_cur, 0) / NULLIF(p.roas_performance_marketing_last_year_prv, 0)-1) * 100) > 0.1
)
, mge_ROAS_REAL_TIME AS (
SELECT 'reports.agg_metrics_global_extended' AS table_name
     , 'date'  AS pk
     , pk     AS pk_value
     , 'ROAS_REAL_TIME'          AS column_name
     , ROAS_REAL_TIME_cur AS current_value_num
     , ROAS_REAL_TIME_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(ROAS_REAL_TIME_cur,0) - COALESCE(ROAS_REAL_TIME_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mge_cur c
  FULL JOIN mge_prv p USING (pk)
 WHERE (ABS(COALESCE(c.ROAS_REAL_TIME_cur, 0) / NULLIF(p.ROAS_REAL_TIME_prv, 0)-1) * 100) > 0.1
)
, mge_roas_real_time_last_year AS (
SELECT 'reports.agg_metrics_global_extended' AS table_name
     , 'date'  AS pk
     , pk     AS pk_value
     , 'ROAS_REAL_TIME_LAST_YEAR'          AS column_name
     , roas_real_time_last_year_cur AS current_value_num
     , roas_real_time_last_year_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(roas_real_time_last_year_cur,0) - COALESCE(roas_real_time_last_year_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mge_cur c
  FULL JOIN mge_prv p USING (pk)
 WHERE (ABS(COALESCE(c.roas_real_time_last_year_cur, 0) / NULLIF(p.roas_real_time_last_year_prv, 0)-1) * 100) > 0.1
)
, mge_TRANSACTIONS AS (
SELECT 'reports.agg_metrics_global_extended' AS table_name
     , 'date'  AS pk
     , pk     AS pk_value
     , 'TRANSACTIONS'          AS column_name
     , TRANSACTIONS_cur AS current_value_num
     , TRANSACTIONS_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(TRANSACTIONS_cur,0) - COALESCE(TRANSACTIONS_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mge_cur c
  FULL JOIN mge_prv p USING (pk)
 WHERE (ABS(COALESCE(c.TRANSACTIONS_cur, 0) / NULLIF(p.TRANSACTIONS_prv, 0)-1) * 100) > 0.1
)
, mge_transactions_last_year AS (
SELECT 'reports.agg_metrics_global_extended' AS table_name
     , 'date'  AS pk
     , pk     AS pk_value
     , 'TRANSACTIONS_LAST_YEAR'          AS column_name
     , transactions_last_year_cur AS current_value_num
     , transactions_last_year_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(transactions_last_year_cur,0) - COALESCE(transactions_last_year_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mge_cur c
  FULL JOIN mge_prv p USING (pk)
 WHERE (ABS(COALESCE(c.transactions_last_year_cur, 0) / NULLIF(p.transactions_last_year_prv, 0)-1) * 100) > 0.1
)
, mge_trip_customers_acq AS (
SELECT 'reports.agg_metrics_global_extended' AS table_name
     , 'date'  AS pk
     , pk     AS pk_value
     , 'TRIP_CUSTOMERS_ACQ'          AS column_name
     , trip_customers_acq_cur AS current_value_num
     , trip_customers_acq_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(trip_customers_acq_cur,0) - COALESCE(trip_customers_acq_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mge_cur c
  FULL JOIN mge_prv p USING (pk)
 WHERE (ABS(COALESCE(c.trip_customers_acq_cur, 0) / NULLIF(p.trip_customers_acq_prv, 0)-1) * 100) > 0.1
)
, mge_trip_customers_next_trip_acq AS (
SELECT 'reports.agg_metrics_global_extended' AS table_name
     , 'date'  AS pk
     , pk     AS pk_value
     , 'TRIP_CUSTOMERS_NEXT_TRIP_ACQ'          AS column_name
     , trip_customers_next_trip_acq_cur AS current_value_num
     , trip_customers_next_trip_acq_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(trip_customers_next_trip_acq_cur,0) - COALESCE(trip_customers_next_trip_acq_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mge_cur c
  FULL JOIN mge_prv p USING (pk)
 WHERE (ABS(COALESCE(c.trip_customers_next_trip_acq_cur, 0) / NULLIF(p.trip_customers_next_trip_acq_prv, 0)-1) * 100) > 0.1
)
, mge_trip_customers_trip_repeat AS (
SELECT 'reports.agg_metrics_global_extended' AS table_name
     , 'date'  AS pk
     , pk     AS pk_value
     , 'TRIP_CUSTOMERS_TRIP_REPEAT'          AS column_name
     , trip_customers_trip_repeat_cur AS current_value_num
     , trip_customers_trip_repeat_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(trip_customers_trip_repeat_cur,0) - COALESCE(trip_customers_trip_repeat_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mge_cur c
  FULL JOIN mge_prv p USING (pk)
 WHERE (ABS(COALESCE(c.trip_customers_trip_repeat_cur, 0) / NULLIF(p.trip_customers_trip_repeat_prv, 0)-1) * 100) > 0.1
)
, mge_VISITORS AS (
SELECT 'reports.agg_metrics_global_extended' AS table_name
     , 'date'  AS pk
     , pk     AS pk_value
     , 'VISITORS'          AS column_name
     , VISITORS_cur AS current_value_num
     , VISITORS_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(VISITORS_cur,0) - COALESCE(VISITORS_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mge_cur c
  FULL JOIN mge_prv p USING (pk)
 WHERE (ABS(COALESCE(c.VISITORS_cur, 0) / NULLIF(p.VISITORS_prv, 0)-1) * 100) > 0.1
)
, mge_visitors_last_year AS (
SELECT 'reports.agg_metrics_global_extended' AS table_name
     , 'date'  AS pk
     , pk     AS pk_value
     , 'VISITORS_LAST_YEAR'          AS column_name
     , visitors_last_year_cur AS current_value_num
     , visitors_last_year_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(visitors_last_year_cur,0) - COALESCE(visitors_last_year_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mge_cur c
  FULL JOIN mge_prv p USING (pk)
 WHERE (ABS(COALESCE(c.visitors_last_year_cur, 0) / NULLIF(p.visitors_last_year_prv, 0)-1) * 100) > 0.1
)

SELECT *
  FROM mge_BOOKINGS
 UNION ALL
SELECT *
  FROM mge_BOOKINGS_LAST_14_DAYS
 UNION ALL
SELECT *
  FROM mge_bookings_last_year
 UNION ALL
SELECT *
  FROM mge_bookings_trip_repeat_last_week
 UNION ALL
SELECT *
  FROM mge_contribution_margin_checkout_last_year
 UNION ALL
SELECT *
  FROM mge_CUSTOMERS
 UNION ALL
SELECT *
  FROM mge_customers_last_year
 UNION ALL
SELECT *
  FROM mge_FIXED_AD_SPEND
 UNION ALL
SELECT *
  FROM mge_GMV
 UNION ALL
SELECT *
  FROM mge_gmv_last_year
 UNION ALL
SELECT *
  FROM mge_NR
 UNION ALL
SELECT *
  FROM mge_NR_COMPONENTS
 UNION ALL
SELECT *
  FROM mge_nr_components_last_year
 UNION ALL
SELECT *
  FROM mge_nr_last_year
 UNION ALL
SELECT *
  FROM mge_NR_NON_PAID_CHANNELS
 UNION ALL
SELECT *
  FROM mge_nr_non_paid_channels_last_year
 UNION ALL
SELECT *
  FROM mge_nr_paid_search_last_year
 UNION ALL
SELECT *
  FROM mge_NR_PERFORMANCE_MARKETING
 UNION ALL
SELECT *
  FROM mge_nr_performance_marketing_last_year
 UNION ALL
SELECT *
  FROM mge_NR_STR
 UNION ALL
SELECT *
  FROM mge_nr_str_last_year
 UNION ALL
SELECT *
  FROM mge_nr_trip_repeat_last_year
 UNION ALL
SELECT *
  FROM mge_QUOTERS
 UNION ALL
SELECT *
  FROM mge_quoters_last_year
 UNION ALL
SELECT *
  FROM mge_ROAS_PAID_SEARCH
 UNION ALL
SELECT *
  FROM mge_roas_paid_search_last_year
 UNION ALL
SELECT *
  FROM mge_ROAS_PERFORMANCE_MARKETING
 UNION ALL
SELECT *
  FROM mge_roas_performance_marketing_last_year
 UNION ALL
SELECT *
  FROM mge_ROAS_REAL_TIME
 UNION ALL
SELECT *
  FROM mge_roas_real_time_last_year
 UNION ALL
SELECT *
  FROM mge_TRANSACTIONS
 UNION ALL
SELECT *
  FROM mge_transactions_last_year
 UNION ALL
SELECT *
  FROM mge_trip_customers_acq
 UNION ALL
SELECT *
  FROM mge_trip_customers_next_trip_acq
 UNION ALL
SELECT *
  FROM mge_trip_customers_trip_repeat
 UNION ALL
SELECT *
  FROM mge_VISITORS
 UNION ALL
SELECT *
  FROM mge_visitors_last_year
