{{ config(schema=var('reports')) }}




with mdd_cur AS (
SELECT COALESCE(CAST(report_date AS string), '') || COALESCE(country_group_source, '') || COALESCE(borders, '') || COALESCE(channel, '') || COALESCE(CAST(is_acquisition AS string), '') AS pk
     , snapshot_date AS snapshot_date_cur
     , ad_spend AS ad_spend_cur
     , realtime_ad_spend AS realtime_ad_spend_cur
     , reseller_costs AS reseller_costs_cur
     , coupon_costs AS coupon_costs_cur
     , bookings AS bookings_cur
     , customers AS customers_cur
     , gmv AS gmv_cur
     , nr AS nr_cur
     , transactions AS transactions_cur
     , gmv_last_year AS gmv_last_year_cur
     , ad_spend_last_7_days_last_year AS ad_spend_last_7_days_last_year_cur
  FROM {{ ref('agg_metrics_drill_down_3_years_snapshot') }}
 WHERE snapshot_date = CURRENT_DATE
   AND date(report_date) <= date_sub(CURRENT_DATE,5) -- last date in the previous snapshot + ROAS/adspend is expected to change for the last 3 days
)
, mdd_prv AS (
SELECT COALESCE(CAST(report_date AS string), '') || COALESCE(country_group_source, '') || COALESCE(borders, '') || COALESCE(channel, '') || COALESCE(CAST(is_acquisition AS string), '') AS pk
     , snapshot_date AS snapshot_date_prv
     , ad_spend AS ad_spend_prv
     , realtime_ad_spend AS realtime_ad_spend_prv
     , reseller_costs AS reseller_costs_prv
     , coupon_costs AS coupon_costs_prv
     , bookings AS bookings_prv
     , customers AS customers_prv
     , gmv AS gmv_prv
     , nr AS nr_prv
     , transactions AS transactions_prv
     , gmv_last_year AS gmv_last_year_prv
     , ad_spend_last_7_days_last_year AS ad_spend_last_7_days_last_year_prv
  FROM {{ ref('agg_metrics_drill_down_3_years_snapshot') }}
 WHERE snapshot_date = date_sub(CURRENT_DATE, 1)
   AND date(report_date) <= date_sub(CURRENT_DATE,5)
)

, mdd_ad_spend AS (
SELECT 'reports.agg_metrics_drill_down_3_years_snapshot' AS table_name
     , 'report_date || country_group_source || borders || channel || is_acquisition'  AS pk
     , pk     AS pk_value
     , 'AD_SPEND'          AS column_name
     , ad_spend_cur AS current_value_num
     , ad_spend_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(ad_spend_cur,0) - COALESCE(ad_spend_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mdd_cur c
  FULL JOIN mdd_prv p USING (pk)
 WHERE (ABS(COALESCE(c.ad_spend_cur, 0) / NULLIF(p.ad_spend_prv, 0)-1) * 100) > 0.1
)
, mdd_realtime_ad_spend AS (
SELECT 'reports.agg_metrics_drill_down_3_years_snapshot' AS table_name
     , 'report_date || country_group_source || borders || channel || is_acquisition'  AS pk
     , pk     AS pk_value
     , 'REALTIME_AD_SPEND'          AS column_name
     , realtime_ad_spend_cur AS current_value_num
     , realtime_ad_spend_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(realtime_ad_spend_cur,0) - COALESCE(realtime_ad_spend_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mdd_cur c
  FULL JOIN mdd_prv p USING (pk)
 WHERE (ABS(COALESCE(c.realtime_ad_spend_cur, 0) / NULLIF(p.realtime_ad_spend_prv, 0)-1) * 100) > 0.1
)
, mdd_reseller_costs AS (
SELECT 'reports.agg_metrics_drill_down_3_years_snapshot' AS table_name
     , 'report_date || country_group_source || borders || channel || is_acquisition'  AS pk
     , pk     AS pk_value
     , 'RESELLER_COSTS'          AS column_name
     , reseller_costs_cur AS current_value_num
     , reseller_costs_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(reseller_costs_cur,0) - COALESCE(reseller_costs_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mdd_cur c
  FULL JOIN mdd_prv p USING (pk)
 WHERE (ABS(COALESCE(c.reseller_costs_cur, 0) / NULLIF(p.reseller_costs_prv, 0)-1) * 100) > 0.1
)
, mdd_coupon_costs AS (
SELECT 'reports.agg_metrics_drill_down_3_years_snapshot' AS table_name
     , 'report_date || country_group_source || borders || channel || is_acquisition'  AS pk
     , pk     AS pk_value
     , 'COUPON_COSTS'          AS column_name
     , coupon_costs_cur AS current_value_num
     , coupon_costs_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(coupon_costs_cur,0) - COALESCE(coupon_costs_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mdd_cur c
  FULL JOIN mdd_prv p USING (pk)
 WHERE (ABS(COALESCE(c.coupon_costs_cur, 0) / NULLIF(p.coupon_costs_prv, 0)-1) * 100) > 0.1
)
, mdd_bookings AS (
SELECT 'reports.agg_metrics_drill_down_3_years_snapshot' AS table_name
     , 'report_date || country_group_source || borders || channel || is_acquisition'  AS pk
     , pk     AS pk_value
     , 'BOOKINGS'          AS column_name
     , bookings_cur AS current_value_num
     , bookings_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(bookings_cur,0) - COALESCE(bookings_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mdd_cur c
  FULL JOIN mdd_prv p USING (pk)
 WHERE (ABS(COALESCE(c.bookings_cur, 0) / NULLIF(p.bookings_prv, 0)-1) * 100) > 0.1
)
, mdd_customers AS (
SELECT 'reports.agg_metrics_drill_down_3_years_snapshot' AS table_name
     , 'report_date || country_group_source || borders || channel || is_acquisition'  AS pk
     , pk     AS pk_value
     , 'CUSTOMERS'          AS column_name
     , customers_cur AS current_value_num
     , customers_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(customers_cur,0) - COALESCE(customers_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mdd_cur c
  FULL JOIN mdd_prv p USING (pk)
 WHERE (ABS(COALESCE(c.customers_cur, 0) / NULLIF(p.customers_prv, 0)-1) * 100) > 0.1
)
, mdd_gmv AS (
SELECT 'reports.agg_metrics_drill_down_3_years_snapshot' AS table_name
     , 'report_date || country_group_source || borders || channel || is_acquisition'  AS pk
     , pk     AS pk_value
     , 'GMV'          AS column_name
     , gmv_cur AS current_value_num
     , gmv_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(gmv_cur,0) - COALESCE(gmv_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mdd_cur c
  FULL JOIN mdd_prv p USING (pk)
 WHERE (ABS(COALESCE(c.gmv_cur, 0) / NULLIF(p.gmv_prv, 0)-1) * 100) > 0.1
)
, mdd_nr AS (
SELECT 'reports.agg_metrics_drill_down_3_years_snapshot' AS table_name
     , 'report_date || country_group_source || borders || channel || is_acquisition'  AS pk
     , pk     AS pk_value
     , 'NR'          AS column_name
     , nr_cur AS current_value_num
     , nr_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(nr_cur,0) - COALESCE(nr_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mdd_cur c
  FULL JOIN mdd_prv p USING (pk)
 WHERE (ABS(COALESCE(c.nr_cur, 0) / NULLIF(p.nr_prv, 0)-1) * 100) > 0.1
)
, mdd_transactions AS (
SELECT 'reports.agg_metrics_drill_down_3_years_snapshot' AS table_name
     , 'report_date || country_group_source || borders || channel || is_acquisition'  AS pk
     , pk     AS pk_value
     , 'TRANSACTIONS'          AS column_name
     , transactions_cur AS current_value_num
     , transactions_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(transactions_cur,0) - COALESCE(transactions_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mdd_cur c
  FULL JOIN mdd_prv p USING (pk)
 WHERE (ABS(COALESCE(c.transactions_cur, 0) / NULLIF(p.transactions_prv, 0)-1) * 100) > 0.1
)
, mdd_gmv_last_year AS (
SELECT 'reports.agg_metrics_drill_down_3_years_snapshot' AS table_name
     , 'report_date || country_group_source || borders || channel || is_acquisition'  AS pk
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
  FROM mdd_cur c
  FULL JOIN mdd_prv p USING (pk)
 WHERE (ABS(COALESCE(c.gmv_last_year_cur, 0) / NULLIF(p.gmv_last_year_prv, 0)-1) * 100) > 0.1
)
, mdd_ad_spend_last_7_days_last_year AS (
SELECT 'reports.agg_metrics_drill_down_3_years_snapshot' AS table_name
     , 'report_date || country_group_source || borders || channel || is_acquisition'  AS pk
     , pk     AS pk_value
     , 'AD_SPEND_LAST_7_DAYS_LAST_YEAR'          AS column_name
     , ad_spend_last_7_days_last_year_cur AS current_value_num
     , ad_spend_last_7_days_last_year_prv AS previous_value_num
     , '' AS current_value_str
     , '' AS previous_value_str
     , COALESCE(ad_spend_last_7_days_last_year_cur,0) - COALESCE(ad_spend_last_7_days_last_year_prv,0) AS diff_num
     , snapshot_date_cur
     , snapshot_date_prv
     , CURRENT_TIMESTAMP AS check_timestamp
     , CURRENT_DATE AS check_date
  FROM mdd_cur c
  FULL JOIN mdd_prv p USING (pk)
 WHERE (ABS(COALESCE(c.ad_spend_last_7_days_last_year_cur, 0) / NULLIF(p.ad_spend_last_7_days_last_year_prv, 0)-1) * 100) > 0.1
)


SELECT *
  FROM mdd_ad_spend
 UNION ALL
SELECT *
  FROM mdd_realtime_ad_spend
 UNION ALL
SELECT *
  FROM mdd_reseller_costs
 UNION ALL
SELECT *
  FROM mdd_coupon_costs
 UNION ALL
SELECT *
  FROM mdd_bookings
 UNION ALL
SELECT *
  FROM mdd_customers
 UNION ALL
SELECT *
  FROM mdd_gmv
 UNION ALL
SELECT *
  FROM mdd_nr
 UNION ALL
SELECT *
  FROM mdd_transactions
 UNION ALL
SELECT *
  FROM mdd_gmv_last_year
 UNION ALL
SELECT *
  FROM mdd_ad_spend_last_7_days_last_year
