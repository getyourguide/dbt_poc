{{ config(schema=var('reports')) }}

-- rivulus syntax cannot be used for budget / forecast tables as these are updated in a notebook
-- so no reference / wait sensor is needed

WITH fact_forecast_raw_unioned (
SELECT date
     , planning_type
     , budget_config_id AS forecast_config_id
     , c.label AS forecast_label
     , bookings
     , ad_spend
     , CASE WHEN budget_segment_value1 IN ('Display & Paid Social', 'Paid Search', 'Performance Media') THEN ad_spend END AS ad_spend_performance_marketing
     , CASE WHEN budget_segment_value1 IN ('Paid Search') THEN ad_spend END AS ad_spend_paid_search
     , customers
     , gmv
     , nr
     , CASE WHEN budget_segment_value1 IN ('Display & Paid Social', 'Paid Search', 'Performance Media') THEN nr END AS nr_performance_marketing
     , CASE WHEN budget_segment_value1 IN ( 'Paid Search') THEN nr END AS nr_paid_search
     , seasonality
     , transactions
     , unique_customers
  FROM {{ source('dwh', 'fact_forecast') }}  f
 INNER JOIN {{ source('dwh', 'dim_forecast_config_abacus') }}  c ON c.forecast_id = f.budget_config_id AND f.date >= c.fc_start_date
 WHERE 1=1
   AND planning_type = 'Marketing'
       
 UNION ALL
       
SELECT date
     , 'Marketing' AS planning_type
     , c.forecast_id AS forecast_config_id
     , c.label AS forecast_label
     , bookings
     , realtime_ad_spend AS ad_spend -- +reseller_costs+coupon_costs
     , CASE WHEN channel IN ('Display & Paid Social', 'Paid Search', 'Performance Media') THEN ad_spend END AS ad_spend_performance_marketing
     , CASE WHEN channel IN ('Paid Search') THEN ad_spend END AS ad_spend_paid_search
     , customers
     , gmv
     , nr
     , CASE WHEN channel IN ('Display & Paid Social', 'Paid Search', 'Performance Media') THEN nr END AS nr_performance_marketing
     , CASE WHEN channel IN ( 'Paid Search') THEN nr END AS nr_paid_search
     , CAST(NULL AS float) AS seasonality
     , transactions
     , customers AS unique_customers
  FROM {{ ref('agg_metrics_drill_down') }} a --agg_metrics_marketing
 INNER JOIN dwh.dim_forecast_config_abacus c ON a.date < c.fc_start_date
 WHERE 1=1
   AND date >= '2021-01-01'
--- this the lowest date in the budget table
)

, fact_forecast AS (
 SELECT date
      , planning_type
      , forecast_config_id
      , forecast_label
      , SUM(bookings) as forecast_bookings
      , SUM(ad_spend) as forecast_ad_spend
      , SUM(ad_spend_performance_marketing) as forecast_ad_spend_performance_marketing
      , SUM(ad_spend_paid_search) as forecast_ad_spend_paid_search
      , SUM(customers) as forecast_customers
      , SUM(gmv) as forecast_gmv
      , SUM(nr) as forecast_nr
      , SUM(nr_performance_marketing) as forecast_nr_performance_marketing
      , SUM(nr_paid_search) as forecast_nr_paid_search
      , SUM(seasonality) as forecast_seasonality
      , SUM(transactions) as forecast_transactions
      , SUM(unique_customers) as forecast_unique_customers
   FROM fact_forecast_raw_unioned
  GROUP BY 1,2,3,4
)


SELECT date
     , planning_type
     , forecast_config_id
     , forecast_label
       -- FORECAST DATA
     , forecast_bookings
     , SUM(forecast_bookings) OVER (PARTITION BY forecast_config_id ORDER BY fb.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS forecast_bookings_last_7_days
     , SUM(forecast_bookings) OVER (PARTITION BY forecast_config_id ORDER BY fb.date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS forecast_bookings_last_14_days
     , SUM(forecast_bookings) OVER (PARTITION BY forecast_config_id, date_trunc('quarter', fb.date) ORDER BY fb.date) AS forecast_bookings_qtd
     , forecast_ad_spend
     , SUM(forecast_ad_spend) OVER (PARTITION BY forecast_config_id ORDER BY fb.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS forecast_ad_spend_last_7_days
     , SUM(forecast_ad_spend) OVER (PARTITION BY forecast_config_id ORDER BY fb.date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS forecast_ad_spend_last_14_days
     , SUM(forecast_ad_spend) OVER (PARTITION BY forecast_config_id, date_trunc('quarter', fb.date) ORDER BY fb.date) AS forecast_ad_spend_qtd
     , forecast_ad_spend_performance_marketing
     , SUM(forecast_ad_spend_performance_marketing) OVER (PARTITION BY forecast_config_id ORDER BY fb.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS forecast_ad_spend_performance_marketing_last_7_days
     , SUM(forecast_ad_spend_performance_marketing) OVER (PARTITION BY forecast_config_id ORDER BY fb.date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS forecast_ad_spend_performance_marketing_last_14_days
     , SUM(forecast_ad_spend_performance_marketing) OVER (PARTITION BY forecast_config_id, date_trunc('quarter', fb.date) ORDER BY fb.date) AS forecast_ad_spend_performance_marketing_qtd
     , forecast_ad_spend_paid_search
     , SUM(forecast_ad_spend_paid_search) OVER (PARTITION BY forecast_config_id ORDER BY fb.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS forecast_ad_spend_paid_search_last_7_days
     , SUM(forecast_ad_spend_paid_search) OVER (PARTITION BY forecast_config_id ORDER BY fb.date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS forecast_ad_spend_paid_search_last_14_days
     , SUM(forecast_ad_spend_paid_search) OVER (PARTITION BY forecast_config_id, date_trunc('quarter', fb.date) ORDER BY fb.date) AS forecast_ad_spend_paid_search_qtd
     , forecast_customers
     , SUM(forecast_customers) OVER (PARTITION BY forecast_config_id ORDER BY fb.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS forecast_customers_last_7_days
     , SUM(forecast_customers) OVER (PARTITION BY forecast_config_id ORDER BY fb.date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS forecast_customers_last_14_days
     , SUM(forecast_customers) OVER (PARTITION BY forecast_config_id, date_trunc('quarter', fb.date) ORDER BY fb.date) AS forecast_customers_qtd
     , forecast_gmv
     , SUM(forecast_gmv) OVER (PARTITION BY forecast_config_id ORDER BY fb.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS forecast_gmv_last_7_days
     , SUM(forecast_gmv) OVER (PARTITION BY forecast_config_id ORDER BY fb.date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS forecast_gmv_last_14_days
     , SUM(forecast_gmv) OVER (PARTITION BY forecast_config_id, date_trunc('quarter', fb.date) ORDER BY fb.date) AS forecast_gmv_qtd
     , forecast_nr
     , SUM(forecast_nr) OVER (PARTITION BY forecast_config_id ORDER BY fb.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS forecast_nr_last_7_days
     , SUM(forecast_nr) OVER (PARTITION BY forecast_config_id ORDER BY fb.date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS forecast_nr_last_14_days
     , SUM(forecast_nr) OVER (PARTITION BY forecast_config_id, date_trunc('quarter', fb.date) ORDER BY fb.date) AS forecast_nr_qtd
     , forecast_nr_performance_marketing
     , SUM(forecast_nr_performance_marketing) OVER (PARTITION BY forecast_config_id ORDER BY fb.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS forecast_nr_performance_marketing_last_7_days
     , SUM(forecast_nr_performance_marketing) OVER (PARTITION BY forecast_config_id ORDER BY fb.date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS forecast_nr_performance_marketing_last_14_days
     , SUM(forecast_nr_performance_marketing) OVER (PARTITION BY forecast_config_id, date_trunc('quarter', fb.date) ORDER BY fb.date) AS forecast_nr_performance_marketing_qtd
     , forecast_nr_paid_search
     , SUM(forecast_nr_paid_search) OVER (PARTITION BY forecast_config_id ORDER BY fb.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS forecast_nr_paid_search_last_7_days
     , SUM(forecast_nr_paid_search) OVER (PARTITION BY forecast_config_id ORDER BY fb.date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS forecast_nr_paid_search_last_14_days
     , SUM(forecast_nr_paid_search) OVER (PARTITION BY forecast_config_id, date_trunc('quarter', fb.date) ORDER BY fb.date) AS forecast_nr_paid_search_qtd
     , forecast_seasonality
     , SUM(forecast_seasonality) OVER (PARTITION BY forecast_config_id ORDER BY fb.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS forecast_seasonality_last_7_days
     , SUM(forecast_seasonality) OVER (PARTITION BY forecast_config_id ORDER BY fb.date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS forecast_seasonality_last_14_days
     , SUM(forecast_seasonality) OVER (PARTITION BY forecast_config_id, date_trunc('quarter', fb.date) ORDER BY fb.date) AS forecast_seasonality_qtd
     , forecast_transactions
     , SUM(forecast_transactions) OVER (PARTITION BY forecast_config_id ORDER BY fb.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS forecast_transactions_last_7_days
     , SUM(forecast_transactions) OVER (PARTITION BY forecast_config_id ORDER BY fb.date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS forecast_transactions_last_14_days
     , SUM(forecast_transactions) OVER (PARTITION BY forecast_config_id, date_trunc('quarter', fb.date) ORDER BY fb.date) AS forecast_transactions_qtd
     , forecast_unique_customers
     , SUM(forecast_unique_customers) OVER (PARTITION BY forecast_config_id ORDER BY fb.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS forecast_unique_customers_last_7_days
     , SUM(forecast_unique_customers) OVER (PARTITION BY forecast_config_id ORDER BY fb.date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS forecast_unique_customers_last_14_days
     , SUM(forecast_unique_customers) OVER (PARTITION BY forecast_config_id, date_trunc('quarter', fb.date) ORDER BY fb.date) AS forecast_unique_customers_qtd
  FROM fact_forecast fb
