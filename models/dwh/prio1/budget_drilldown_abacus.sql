{{ config(schema=var('reports')) }}

WITH fact_budget_raw AS (
 SELECT date
      , planning_type
      , budget_config_id
      , c.label AS budget_label
      , CASE
          WHEN BUDGET_SEGMENT_VALUE1 = 'Direct & Brand' THEN 'Direct & Branded Links'
          WHEN BUDGET_SEGMENT_VALUE1 = 'Display & Paid Social' THEN 'Performance Media'
          ELSE BUDGET_SEGMENT_VALUE1
        END AS BUDGET_SEGMENT_VALUE1
      , CASE
          WHEN BUDGET_SEGMENT_VALUE2 = 'English RoW' THEN 'ANZ'
          ELSE BUDGET_SEGMENT_VALUE2
        END AS BUDGET_SEGMENT_VALUE2
      , BUDGET_SEGMENT_VALUE3
      , BUDGET_SEGMENT_VALUE4
      , customers         AS budget_customers
      , unique_customers  AS budget_unique_customers
      , transactions      AS budget_transactions
      , bookings          AS budget_bookings
      , gmv               AS budget_gmv
      , nr                AS budget_nr
      , ad_spend          AS budget_ad_spend
      , seasonality       AS budget_seasonality
   FROM {{ source('dwh', 'fact_budget') }} b
  INNER JOIN {{ source('dwh', 'dim_budget_config_abacus') }} c ON b.budget_config_id = c.budget_id
  WHERE 1=1
),

fact_budget AS (
 SELECT *
      , sum(budget_nr) OVER (PARTITION BY date_trunc('quarter', date), planning_type, budget_config_id, BUDGET_SEGMENT_VALUE1, BUDGET_SEGMENT_VALUE2, BUDGET_SEGMENT_VALUE3, BUDGET_SEGMENT_VALUE4 ORDER BY date) AS budget_nr_qtd
   FROM fact_budget_raw
),

fact_forecast AS (
 SELECT date
      , planning_type
      , budget_config_id AS forecast_config_id
      , c.label AS forecast_label
      , CASE
          WHEN BUDGET_SEGMENT_VALUE1 = 'Organic' THEN 'SEO'
          WHEN BUDGET_SEGMENT_VALUE1 IN ('Display & Paid Social', 'Performance Marketing') THEN 'Performance Media'
          WHEN BUDGET_SEGMENT_VALUE1 IN ('Strategic Partnerships','Travel Communities','In Destination') then 'Partnerships'
          WHEN BUDGET_SEGMENT_VALUE1 IN ('Navigational Search','Brand - Others','Brand - Referral') THEN 'Direct & Branded Links'
          ELSE BUDGET_SEGMENT_VALUE1
        END AS BUDGET_SEGMENT_VALUE1
      , CASE
          WHEN BUDGET_SEGMENT_VALUE2 = 'English RoW' THEN 'ANZ'
          ELSE BUDGET_SEGMENT_VALUE2
        END AS BUDGET_SEGMENT_VALUE2
      , BUDGET_SEGMENT_VALUE3
      , BUDGET_SEGMENT_VALUE4
      , sum(customers)         AS forecast_customers
      , sum(unique_customers)  AS forecast_unique_customers
      , sum(transactions)      AS forecast_transactions
      , sum(bookings)          AS forecast_bookings
      , sum(gmv)               AS forecast_gmv
      , sum(nr)                AS forecast_nr
      , sum(ad_spend)          AS forecast_ad_spend
      , avg(seasonality)       AS forecast_seasonality
   FROM {{ source('dwh', 'fact_forecast') }}  f
  INNER JOIN {{ source('dwh', 'dim_forecast_config_abacus') }} c ON c.forecast_id = f.budget_config_id
 -- this condition controls taking the actuals instead of the forecast data for 'Marketing' planning_type
                                             AND CASE WHEN planning_type='Marketing' THEN f.date >= c.fc_start_date ELSE true END
  WHERE 1=1
  GROUP BY 1,2,3,4,5,6,7,8
-- Union the actuals for taking the actuals instead of the forecast data for 'Marketing' planning_type
 UNION
SELECT date
     , 'Marketing' AS planning_type
     , c.forecast_id AS forecast_config_id
     , c.label AS forecast_label
     , channel AS BUDGET_SEGMENT_VALUE1
     , country_group_source AS BUDGET_SEGMENT_VALUE2
     , borders AS BUDGET_SEGMENT_VALUE3
     , CASE
         WHEN is_acquisition THEN 'Acquisition'
         WHEN is_acquisition = false THEN 'Repeat'
       END AS BUDGET_SEGMENT_VALUE4
     , customers AS forecast_customers
     , customers AS forecast_unique_customers
     , transactions AS forecast_transactions
     , bookings AS forecast_bookings
     , gmv AS forecast_gmv
     , nr AS forecast_nr
     , realtime_ad_spend AS forecast_ad_spend
     , NULL AS forecast_seasonality
  FROM {{ ref('agg_metrics_drill_down') }} a
 INNER JOIN {{ source('dwh', 'dim_forecast_config_abacus') }} c ON a.date < c.fc_start_date
 WHERE 1=1
   AND date >= '2021-01-01' 
)

,expanded_budget as (
select *
  from fact_budget
 cross join {{ source('dwh', 'dim_forecast_config_abacus') }}
)


,expanded_forecast as (
select *
  from fact_forecast
 cross join {{ source('dwh', 'dim_budget_config_abacus') }}
)
, final AS (
  SELECT COALESCE(ff.date, fb.date) as date
       , DATE(DATE_TRUNC('QUARTER', COALESCE(ff.date, fb.date))) AS quarter
       , COALESCE(ff.planning_type, fb.planning_type) as combined_planning_type
       , COALESCE(ff.forecast_config_id, fb.forecast_id) as forecast_config_id
       , COALESCE(ff.forecast_label, fb.label) as forecast_label
       , COALESCE(ff.budget_id, fb.budget_config_id) as budget_config_id 
       , COALESCE(ff.label, fb.budget_label) as budget_label
       , COALESCE(ff.BUDGET_SEGMENT_VALUE1, fb.BUDGET_SEGMENT_VALUE1) as BUDGET_SEGMENT_VALUE1
       , ff.BUDGET_SEGMENT_VALUE2 -- country groups changed and budget has old groups -- COALESCE(ff.BUDGET_SEGMENT_VALUE2, fb.BUDGET_SEGMENT_VALUE2) as BUDGET_SEGMENT_VALUE2
       , COALESCE(ff.BUDGET_SEGMENT_VALUE3, fb.BUDGET_SEGMENT_VALUE3) as BUDGET_SEGMENT_VALUE3
       , COALESCE(ff.BUDGET_SEGMENT_VALUE4, fb.BUDGET_SEGMENT_VALUE4) as BUDGET_SEGMENT_VALUE4
       , sum(forecast_customers) as forecast_customers
       , sum(forecast_unique_customers) as forecast_unique_customers
       , sum(forecast_transactions) as forecast_transactions
       , sum(forecast_bookings) as forecast_bookings
       , sum(forecast_gmv) as forecast_gmv
       , sum(forecast_nr) as forecast_nr
       , sum(forecast_ad_spend) as forecast_ad_spend
       , sum(forecast_seasonality) as forecast_seasonality
       , sum(budget_customers) as budget_customers
       , sum(budget_unique_customers) as budget_unique_customers
       , sum(budget_transactions) as budget_transactions
       , sum(budget_bookings) as budget_bookings
       , sum(budget_gmv) as budget_gmv
       , sum(budget_nr) as budget_nr
       , sum(budget_ad_spend) as budget_ad_spend
       , sum(budget_seasonality) as budget_seasonality
       , sum(budget_nr_qtd) as budget_nr_qtd
    FROM expanded_forecast ff
    FULL OUTER JOIN expanded_budget fb ON ff.date=fb.date
                                  AND ff.planning_type=fb.planning_type
                                  AND ff.BUDGET_SEGMENT_VALUE1=fb.BUDGET_SEGMENT_VALUE1
                                  AND ff.BUDGET_SEGMENT_VALUE2=fb.BUDGET_SEGMENT_VALUE2
                                  AND ff.BUDGET_SEGMENT_VALUE3=fb.BUDGET_SEGMENT_VALUE3
                                  AND ff.BUDGET_SEGMENT_VALUE4=fb.BUDGET_SEGMENT_VALUE4
                                  AND ff.forecast_config_id = fb.forecast_id
                                  AND ff.budget_id = fb.budget_config_id
   WHERE 1=1
   GROUP BY 1,2,3,4,5,6,7,8,9,10,11
)

, prep AS (
SELECT date
     , combined_planning_type
     , forecast_config_id
     , forecast_label
     , budget_config_id 
     , budget_label
     , BUDGET_SEGMENT_VALUE1
     , BUDGET_SEGMENT_VALUE2 -- country groups changed and budget has old groups -- COALESCE(ff.BUDGET_SEGMENT_VALUE2, fb.BUDGET_SEGMENT_VALUE2) as BUDGET_SEGMENT_VALUE2
     , BUDGET_SEGMENT_VALUE3
     , BUDGET_SEGMENT_VALUE4
     , forecast_customers
     , forecast_unique_customers
     , forecast_transactions
     , forecast_bookings
     , forecast_gmv
     , forecast_nr
     , forecast_ad_spend
     , forecast_seasonality
     , budget_customers
     , budget_unique_customers
     , budget_transactions
     , budget_bookings
     , budget_gmv
     , budget_nr
     , budget_ad_spend
     , budget_seasonality
     , budget_nr_qtd
     
     , SUM(forecast_customers) OVER w_last_7_days  AS forecast_customers_last_7_days
     , SUM(forecast_transactions) OVER w_last_7_days  AS forecast_transactions_last_7_days
     , SUM(forecast_bookings) OVER w_last_7_days  AS forecast_bookings_last_7_days
     , SUM(forecast_gmv) OVER w_last_7_days  AS forecast_gmv_last_7_days
     , SUM(forecast_nr) OVER w_last_7_days  AS forecast_nr_last_7_days
     , SUM(forecast_ad_spend) OVER w_last_7_days  AS forecast_ad_spend_last_7_days
     , SUM(forecast_seasonality) OVER w_last_7_days  AS forecast_seasonality_last_7_days
     , SUM(budget_customers) OVER w_last_7_days  AS budget_customers_last_7_days
     , SUM(budget_unique_customers) OVER w_last_7_days  AS budget_unique_customers_last_7_days
     , SUM(budget_transactions) OVER w_last_7_days  AS budget_transactions_last_7_days
     , SUM(budget_bookings) OVER w_last_7_days  AS budget_bookings_last_7_days
     , SUM(budget_gmv) OVER w_last_7_days  AS budget_gmv_last_7_days
     , SUM(budget_nr) OVER w_last_7_days  AS budget_nr_last_7_days
     , SUM(budget_ad_spend) OVER w_last_7_days  AS budget_ad_spend_last_7_days
     , SUM(budget_seasonality) OVER w_last_7_days  AS budget_seasonality_last_7_days

     , SUM(forecast_customers) OVER w_last_14_days  AS forecast_customers_last_14_days
     , SUM(forecast_transactions) OVER w_last_14_days  AS forecast_transactions_last_14_days
     , SUM(forecast_bookings) OVER w_last_14_days  AS forecast_bookings_last_14_days
     , SUM(forecast_gmv) OVER w_last_14_days  AS forecast_gmv_last_14_days
     , SUM(forecast_nr) OVER w_last_14_days  AS forecast_nr_last_14_days
     , SUM(forecast_ad_spend) OVER w_last_14_days  AS forecast_ad_spend_last_14_days
     , SUM(forecast_seasonality) OVER w_last_14_days  AS forecast_seasonality_last_14_days
     , SUM(budget_customers) OVER w_last_14_days  AS budget_customers_last_14_days
     , SUM(budget_unique_customers) OVER w_last_14_days  AS budget_unique_customers_last_14_days
     , SUM(budget_transactions) OVER w_last_14_days  AS budget_transactions_last_14_days
     , SUM(budget_bookings) OVER w_last_14_days  AS budget_bookings_last_14_days
     , SUM(budget_gmv) OVER w_last_14_days  AS budget_gmv_last_14_days
     , SUM(budget_nr) OVER w_last_14_days  AS budget_nr_last_14_days
     , SUM(budget_ad_spend) OVER w_last_14_days  AS budget_ad_spend_last_14_days
     , SUM(budget_seasonality) OVER w_last_14_days  AS budget_seasonality_last_14_days

  FROM final
WINDOW w_last_7_days AS (PARTITION BY combined_planning_type, forecast_config_id, budget_config_id, COALESCE(BUDGET_SEGMENT_VALUE1, ''), COALESCE(BUDGET_SEGMENT_VALUE2, ''), COALESCE(BUDGET_SEGMENT_VALUE3, ''), COALESCE(BUDGET_SEGMENT_VALUE4, '') ORDER BY date RANGE BETWEEN 6 PRECEDING AND CURRENT ROW)
     , w_last_14_days AS (PARTITION BY combined_planning_type, forecast_config_id, budget_config_id, COALESCE(BUDGET_SEGMENT_VALUE1, ''), COALESCE(BUDGET_SEGMENT_VALUE2, ''), COALESCE(BUDGET_SEGMENT_VALUE3, ''), COALESCE(BUDGET_SEGMENT_VALUE4, '') ORDER BY date RANGE BETWEEN 13 PRECEDING AND CURRENT ROW)

)
SELECT date
     , combined_planning_type
     , forecast_config_id
     , forecast_label
     , budget_config_id 
     , budget_label
     , BUDGET_SEGMENT_VALUE1
     , BUDGET_SEGMENT_VALUE2 -- country groups changed and budget has old groups -- COALESCE(ff.BUDGET_SEGMENT_VALUE2, fb.BUDGET_SEGMENT_VALUE2) as BUDGET_SEGMENT_VALUE2
     , BUDGET_SEGMENT_VALUE3
     , BUDGET_SEGMENT_VALUE4
     , forecast_customers
     , forecast_unique_customers
     , forecast_transactions
     , forecast_bookings
     , forecast_gmv
     , forecast_nr
     , forecast_ad_spend
     , forecast_seasonality
     , budget_customers
     , budget_unique_customers
     , budget_transactions
     , budget_bookings
     , budget_gmv
     , budget_nr
     , budget_ad_spend
     , budget_seasonality
     , budget_nr_qtd
     
     , forecast_customers_last_7_days
     , forecast_transactions_last_7_days
     , forecast_bookings_last_7_days
     , forecast_gmv_last_7_days
     , forecast_nr_last_7_days
     , forecast_ad_spend_last_7_days
     , forecast_seasonality_last_7_days
     , budget_customers_last_7_days
     , budget_unique_customers_last_7_days
     , budget_transactions_last_7_days
     , budget_bookings_last_7_days
     , budget_gmv_last_7_days
     , budget_nr_last_7_days
     , budget_ad_spend_last_7_days
     , budget_seasonality_last_7_days

     , forecast_customers_last_14_days
     , forecast_transactions_last_14_days
     , forecast_bookings_last_14_days
     , forecast_gmv_last_14_days
     , forecast_nr_last_14_days
     , forecast_ad_spend_last_14_days
     , forecast_seasonality_last_14_days
     , budget_customers_last_14_days
     , budget_unique_customers_last_14_days
     , budget_transactions_last_14_days
     , budget_bookings_last_14_days
     , budget_gmv_last_14_days
     , budget_nr_last_14_days
     , budget_ad_spend_last_14_days
     , budget_seasonality_last_14_days

     , forecast_customers_last_14_days - forecast_customers_last_7_days AS forecast_customers_prev_7_days
     , forecast_transactions_last_14_days - forecast_transactions_last_7_days AS forecast_transactions_prev_7_days
     , forecast_bookings_last_14_days - forecast_bookings_last_7_days AS forecast_bookings_prev_7_days
     , forecast_gmv_last_14_days - forecast_gmv_last_7_days AS forecast_gmv_prev_7_days
     , forecast_nr_last_14_days - forecast_nr_last_7_days AS forecast_nr_prev_7_days
     , forecast_ad_spend_last_14_days - forecast_ad_spend_last_7_days AS forecast_ad_spend_prev_7_days
     , forecast_seasonality_last_14_days - forecast_seasonality_last_7_days AS forecast_seasonality_prev_7_days
     , budget_customers_last_14_days - budget_customers_last_7_days AS budget_customers_prev_7_days
     , budget_unique_customers_last_14_days - budget_unique_customers_last_7_days AS budget_unique_customers_prev_7_days
     , budget_transactions_last_14_days - budget_transactions_last_7_days AS budget_transactions_prev_7_days
     , budget_bookings_last_14_days - budget_bookings_last_7_days AS budget_bookings_prev_7_days
     , budget_gmv_last_14_days - budget_gmv_last_7_days AS budget_gmv_prev_7_days
     , budget_nr_last_14_days - budget_nr_last_7_days AS budget_nr_prev_7_days
     , budget_ad_spend_last_14_days - budget_ad_spend_last_7_days AS budget_ad_spend_prev_7_days
     , budget_seasonality_last_14_days - budget_seasonality_last_7_days AS budget_seasonality_prev_7_days
  FROM prep