{{ config(schema=var('reports')) }}

-- rivulus syntax cannot be used for budget / forecast tables as these are updated in a notebook
-- so no reference / wait sensor is needed

WITH fact_budget (
SELECT date
     , planning_type
     , budget_config_id
     , label AS budget_label
     , SUM(bookings) as budget_bookings
     , SUM(ad_spend) as budget_ad_spend
     , SUM(CASE WHEN budget_segment_value1 IN ('Display & Paid Social', 'Paid Search', 'Performance Media') THEN ad_spend END) as budget_ad_spend_performance_marketing
     , SUM(CASE WHEN budget_segment_value1 IN ('Paid Search') THEN ad_spend END) as budget_ad_spend_paid_search
     , SUM(customers) as budget_customers
     , SUM(gmv) as budget_gmv
     , SUM(nr) as budget_nr
     , SUM(CASE WHEN budget_segment_value1 IN ('Display & Paid Social', 'Paid Search', 'Performance Media') THEN nr END) as budget_nr_performance_marketing
     , SUM(CASE WHEN budget_segment_value1 IN ( 'Paid Search') THEN nr END) as budget_nr_paid_search
     , SUM(seasonality) as budget_seasonality
     , SUM(transactions) as budget_transactions
     , SUM(unique_customers) as budget_unique_customers
  FROM {{ source('dwh', 'fact_budget') }}  b
 INNER JOIN {{ source('dwh', 'dim_budget_config_abacus') }} c ON b.budget_config_id = c.budget_id
 WHERE 1=1
   AND planning_type = 'Marketing'
 GROUP BY 1,2,3,4
)


SELECT date
     , planning_type
     , budget_config_id
     , budget_label
       -- FORECAST DATA
     , budget_bookings
     , SUM(budget_bookings) OVER (PARTITION BY budget_config_id ORDER BY fb.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS budget_bookings_last_7_days
     , SUM(budget_bookings) OVER (PARTITION BY budget_config_id ORDER BY fb.date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS budget_bookings_last_14_days
     , SUM(budget_bookings) OVER (PARTITION BY budget_config_id, date_trunc('quarter',fb.date) ORDER BY fb.date) AS budget_bookings_qtd
     , budget_ad_spend
     , SUM(budget_ad_spend) OVER (PARTITION BY budget_config_id ORDER BY fb.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS budget_ad_spend_last_7_days
     , SUM(budget_ad_spend) OVER (PARTITION BY budget_config_id ORDER BY fb.date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS budget_ad_spend_last_14_days
     , SUM(budget_ad_spend) OVER (PARTITION BY budget_config_id, date_trunc('quarter',fb.date) ORDER BY fb.date) AS budget_ad_spend_qtd
     , budget_ad_spend_performance_marketing
     , SUM(budget_ad_spend_performance_marketing) OVER (PARTITION BY budget_config_id ORDER BY fb.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS budget_ad_spend_performance_marketing_last_7_days
     , SUM(budget_ad_spend_performance_marketing) OVER (PARTITION BY budget_config_id ORDER BY fb.date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS budget_ad_spend_performance_marketing_last_14_days
     , SUM(budget_ad_spend_performance_marketing) OVER (PARTITION BY budget_config_id, date_trunc('quarter',fb.date) ORDER BY fb.date) AS budget_ad_spend_performance_marketing_qtd
     , budget_ad_spend_paid_search
     , SUM(budget_ad_spend_paid_search) OVER (PARTITION BY budget_config_id ORDER BY fb.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS budget_ad_spend_paid_search_last_7_days
     , SUM(budget_ad_spend_paid_search) OVER (PARTITION BY budget_config_id ORDER BY fb.date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS budget_ad_spend_paid_search_last_14_days
     , SUM(budget_ad_spend_paid_search) OVER (PARTITION BY budget_config_id, date_trunc('quarter',fb.date) ORDER BY fb.date) AS budget_ad_spend_paid_search_qtd
     , budget_customers
     , SUM(budget_customers) OVER (PARTITION BY budget_config_id ORDER BY fb.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS budget_customers_last_7_days
     , SUM(budget_customers) OVER (PARTITION BY budget_config_id ORDER BY fb.date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS budget_customers_last_14_days
     , SUM(budget_customers) OVER (PARTITION BY budget_config_id, date_trunc('quarter',fb.date) ORDER BY fb.date) AS budget_customers_qtd
     , budget_gmv
     , SUM(budget_gmv) OVER (PARTITION BY budget_config_id ORDER BY fb.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS budget_gmv_last_7_days
     , SUM(budget_gmv) OVER (PARTITION BY budget_config_id ORDER BY fb.date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS budget_gmv_last_14_days
     , SUM(budget_gmv) OVER (PARTITION BY budget_config_id, date_trunc('quarter',fb.date) ORDER BY fb.date) AS budget_gmv_qtd
     , budget_nr
     , SUM(budget_nr) OVER (PARTITION BY budget_config_id ORDER BY fb.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS budget_nr_last_7_days
     , SUM(budget_nr) OVER (PARTITION BY budget_config_id ORDER BY fb.date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS budget_nr_last_14_days
     , SUM(budget_nr) OVER (PARTITION BY budget_config_id, date_trunc('quarter',fb.date) ORDER BY fb.date) AS budget_nr_qtd
     , budget_nr_performance_marketing
     , SUM(budget_nr_performance_marketing) OVER (PARTITION BY budget_config_id ORDER BY fb.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS budget_nr_performance_marketing_last_7_days
     , SUM(budget_nr_performance_marketing) OVER (PARTITION BY budget_config_id ORDER BY fb.date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS budget_nr_performance_marketing_last_14_days
     , SUM(budget_nr_performance_marketing) OVER (PARTITION BY budget_config_id, date_trunc('quarter',fb.date) ORDER BY fb.date) AS budget_nr_performance_marketing_qtd
     , budget_nr_paid_search
     , SUM(budget_nr_paid_search) OVER (PARTITION BY budget_config_id ORDER BY fb.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS budget_nr_paid_search_last_7_days
     , SUM(budget_nr_paid_search) OVER (PARTITION BY budget_config_id ORDER BY fb.date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS budget_nr_paid_search_last_14_days
     , SUM(budget_nr_paid_search) OVER (PARTITION BY budget_config_id, date_trunc('quarter',fb.date) ORDER BY fb.date) AS budget_nr_paid_search_qtd
     , budget_seasonality
     , SUM(budget_seasonality) OVER (PARTITION BY budget_config_id ORDER BY fb.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS budget_seasonality_last_7_days
     , SUM(budget_seasonality) OVER (PARTITION BY budget_config_id ORDER BY fb.date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS budget_seasonality_last_14_days
     , SUM(budget_seasonality) OVER (PARTITION BY budget_config_id, date_trunc('quarter',fb.date) ORDER BY fb.date) AS budget_seasonality_qtd
     , budget_transactions
     , SUM(budget_transactions) OVER (PARTITION BY budget_config_id ORDER BY fb.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS budget_transactions_last_7_days
     , SUM(budget_transactions) OVER (PARTITION BY budget_config_id ORDER BY fb.date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS budget_transactions_last_14_days
     , SUM(budget_transactions) OVER (PARTITION BY budget_config_id, date_trunc('quarter',fb.date) ORDER BY fb.date) AS budget_transactions_qtd
     , budget_unique_customers
     , SUM(budget_unique_customers) OVER (PARTITION BY budget_config_id ORDER BY fb.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS budget_unique_customers_last_7_days
     , SUM(budget_unique_customers) OVER (PARTITION BY budget_config_id ORDER BY fb.date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS budget_unique_customers_last_14_days
     , SUM(budget_unique_customers) OVER (PARTITION BY budget_config_id, date_trunc('quarter',fb.date) ORDER BY fb.date) AS budget_unique_customers_qtd
  FROM fact_budget fb
