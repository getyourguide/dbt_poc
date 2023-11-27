{{ config(schema=var('reports')) }}



SELECT COALESCE(ff.date,fb.date) date
     , fb.planning_type
     , budget_config_id
     , budget_label
     , forecast_config_id
     , forecast_label

     -- FORECAST DATA
     , forecast_bookings
     , forecast_bookings_last_7_days
     , forecast_bookings_last_14_days
     , forecast_bookings_qtd
     , forecast_ad_spend
     , forecast_ad_spend_last_7_days
     , forecast_ad_spend_last_14_days
     , forecast_ad_spend_qtd
     , forecast_ad_spend_performance_marketing
     , forecast_ad_spend_performance_marketing_last_7_days
     , forecast_ad_spend_performance_marketing_last_14_days
     , forecast_ad_spend_performance_marketing_qtd
     , forecast_ad_spend_paid_search
     , forecast_ad_spend_paid_search_last_7_days
     , forecast_ad_spend_paid_search_last_14_days
     , forecast_ad_spend_paid_search_qtd
     , forecast_customers
     , forecast_customers_last_7_days
     , forecast_customers_last_14_days
     , forecast_customers_qtd
     , forecast_gmv
     , forecast_gmv_last_7_days
     , forecast_gmv_last_14_days
     , forecast_gmv_qtd
     , forecast_nr
     , forecast_nr_last_7_days
     , forecast_nr_last_14_days
     , forecast_nr_qtd
     , forecast_nr_performance_marketing
     , forecast_nr_performance_marketing_last_7_days
     , forecast_nr_performance_marketing_last_14_days
     , forecast_nr_performance_marketing_qtd
     , forecast_nr_paid_search
     , forecast_nr_paid_search_last_7_days
     , forecast_nr_paid_search_last_14_days
     , forecast_nr_paid_search_qtd
     , forecast_seasonality
     , forecast_seasonality_last_7_days
     , forecast_seasonality_last_14_days
     , forecast_seasonality_qtd
     , forecast_transactions
     , forecast_transactions_last_7_days
     , forecast_transactions_last_14_days
     , forecast_transactions_qtd
     , forecast_unique_customers
     , forecast_unique_customers_last_7_days
     , forecast_unique_customers_last_14_days
     , forecast_unique_customers_qtd

     -- BUDGET DATA
     , budget_bookings
     , budget_bookings_last_7_days
     , budget_bookings_last_14_days
     , budget_bookings_qtd
     , budget_ad_spend
     , budget_ad_spend_last_7_days
     , budget_ad_spend_last_14_days
     , budget_ad_spend_qtd
     , budget_ad_spend_performance_marketing
     , budget_ad_spend_performance_marketing_last_7_days
     , budget_ad_spend_performance_marketing_last_14_days
     , budget_ad_spend_performance_marketing_qtd
     , budget_ad_spend_paid_search
     , budget_ad_spend_paid_search_last_7_days
     , budget_ad_spend_paid_search_last_14_days
     , budget_ad_spend_paid_search_qtd
     , budget_customers
     , budget_customers_last_7_days
     , budget_customers_last_14_days
     , budget_customers_qtd
     , budget_gmv
     , budget_gmv_last_7_days
     , budget_gmv_last_14_days
     , budget_gmv_qtd
     , budget_nr
     , budget_nr_last_7_days
     , budget_nr_last_14_days
     , budget_nr_qtd
     , budget_nr_performance_marketing
     , budget_nr_performance_marketing_last_7_days
     , budget_nr_performance_marketing_last_14_days
     , budget_nr_performance_marketing_qtd
     , budget_nr_paid_search
     , budget_nr_paid_search_last_7_days
     , budget_nr_paid_search_last_14_days
     , budget_nr_paid_search_qtd
     , budget_seasonality
     , budget_seasonality_last_7_days
     , budget_seasonality_last_14_days
     , budget_seasonality_qtd
     , budget_transactions
     , budget_transactions_last_7_days
     , budget_transactions_last_14_days
     , budget_transactions_qtd
     , budget_unique_customers
     , budget_unique_customers_last_7_days
     , budget_unique_customers_last_14_days
     , budget_unique_customers_qtd
  FROM {{ ref('budget_abacus') }} fb
  FULL OUTER JOIN {{ ref('forecast_abacus') }} ff ON ff.date = fb.date AND ff.planning_type = fb.planning_type
 ORDER BY 2,3,5,1
