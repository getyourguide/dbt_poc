{{ config(schema=var('reports')) }}



--
-- All previous transformations are incremental so we are dealing with NULLs here to avoid backfills.
-- The reason to deal with NULLs is to avoid "fan out" in joins due to NULLs
--

SELECT date, country_group_source, borders, channel, is_acquisition
     --
     , SUM(COALESCE(b.bookings         , 0)) AS bookings
     , SUM(COALESCE(b.bookings_qtd     , 0)) AS bookings_qtd
     , SUM(COALESCE(b.daily_customers  , 0)) AS customers
     , SUM(COALESCE(b.gmv              , 0)) AS gmv
     , SUM(COALESCE(b.gmv_qtd          , 0)) AS gmv_qtd
     , SUM(COALESCE(b.nr               , 0)) AS nr
     , SUM(COALESCE(b.nr_qtd           , 0)) AS nr_qtd
     , SUM(COALESCE(b.transactions     , 0)) AS transactions_bk
     , SUM(COALESCE(b.transactions_qtd , 0)) AS transactions_qtd_bk
     --
     , SUM(COALESCE(c.customers_qtd, 0)) AS customers_qtd
     --
     , SUM(COALESCE(m.reseller_costs    , 0)) AS reseller_costs    
     , SUM(COALESCE(m.reseller_costs_qtd, 0)) AS reseller_costs_qtd
     , SUM(COALESCE(m.coupon_costs      , 0)) AS coupon_costs      
     , SUM(COALESCE(m.coupon_costs_qtd  , 0)) AS coupon_costs_qtd  
     , SUM(COALESCE(m.transactions      , 0)) AS transactions      
     , SUM(COALESCE(m.transactions_qtd  , 0)) AS transactions_qtd  
     --
     , SUM(COALESCE(cm.contribution_margin_checkout    , 0)) AS contribution_margin_checkout    
     , SUM(COALESCE(cm.contribution_margin_checkout_qtd, 0)) AS contribution_margin_checkout_qtd
     , SUM(COALESCE(cm.nr_components                   , 0)) AS nr_components                   
     , SUM(COALESCE(cm.nr_components_qtd               , 0)) AS nr_components_qtd               
     , SUM(COALESCE(cm.fixed_ad_spend                  , 0)) AS fixed_ad_spend                  
     , SUM(COALESCE(cm.fixed_ad_spend_qtd              , 0)) AS fixed_ad_spend_qtd              
     , SUM(COALESCE(-cm.realtime_ad_spend              , 0)) AS realtime_ad_spend     -- includes reseller & coupon cost
     , SUM(COALESCE(-cm.realtime_ad_spend_qtd          , 0)) AS realtime_ad_spend_qtd -- includes reseller & coupon cost
     , SUM(COALESCE(-cm.ad_spend                       , 0)) AS ad_spend
     , SUM(COALESCE(-cm.ad_spend_qtd                   , 0)) AS ad_spend_qtd
  FROM {{ ref('agg_bookings_drill_down') }} b
  FULL OUTER JOIN {{ ref('agg_customers_drill_down') }} c USING (date, country_group_source, borders, channel, is_acquisition)
  FULL OUTER JOIN {{ ref('agg_marketing_drill_down') }} m USING (date, country_group_source, borders, channel, is_acquisition)
  FULL OUTER JOIN {{ ref('agg_metrics_contribution_margin_drill_down') }} cm USING (date, country_group_source, borders, channel, is_acquisition)
 GROUP BY 1,2,3,4,5