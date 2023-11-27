{{ config(schema=var('reports')) }}




WITH date_config AS (
       SELECT
         {% if is_incremental() %}
         -- -15 days here is used to enable calculating last 7 & 14 days with windows
         -- and then in the last part of the query we limit the period to quarter start
         -- since start-date can be set to any value with date trunc to quarter we are
         -- ensuring we always calculate correct QTD values
         DATE(DATE_TRUNC('QUARTER', a.date)) - INTERVAL 15 days AS start_date
         {% endif %}
         , a.end_date AS end_date
         {% if is_incremental() %}
         , DATE(DATE_TRUNC('QUARTER', a.date)) AS quarter_start_date
         {% endif %}
       FROM (SELECT
                {% if is_incremental() %}
                '{{ var ('start-date') }}'
                {% endif %}
                {% if target.name == 'dev' and not is_incremental() %}
                CURRENT_DATE()
                {% endif %}
                {% if target.name != 'dev' and not is_incremental() %}
                '2020-03-31'
                {% endif %}
                AS date
              , {% if is_incremental() %}
                '{{ var ('end-date') }}'
                {% endif %}
                {% if target.name == 'dev' and not is_incremental() %}
                CURRENT_DATE()
                {% endif %}
                {% if target.name != 'dev' and not is_incremental() %}
                '2020-03-31'
                {% endif %}
                AS end_date
       ) a
     ),

dates AS (
    SELECT DISTINCT
        *
        , DATE(DATE_TRUNC('QUARTER', date_id)) AS quarter
        , ARRAY_MIN(ARRAY(DATE(date_id) - INTERVAL 13 days, DATE(DATE_TRUNC('QUARTER', date_id)))) AS join_start_date
        , DATE(date_id) - INTERVAL 13 days AS start_14_days
        , DATE(date_id) - INTERVAL 6 days AS start_7_days
        , DATE(date_id) - INTERVAL 7 days AS end_p7_days
    FROM {{ source('default', 'dim_date') }}
    CROSS JOIN date_config ON date_id BETWEEN start_date AND end_date
    WHERE 1=1
    {% if target.name == 'dev' and not is_incremental() %}
    LIMIT 1
    {% endif %}
),

 raw_bookings AS (
   SELECT
     date_id as date
     , COUNT(DISTINCT bookings.booking_id) AS bookings
     , COUNT(DISTINCT bookings.shopping_cart_id) AS transactions
     , SUM(bookings.nr) AS nr
     , SUM(bookings.gmv) AS gmv
     , SUM(CASE WHEN bookings.device_id IN (4,5) THEN bookings.nr END) AS nr_app -- android and iPhone
     , SUM(CASE WHEN transactions.purchase_type_id = 3 THEN bookings.nr END) AS nr_str
     , SUM(CASE WHEN tour.category IN ('Attraction Walking Tour', 'City Walking Tour', 'Day Trip', 'City Motorized Tour') AND tour.is_gyg_originals THEN bookings.nr END) AS nr_originals
     , SUM(bookings.reseller_share) AS reseller_share
     , COUNT(DISTINCT CASE WHEN booking_trip.booking_in_trip_number > 1 THEN bookings.booking_id END) AS bookings_trip_repeat
     , SUM(CASE WHEN booking_trip.BOOKING_IN_TRIP_NUMBER > 1 THEN bookings.nr END) AS nr_trip_repeat
   FROM dates
     LEFT JOIN {{ source('dwh', 'fact_booking') }} AS bookings ON dates.date_id = TO_DATE(bookings.date_of_checkout)
     LEFT JOIN {{ source('dwh', 'fact_shopping_cart') }} AS transactions ON bookings.shopping_cart_id = transactions.shopping_cart_id
     LEFT JOIN {{ source('dwh', 'dim_reseller_campaign') }} AS reseller ON bookings.reseller_campaign_id = reseller.reseller_campaign_id
     LEFT JOIN {{ source('dwh', 'dim_tour') }} AS tour ON bookings.tour_id = tour.tour_id
     LEFT JOIN {{ ref('fact_booking_trip') }} AS booking_trip ON bookings.booking_id = booking_trip.booking_id
   WHERE 1=1
   AND bookings.status_id IN (1, 2)
   GROUP BY 1
 ),

agg_customers AS (
   SELECT
     date_id as date
     , COUNT(DISTINCT CASE WHEN TO_DATE(bookings.date_of_checkout) = date_id THEN customer_id END) AS customers
     , COUNT(DISTINCT CASE WHEN TO_DATE(bookings.date_of_checkout) BETWEEN start_7_days  AND date_id     THEN customer_id END) AS customers_last_7_days
     , COUNT(DISTINCT CASE WHEN TO_DATE(bookings.date_of_checkout) BETWEEN start_14_days AND date_id     THEN customer_id END) AS customers_last_14_days
     , COUNT(DISTINCT CASE WHEN TO_DATE(bookings.date_of_checkout) BETWEEN start_14_days AND end_p7_days THEN customer_id END) AS customers_prev_7_days
     , COUNT(DISTINCT CASE WHEN TO_DATE(bookings.date_of_checkout) BETWEEN DATE(DATE_TRUNC('QUARTER', date_id)) AND date_id THEN customer_id ELSE NULL END) AS customers_qtd

   FROM {{ source('dwh', 'fact_booking') }} AS bookings
   CROSS JOIN dates ON TO_DATE(bookings.date_of_checkout) BETWEEN join_start_date and end_date
   WHERE bookings.status_id IN (1, 2)
   GROUP BY 1
 ),

 bookings AS (
 SELECT
     DATE(bookings.date) AS date
   , bookings
   , transactions
   , bookings_trip_repeat
   , nr_trip_repeat
   , CAST(nr AS DOUBLE) AS nr
   , CAST(gmv AS DOUBLE) AS gmv
   , CAST(nr_str AS DOUBLE) AS nr_str
   , CAST(nr_app AS DOUBLE) AS nr_app
   , CAST(nr_originals AS DOUBLE) AS nr_originals
   , CAST(reseller_share AS DOUBLE) AS reseller_share
 FROM raw_bookings as bookings
 ),

 agg_mkt_attr AS (
   SELECT
     date(contr_marg.checkout_date) as date
     --, SUM(gmv_markov_checkout_date) AS gmv
     --, SUM(nr_markov_checkout_date) AS nr
     , SUM(ad_spend) AS cost
     , SUM(CASE WHEN group.is_roas_relevant THEN ad_spend END) AS cost_for_roas
     , SUM(coalesce(reseller_cost_active,0) + coalesce(reseller_cost_cancelled,0)) AS reseller_cost_by_checkout_date
     , SUM(marketing_coupon_cost) AS coupon_cost_by_checkout_date
     , SUM(CASE WHEN group.group IN ('Performance Media', 'Paid Search') THEN nr END) AS nr_performance_marketing
     , SUM(CASE WHEN group.group IN ('Performance Media', 'Paid Search') THEN COALESCE(ad_spend, 0) + COALESCE(marketing_coupon_cost, 0) + COALESCE(coalesce(reseller_cost_active,0) + coalesce(reseller_cost_cancelled,0), 0) END) AS cost_performance_marketing
     , SUM(CASE WHEN group.group = 'Paid Search' THEN nr END) AS nr_paid_search
     , SUM(CASE WHEN group.group IN ('CRM', 'Direct & Branded Links', 'SEO') THEN nr END) AS nr_non_paid_channels
     , SUM(CASE WHEN group.group = 'Paid Search' THEN COALESCE(ad_spend, 0) + COALESCE(marketing_coupon_cost, 0) + COALESCE(coalesce(reseller_cost_active,0) + coalesce(reseller_cost_cancelled,0), 0) END) AS cost_paid_search
     -- this channel group is no longer valid, so we can remove it - Robert 2021-11-11
     --, SUM(CASE WHEN mkt_attr.group = 'display & paid_social' THEN nr_markov_checkout_date END) AS nr_display_paid_social
     --, SUM(CASE WHEN mkt_attr.group = 'display & paid_social' THEN COALESCE(cost, 0) + COALESCE(coupon_cost_by_checkout_date, 0) + COALESCE(reseller_cost_by_checkout_date, 0) END) AS cost_display_paid_social
   FROM {{ ref('agg_contribution_margin') }} AS contr_marg
   LEFT JOIN {{ source('dwh', 'dim_attribution_channel_group') }} AS group ON group.channel = contr_marg.channel
   JOIN dates ON dates.date_id = date(contr_marg.checkout_date)
   GROUP BY 1
 ),

cm_base AS (
  SELECT date_id AS date
       , COALESCE(SUM(cm.NR_MARKETPLACE_ACTIVE ), 0) - COALESCE(SUM(cm.NR_MARKETPLACE_FORECASTED_CANCELLATION ), 0) 
          - (COALESCE(SUM(cm.FX_PREMIUM_MARKETPLACE ), 0) - COALESCE(SUM(cm.FX_PREMIUM_MARKETPLACE_FORECASTED_CANCELLATION ), 0)) 
          + (COALESCE(SUM(cm.NR_TT_ACTIVE ), 0) - COALESCE(SUM(cm.NR_TT_FORECASTED_CANCELLATION ), 0) - (COALESCE(SUM(cm.FX_PREMIUM_TT ), 0) - COALESCE(SUM(cm.FX_PREMIUM_TT_FORECASTED_CANCELLATION ), 0)))
          + (COALESCE(SUM(-cm.COUPON_COST_ACTIVE ), 0) - COALESCE(SUM(-cm.COUPON_COST_FORECASTED_CANCELLATION ), 0)) + (COALESCE(SUM(-cm.VAT_TT ), 0) - COALESCE(SUM(-cm.VAT_TT_FORECASTED_CANCELLATION ), 0))
          + (COALESCE(SUM(cm.VAT_ADJUSTMENT_TO_TOMS ), 0) - COALESCE(SUM(cm.VAT_ADJUSTMENT_TO_TOMS_FORECASTED_CANCELLATION ), 0)) 
          + COALESCE(SUM(cm.supplier_cancellation_fee ), 0) 
          - 0.00209 * (COALESCE(SUM(cm.NR_MARKETPLACE_ACTIVE ), 0) - COALESCE(SUM(cm.NR_MARKETPLACE_FORECASTED_CANCELLATION ), 0) - (COALESCE(SUM(cm.FX_PREMIUM_MARKETPLACE ), 0) - COALESCE(SUM(cm.FX_PREMIUM_MARKETPLACE_FORECASTED_CANCELLATION ), 0)) + (COALESCE(SUM(cm.NR_TT_ACTIVE ), 0) - COALESCE(SUM(cm.NR_TT_FORECASTED_CANCELLATION ), 0) - (COALESCE(SUM(cm.FX_PREMIUM_TT ), 0) - COALESCE(SUM(cm.FX_PREMIUM_TT_FORECASTED_CANCELLATION ), 0))))
          + (COALESCE(SUM(cm.FX_PREMIUM_MARKETPLACE ), 0) + COALESCE(SUM(cm.FX_PREMIUM_TT ), 0) - (COALESCE(SUM(cm.FX_PREMIUM_MARKETPLACE_FORECASTED_CANCELLATION ), 0) + COALESCE(SUM(cm.FX_PREMIUM_TT_FORECASTED_CANCELLATION ), 0))) 
          + COALESCE(SUM( 0 ), 0) - COALESCE(SUM(cm.rr_total ), 0) AS nr_components
      --
      , COALESCE(SUM(-cm.PAYMENT_COST ), 0) AS payment_costs_components
      --
      , COALESCE(SUM(-cm.RESELLER_COST_ACTIVE ), 0) - COALESCE(SUM(-cm.RESELLER_COST_FORECASTED_CANCELLATION ), 0) 
        - COALESCE(SUM(cm.gwc_total ), 0) + COALESCE(SUM(CASE WHEN  channel_group.is_roas_relevant   THEN -cm.AD_SPEND END ), 0) 
        + COALESCE(SUM(CASE WHEN checkout_date <= ad_fix_up.ad_fix_date_max
                            THEN -cm.FIXED_AD_SPEND
                            ELSE -cm.FIXED_AD_SPEND_BUDGET
                       END ), 0) AS ad_spend_components
      --
      , COALESCE(SUM(CASE WHEN checkout_date <= ad_fix_up.ad_fix_date_max
                            THEN -cm.FIXED_AD_SPEND
                            ELSE -cm.FIXED_AD_SPEND_BUDGET
                       END ), 0) AS fixed_ad_spend
      --
      , COALESCE(SUM(-cm.TICKET_COST_TT ), 0) - COALESCE(SUM(-cm.TICKET_COST_TT_FORECASTED_CANCELLATION ), 0) AS ticket_cost_tt_adjusted
      , COALESCE(SUM(CASE WHEN checkout_date <= brkg_up.brkg_up_date_max
                          THEN -cm.BREAKAGE
                          ELSE -cm.BREAKAGE_BUDGET
                      END), 0) AS breakage
  -- {nr_components} + {payment_costs_components} + {ad_spend_components} + {ticket_cost_tt_adjusted} + {breakage}
    FROM dates  
    LEFT JOIN {{ ref('agg_contribution_margin') }} cm ON dates.date_id = date(cm.actual_date_checkout)
    LEFT JOIN {{ source('dwh', 'dim_attribution_channel_group') }} AS channel_group ON (cm.CHANNEL) = channel_group.channel
   CROSS JOIN (SELECT date_add(add_months(date_trunc('month', MAX(month)), 1), -1) AS ad_fix_date_max FROM {{ source('default', 'contribution_margin_fixed_adspend_upload') }}) AS ad_fix_up
   CROSS JOIN (SELECT date_add(add_months(date_trunc('month', MAX(date)), 1), -1) AS brkg_up_date_max FROM {{ source('default', 'cost_allocation_breakage_upload') }}) AS brkg_up
   GROUP BY 1
),

cm AS (
  SELECT date
       , nr_components + payment_costs_components + ad_spend_components + ticket_cost_tt_adjusted + breakage AS contribution_margin_checkout
       , nr_components
       , fixed_ad_spend
    FROM cm_base
),

summary AS (
   SELECT
     bookings.date
     , bookings
     , SUM(bookings) OVER w_last_7_days AS bookings_last_7_days
     , SUM(bookings) OVER w_last_14_days AS bookings_last_14_days
     , SUM(bookings) OVER w_qtd AS bookings_qtd
     , bookings_trip_repeat
     , SUM(bookings_trip_repeat) OVER w_last_7_days AS bookings_trip_repeat_last_7_days
     , SUM(bookings_trip_repeat) OVER w_last_14_days AS bookings_trip_repeat_last_14_days
     , SUM(bookings_trip_repeat) OVER w_qtd AS bookings_trip_repeat_qtd
     , CAST(nr_trip_repeat AS DOUBLE) AS nr_trip_repeat
     , CAST(SUM(nr_trip_repeat) OVER w_last_7_days   AS DOUBLE) AS nr_trip_repeat_last_7_days
     , CAST(SUM(nr_trip_repeat) OVER w_last_14_days  AS DOUBLE) AS nr_trip_repeat_last_14_days
     , CAST(SUM(nr_trip_repeat) OVER w_qtd           AS DOUBLE) AS nr_trip_repeat_qtd
     , bookings.nr
     , SUM(bookings.nr) OVER w_last_7_days AS nr_last_7_days
     , SUM(bookings.nr) OVER w_last_14_days AS nr_last_14_days
     , SUM(bookings.nr) OVER w_qtd AS nr_qtd
     , agg_mkt_attr.nr_paid_search
     , SUM(agg_mkt_attr.nr_paid_search) OVER w_last_7_days AS nr_paid_search_last_7_days
     , SUM(agg_mkt_attr.nr_paid_search) OVER w_last_14_days AS nr_paid_search_last_14_days
     , SUM(agg_mkt_attr.nr_paid_search) OVER w_qtd AS nr_paid_search_qtd
     , agg_mkt_attr.nr_non_paid_channels
     , SUM(agg_mkt_attr.nr_non_paid_channels) OVER w_last_7_days  AS nr_non_paid_channels_last_7_days
     , SUM(agg_mkt_attr.nr_non_paid_channels) OVER w_last_14_days AS nr_non_paid_channels_last_14_days
     , SUM(agg_mkt_attr.nr_non_paid_channels) OVER w_qtd          AS nr_non_paid_channels_qtd
     , agg_mkt_attr.nr_performance_marketing
     , SUM(agg_mkt_attr.nr_performance_marketing) OVER w_last_7_days AS nr_performance_marketing_last_7_days
     , SUM(agg_mkt_attr.nr_performance_marketing) OVER w_last_14_days AS nr_performance_marketing_last_14_days
     , SUM(agg_mkt_attr.nr_performance_marketing) OVER w_qtd AS nr_performance_marketing_qtd
     , bookings.nr_str
     , SUM(bookings.nr_str) OVER w_last_7_days AS nr_str_last_7_days
     , SUM(bookings.nr_str) OVER w_last_14_days AS nr_str_last_14_days
     , SUM(bookings.nr_str) OVER w_qtd AS nr_str_qtd
     , bookings.nr_app
     , SUM(bookings.nr_app) OVER w_last_7_days  AS nr_app_last_7_days
     , SUM(bookings.nr_app) OVER w_last_14_days AS nr_app_last_14_days
     , SUM(bookings.nr_app) OVER w_qtd          AS nr_app_qtd     
     , bookings.gmv
     , SUM(bookings.gmv) OVER w_last_7_days AS gmv_last_7_days
     , SUM(bookings.gmv) OVER w_last_14_days AS gmv_last_14_days
     , SUM(bookings.gmv) OVER w_qtd AS gmv_qtd
     , (bookings.nr / (agg_mkt_attr.cost_for_roas + agg_mkt_attr.reseller_cost_by_checkout_date + agg_mkt_attr.coupon_cost_by_checkout_date)) as roas_real_time
     , SUM(bookings.nr) OVER w_last_7_days / SUM(agg_mkt_attr.cost_for_roas + agg_mkt_attr.reseller_cost_by_checkout_date + agg_mkt_attr.coupon_cost_by_checkout_date) OVER w_last_7_days AS roas_real_time_last_7_days
     , SUM(bookings.nr) OVER w_last_14_days / SUM(agg_mkt_attr.cost_for_roas + agg_mkt_attr.reseller_cost_by_checkout_date + agg_mkt_attr.coupon_cost_by_checkout_date) OVER w_last_14_days AS roas_real_time_last_14_days
     , SUM(bookings.nr) OVER w_qtd / SUM(agg_mkt_attr.cost_for_roas + agg_mkt_attr.reseller_cost_by_checkout_date + agg_mkt_attr.coupon_cost_by_checkout_date) OVER w_qtd AS roas_real_time_qtd
     , (agg_mkt_attr.nr_performance_marketing / agg_mkt_attr.cost_performance_marketing) AS roas_performance_marketing
     , SUM(agg_mkt_attr.nr_performance_marketing) OVER w_last_7_days / SUM(agg_mkt_attr.cost_performance_marketing) OVER w_last_7_days AS roas_performance_marketing_last_7_days
     , SUM(agg_mkt_attr.nr_performance_marketing) OVER w_last_14_days / SUM(agg_mkt_attr.cost_performance_marketing) OVER w_last_14_days AS roas_performance_marketing_last_14_days
     , SUM(agg_mkt_attr.nr_performance_marketing) OVER w_qtd / SUM(agg_mkt_attr.cost_performance_marketing) OVER w_qtd AS roas_performance_marketing_qtd
     , (agg_mkt_attr.nr_paid_search / agg_mkt_attr.cost_paid_search) AS roas_paid_search
     , SUM(agg_mkt_attr.nr_paid_search) OVER w_last_7_days / SUM(agg_mkt_attr.cost_paid_search) OVER w_last_7_days AS roas_paid_search_last_7_days
     , SUM(agg_mkt_attr.nr_paid_search) OVER w_last_14_days / SUM(agg_mkt_attr.cost_paid_search) OVER w_last_14_days AS roas_paid_search_last_14_days
     , SUM(agg_mkt_attr.nr_paid_search) OVER w_qtd / SUM(agg_mkt_attr.cost_paid_search) OVER w_qtd AS roas_paid_search_qtd
     , transactions
     , SUM(transactions) OVER w_last_7_days AS transactions_last_7_days
     , SUM(transactions) OVER w_last_14_days AS transactions_last_14_days
     , SUM(transactions) OVER w_qtd AS transactions_qtd
     , (nr_originals / nr) AS guided_tours_originals_nr_share
     , AVG(nr_originals / nr) OVER w_last_7_days AS guided_tours_originals_nr_share_last_7_days
     , AVG(nr_originals / nr) OVER w_last_14_days AS guided_tours_originals_nr_share_last_14_days
     , AVG(nr_originals / nr) OVER w_qtd AS guided_tours_originals_nr_share_qtd
     , CAST(null AS DOUBLE) AS guided_tours_originals_experience_share
     , AVG(CAST(null AS DOUBLE)) OVER w_last_7_days AS guided_tours_originals_experience_share_last_7_days
     , AVG(CAST(null AS DOUBLE)) OVER w_last_14_days AS guided_tours_originals_experience_share_last_14_days
     , AVG(CAST(null AS DOUBLE)) OVER w_qtd AS guided_tours_originals_experience_share_qtd
     , cm.contribution_margin_checkout
     , SUM(cm.contribution_margin_checkout) OVER w_last_7_days  AS contribution_margin_checkout_last_7_days
     , SUM(cm.contribution_margin_checkout) OVER w_last_14_days AS contribution_margin_checkout_last_14_days
     , SUM(cm.contribution_margin_checkout) OVER w_qtd          AS contribution_margin_checkout_qtd
     , cm.nr_components
     , SUM(cm.nr_components) OVER w_last_7_days  AS nr_components_last_7_days
     , SUM(cm.nr_components) OVER w_last_14_days AS nr_components_last_14_days
     , SUM(cm.nr_components) OVER w_qtd          AS nr_components_qtd 
     , cm.fixed_ad_spend
     , SUM(cm.fixed_ad_spend) OVER w_last_7_days  AS fixed_ad_spend_last_7_days
     , SUM(cm.fixed_ad_spend) OVER w_last_14_days AS fixed_ad_spend_last_14_days
     , SUM(cm.fixed_ad_spend) OVER w_qtd          AS fixed_ad_spend_qtd 
   FROM bookings
   LEFT JOIN agg_mkt_attr ON agg_mkt_attr.date = bookings.date
   LEFT JOIN cm ON cm.date = bookings.date
   LEFT JOIN dates ON bookings.date = dates.date_id

   WINDOW w_last_7_days AS (ORDER BY bookings.date RANGE BETWEEN 6 PRECEDING AND CURRENT ROW)
   , w_last_14_days AS (ORDER BY bookings.date RANGE BETWEEN 13 PRECEDING AND CURRENT ROW)
   , w_qtd AS (PARTITION BY quarter ORDER BY bookings.date)
 )

 SELECT
     summary.*
     , ev.visitors
     , ev.visitors_last_7_days
     , ev.visitors_last_14_days
     , ev.visitors_prev_7_days
     , ev.visitors_qtd
     , ev.quoters
     , ev.quoters_last_7_days
     , ev.quoters_last_14_days
     , ev.quoters_prev_7_days
     , ev.quoters_qtd
     , agg_customers.customers
     , agg_customers.customers_last_7_days
     , agg_customers.customers_last_14_days
     , agg_customers.customers_prev_7_days
     , agg_customers.customers_qtd
 FROM summary
 LEFT JOIN {{ ref('agg_metrics_events') }} ev ON ev.date = summary.date
 LEFT JOIN agg_customers ON agg_customers.date = summary.date
 WHERE summary.date >= (SELECT quarter_start_date FROM date_config)
