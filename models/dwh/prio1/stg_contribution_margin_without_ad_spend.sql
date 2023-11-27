{{ config(schema=var('default')) }}



/* Main part of contribution margin data model, all drivers (e.g. goodwill, payment cost, coupon cost) are
included in this script. It's a daily full load process.
*/

-- goodwill cost --
WITH gwc_base AS (
-- add new GWC table
-- FP&A needs CAT on REFUND date with negative sign !!!
SELECT bb.checkout_date
     , bb.date_of_travel_berlin_time
     , bb.date_of_cancelation AS cancellation_date
     , g.gwc_date AS refund_date
     , CAST(NULL AS DATE) AS event_date
     , g.gwc_date AS actual_date_checkout
     , g.gwc_date AS actual_date_travel
     , bb.reseller_id
     , bb.customer_first_checkout_date
     , cw.channel
     , bb.purchase_type_id
     , bb.destination_city_id
     , bb.source_market_country_id
     , bb.tour_id
     , bb.tour_option_id
     , bb.is_rnpl
     , bb.is_inventory_relevant
     , SUM(CAST(COALESCE(cw.channel_weight,1) * g.gwc_free_cancellation      AS DOUBLE)) AS gwc_free_cancellation
     , SUM(CAST(COALESCE(cw.channel_weight,1) * g.gwc_total                  AS DOUBLE)) AS gwc_total
     , SUM(CAST(COALESCE(cw.channel_weight,1) * g.rr_total                   AS DOUBLE)) AS rr_total
     , SUM(CAST(COALESCE(cw.channel_weight,1) * g.cat_total * (-1)           AS DOUBLE)) AS cat_total  -- NEGATIV !!!!!
     , SUM(CAST(COALESCE(cw.channel_weight,1) * g.premium_option_fee_revenue AS DOUBLE)) AS premium_option_fee_revenue
     , SUM(CAST(COALESCE(cw.channel_weight,1) * g.supplier_cancellation_fee  AS DOUBLE)) AS supplier_cancellation_fee
     , SUM(CAST(COALESCE(cw.channel_weight,1) * g.fee_tootal                 AS DOUBLE)) AS fee_tootal

  FROM {{ source('dwh', 'fact_gwc') }} g
 INNER JOIN {{ ref('stg_contribution_margin_booking_base') }} bb ON g.booking_id = bb.booking_id
  LEFT JOIN {{ source('default', 'agg_attribution_channel_weights') }} cw ON g.shopping_cart_id = cw.shopping_cart_id
 GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
)
, gwc_cat AS (
-- add new GWC table
-- FP&A needs CAT on TRAVEL date with positive sign !!!
SELECT bb.checkout_date
     , bb.date_of_travel_berlin_time
     , bb.date_of_cancelation AS cancellation_date
     , g.gwc_date AS refund_date
     , CAST(NULL AS DATE) AS event_date
     , bb.date_of_travel_berlin_time AS actual_date_checkout
     , bb.date_of_travel_berlin_time AS actual_date_travel
     , bb.reseller_id
     , bb.customer_first_checkout_date
     , cw.channel
     , bb.purchase_type_id
     , bb.destination_city_id
     , bb.source_market_country_id
     , bb.tour_id
     , bb.tour_option_id
     , bb.is_rnpl
     , bb.is_inventory_relevant
     , (CAST(0                                               AS DOUBLE)) AS gwc_free_cancellation
     , (CAST(0                                               AS DOUBLE)) AS gwc_total
     , (CAST(0                                               AS DOUBLE)) AS rr_total
     , SUM(CAST(COALESCE(cw.channel_weight,1) * g.cat_total  AS DOUBLE)) AS cat_total -- POSITIVE !!!
     , (CAST(0                                               AS DOUBLE)) AS premium_option_fee_revenue
     , (CAST(0                                               AS DOUBLE)) AS supplier_cancellation_fee
     , (CAST(0                                               AS DOUBLE)) AS fee_tootal

  FROM {{ source('dwh', 'fact_gwc') }} g
 INNER JOIN {{ ref('stg_contribution_margin_booking_base') }} bb ON g.booking_id = bb.booking_id
  LEFT JOIN {{ source('default', 'agg_attribution_channel_weights') }} cw ON g.shopping_cart_id = cw.shopping_cart_id
 GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
)

, gwc AS (
-- Union the 2 above to get the correct CAT
SELECT actual_date_checkout AS checkout_date
     , actual_date_travel AS date_of_travel_berlin_time
     , cancellation_date
     , refund_date
     , event_date
     , actual_date_checkout
     , actual_date_travel
     , reseller_id
     , customer_first_checkout_date
     , channel
     , purchase_type_id
     , destination_city_id
     , source_market_country_id
     , tour_id
     , tour_option_id
     , is_rnpl
     , is_inventory_relevant
     , SUM(gwc_free_cancellation     ) AS gwc_free_cancellation
     , SUM(gwc_total                 ) AS gwc_total
     , SUM(rr_total                  ) AS rr_total
     , SUM(cat_total                 ) AS cat_total
     , SUM(premium_option_fee_revenue) AS premium_option_fee_revenue
     , SUM(supplier_cancellation_fee ) AS supplier_cancellation_fee
     , SUM(fee_tootal                ) AS fee_tootal
  FROM (
    SELECT checkout_date
         , date_of_travel_berlin_time
         , cancellation_date
         , refund_date
         , event_date
         , actual_date_checkout
         , actual_date_travel
         , reseller_id
         , customer_first_checkout_date
         , channel
         , purchase_type_id
         , destination_city_id
         , source_market_country_id
         , tour_id
         , tour_option_id
         , is_rnpl
         , is_inventory_relevant
         , gwc_free_cancellation
         , gwc_total
         , rr_total
         , cat_total
         , premium_option_fee_revenue
         , supplier_cancellation_fee
         , fee_tootal
      FROM gwc_base
     UNION ALL
    SELECT checkout_date
         , date_of_travel_berlin_time
         , cancellation_date
         , refund_date
         , event_date
         , actual_date_checkout
         , actual_date_travel
         , reseller_id
         , customer_first_checkout_date
         , channel
         , purchase_type_id
         , destination_city_id
         , source_market_country_id
         , tour_id
         , tour_option_id
         , is_rnpl
         , is_inventory_relevant
         , gwc_free_cancellation
         , gwc_total
         , rr_total
         , cat_total
         , premium_option_fee_revenue
         , supplier_cancellation_fee
         , fee_tootal
      FROM gwc_cat
  )
 GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
)
,
reseller_channel AS (
SELECT re.reseller_id
        , CASE WHEN  re.channel = 'non_partner_channel' THEN re.channel
               ELSE COALESCE(p.channel, 'affiliate')
           END AS channel
        , re.team
FROM {{ source('dwh', 'dim_reseller') }} re
LEFT JOIN dwh.dim_reseller_mapping p ON re.reseller_id = p.reseller_id
WHERE re.channel <> 'non_partner_channel'
)

---- Supplier and Reseller adjustment payments ----

, adjustment_payments AS (
-- add new GWC table
-- FP&A needs CAT on REFUND date with negative sign !!!
SELECT bb.checkout_date
     , bb.date_of_travel_berlin_time
     , bb.date_of_cancelation AS cancellation_date
     , g.date AS payment_date
     , CAST(NULL AS DATE) AS refund_date
     , CAST(NULL AS DATE) AS event_date
     , bb.date_of_travel_berlin_time AS actual_date_checkout
     , bb.date_of_travel_berlin_time AS actual_date_travel
     , bb.reseller_id
     , bb.customer_first_checkout_date
     , cw.channel
     , bb.purchase_type_id
     , bb.destination_city_id
     , bb.source_market_country_id
     , bb.tour_id
     , bb.tour_option_id
     , bb.is_rnpl
     , bb.is_inventory_relevant
     
     , SUM(CAST(COALESCE(cw.channel_weight,1) * g.cancelation_compensation_amount_eur       AS DOUBLE)) AS cancelation_compensation_amount_eur
     , SUM(CAST(COALESCE(cw.channel_weight,1) * g.manual_cancellation_adjustment_amount_eur AS DOUBLE)) AS manual_cancellation_adjustment_amount_eur
     , SUM(CAST(COALESCE(cw.channel_weight,1) * g.manual_commission_adjustment_amount_eur   AS DOUBLE)) AS manual_commission_adjustment_amount_eur

  FROM {{ source('dwh', 'fact_supplier_adjustment_payment') }} g
 INNER JOIN {{ ref('stg_contribution_margin_booking_base') }} bb ON g.booking_id = bb.booking_id
  LEFT JOIN {{ source('default', 'agg_attribution_channel_weights') }} cw ON bb.shopping_cart_id = cw.shopping_cart_id
 GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18
)

-- coupon cost --
,
contribution_margin_coupon_cost AS (
SELECT bbase.checkout_date
     , bbase.date_of_travel_berlin_time
     , bbase.date_of_cancelation AS cancellation_date
     , CAST(NULL AS DATE) AS refund_date
     , CAST(NULL AS DATE) AS event_date
     , bbase.checkout_date AS actual_date_checkout
     , bbase.date_of_travel_berlin_time AS actual_date_travel
     , tr.reseller_id
     , tr.channel
     , bbase.purchase_type_id
     , bbase.destination_city_id
     , bbase.source_market_country_id
     , bbase.tour_id
     , bbase.tour_option_id
     , bbase.customer_first_checkout_date -- newly added
     , 'coupon_cost' AS cost_type
     , bbase.is_rnpl
     , bbase.is_inventory_relevant
     , SUM(tr.marketing_coupon_cost) AS marketing_coupon_cost
     , SUM(tr.non_marketing_coupon_cost) AS non_marketing_coupon_cost
     , SUM(CASE WHEN bbase.status_id = 1 THEN COALESCE(marketing_coupon_cost, 0) + COALESCE(non_marketing_coupon_cost, 0) END) AS coupon_cost_active -- new metric added 2023-03-30
     , SUM(CASE WHEN bbase.status_id = 1 THEN (COALESCE(marketing_coupon_cost, 0) + COALESCE(non_marketing_coupon_cost, 0)) * bbase.forcasted_cancellation_rate END) AS coupon_cost_forecasted_cancellation -- new metric added 2023-03-30
  FROM {{ source('marketing', 'fact_booking_marketing') }} tr
  LEFT JOIN {{ ref('stg_contribution_margin_booking_base') }} bbase ON tr.booking_id = bbase.booking_id
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
 )

-- payment cost --
, actual_payment_cost AS (
  SELECT DATE(sp.date_of_checkout) AS checkout_date
       , bo.date_of_travel_berlin_time
       , CAST(NULL AS DATE) AS cancellation_date
       , CAST(NULL AS DATE) AS refund_date
       , CAST(NULL AS DATE) AS event_date
       , DATE(sp.date_of_checkout) AS actual_date_checkout
       , DATE(bo.date_of_travel_berlin_time) AS actual_date_travel
       , cw.channel
       , bo.purchase_type_id
       , bo.destination_city_id
       , bo.source_market_country_id
       , bo.tour_id
       , bo.tour_option_id
       , bo.customer_first_checkout_date --newly added
       , cw.reseller_id --newly added
       , 'payment_cost' AS cost_type
       , COALESCE(sp.is_rnpl, bo.is_rnpl) AS is_rnpl
       , bo.is_inventory_relevant
       , SUM(cost_of_payment.debit_amount * COALESCE(bo.booking_weight, 1) * COALESCE(cw.channel_weight, 1)) AS cost_of_payment
       , SUM(bo.gmv) AS gmv
    FROM {{ source('dwh', 'fact_shopping_cart') }}  AS sp
    LEFT JOIN {{ ref('stg_contribution_margin_booking_base') }} bo ON sp.shopping_cart_id = bo.shopping_cart_id
    LEFT JOIN {{ source('default', 'agg_attribution_channel_weights') }} cw ON bo.shopping_cart_id = cw.shopping_cart_id
    LEFT JOIN {{ source('dwh', 'fact_accounting_transaction') }}  AS cost_of_payment ON cost_of_payment.accounting_transaction_reference_tech_code_id = 7 AND cost_of_payment.accounting_transaction_label_id = 18 AND cost_of_payment.reference_tech_id = sp.billing_id
   WHERE sp.shopping_cart_status_id IN (1, 5, 6) AND  sp.date_of_checkout >= '2017-01-01'
   GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
)

, daily_source_market_payment_cost AS (
  SELECT checkout_date
       , source_market_country_id
       , COALESCE(SUM(cost_of_payment), 0) AS country_payment_cost
       , SUM(gmv) AS gmv
       , COALESCE(SUM(cost_of_payment), 0) / NULLIF(SUM(gmv), 0) AS country_payment_cost_rate
    FROM actual_payment_cost
   WHERE checkout_date BETWEEN date_sub(CURRENT_DATE, 10) AND date_sub(CURRENT_DATE, 1)
   GROUP BY 1,2
)

, estimated_payment_cost AS (
  SELECT source_market_country_id
       , AVG(country_payment_cost) AS avg_payment_cost
       , AVG(country_payment_cost / NULLIF(gmv,0)) AS avg_payment_cost_rate
    FROM daily_source_market_payment_cost
   WHERE checkout_date BETWEEN date_sub(CURRENT_DATE, 10) AND date_sub(CURRENT_DATE, 6)
   GROUP BY 1
)
, estimated_missing_payment_cost AS (
  SELECT a.checkout_date AS checkout_date
       , a.source_market_country_id AS source_market_country_id
       , CASE WHEN a.country_payment_cost_rate < 0.75 * avg_payment_cost_rate THEN 'yes' ELSE 'no' END AS is_missing_cost
       , CASE WHEN a.country_payment_cost_rate < 0.75 * avg_payment_cost_rate THEN b.avg_payment_cost_rate * a.gmv --b.avg_payment_cost
              ELSE a.country_payment_cost
         END AS estimated_payment_cost
    FROM daily_source_market_payment_cost a
    LEFT JOIN estimated_payment_cost b ON a.source_market_country_id = b.source_market_country_id
   WHERE a.checkout_date BETWEEN date_sub(CURRENT_DATE, 6) AND date_sub(CURRENT_DATE, 1)
)
, contribution_margin_payment_cost AS (
SELECT ac.checkout_date
     , ac.date_of_travel_berlin_time
     , ac.cancellation_date
     , ac.refund_date
     , ac.event_date
     , ac.actual_date_checkout
     , ac.actual_date_travel
     , ac.channel
     , ac.purchase_type_id
     , ac.destination_city_id
     , ac.source_market_country_id
     , ac.tour_id
     , ac.tour_option_id
     , ac.customer_first_checkout_date --newly added
     , ac.reseller_id
     , ac.cost_type
     , ac.is_rnpl
     , ac.is_inventory_relevant
     , cost_of_payment
  FROM actual_payment_cost ac
  LEFT JOIN estimated_missing_payment_cost mi
         ON ac.source_market_country_id = mi.source_market_country_id
        AND ac.checkout_date = mi.checkout_date
WHERE mi.is_missing_cost = 'no'
   OR mi.is_missing_cost IS NULL
UNION ALL
/*Union because we don't allocate to other dimensions atm, also to avoide overcounting by using join.*/
SELECT checkout_date
     , CAST(NULL AS DATE) AS date_of_travel_berlin_time
     , CAST(NULL AS DATE) AS cancellation_date
     , CAST(NULL AS DATE) AS refund_date
     , CAST(NULL AS DATE) AS event_date
     , checkout_date AS actual_date_checkout
     , checkout_date AS actual_date_travel
     , CAST(NULL AS string) AS channel
     , CAST(NULL AS BIGINT) AS purchase_type_id
     , CAST(NULL AS BIGINT) AS asdestination_city_id
     , source_market_country_id
     , CAST(NULL AS BIGINT) AS tour_id
     , CAST(NULL AS BIGINT) AS tour_option_id
     , CAST(NULL AS DATE) AS customer_first_checkout_date
     , CAST(NULL AS BIGINT) AS reseller_id
     , 'payment_cost' AS cost_type
     , CAST(NULL AS BOOLEAN) AS is_rnpl
     , CAST(NULL AS BOOLEAN) AS is_inventory_relevant
     , estimated_payment_cost AS cost_of_payment
  FROM estimated_missing_payment_cost
 WHERE is_missing_cost = 'yes'
)


-- other drivers --
, booking_with_channel_base AS (
 SELECT booking_base.checkout_date
      , booking_base.date_of_travel_berlin_time
      , booking_base.travel_date
      , booking_base.days_to_conduction
      , booking_base.customer_id
      , booking_base.customer_first_checkout_date
      , booking_base.booking_id
      , booking_base.shopping_cart_id
      , cw.reseller_id
      , cw.channel
      , booking_base.reseller_channel
      , booking_base.status_id
      , booking_base.source_market_country_id
      , booking_base.source_market_country_name
      , booking_base.destination_location_id
      , booking_base.destination_city_id
      , booking_base.destination_city_name
      , booking_base.purchase_type
      , booking_base.tour_id
      , booking_base.tour_option_id
      , booking_base.is_supplier_force_majeure
      , booking_base.purchase_type_id
      , booking_base.is_inventory_relevant
      , booking_base.sales_area
      , booking_base.booking_weight
      , booking_base.is_cancelled_after_travel
      , booking_base.date_of_cancelation
      , booking_base.cancellation_category
      , booking_base.cancellation_sub_category
      , 1 * COALESCE(cw.channel_weight, 1) / daily_customer_bookings AS daily_customers -- for getting unique customers on daily level
      , 1 * COALESCE(cw.channel_weight, 1) / monthly_customer_bookings AS monthly_customers -- for getting unique customers on monthly level
      , 1 * COALESCE(cw.channel_weight, 1) / quarterly_customer_bookings AS quarterly_customers -- for getting unique customers on quarterly level
      , 1 * COALESCE(cw.channel_weight, 1) / yearly_customer_bookings AS yearly_customers -- for getting unique customers on yearly level
      , 1 * COALESCE(cw.channel_weight, 1)  AS bookings --newly added
      , 1 * COALESCE(cw.channel_weight, 1) / booking_base.shopping_cart_bookings  AS transactions --newly added
      , booking_base.nr * COALESCE(cw.channel_weight, 1)  AS nr
      , booking_base.gmv * COALESCE(cw.channel_weight, 1)  AS gmv
      , booking_base.gmv_supplier * COALESCE(cw.channel_weight, 1)  AS gmv_supplier
      , booking_base.total_tax * COALESCE(cw.channel_weight, 1) AS total_tax
      , booking_base.selling_tax_eur * COALESCE(cw.channel_weight, 1) AS selling_tax_eur
      , booking_base.net_tax_eur * COALESCE(cw.channel_weight, 1) AS net_tax_eur
      , booking_base.forcasted_cancellation_rate
      , booking_base.is_rnpl
      , CASE WHEN booking_base.sales_area = 'AMERICAS' AND booking_base.date_of_travel_berlin_time < '2021-01-01' THEN FALSE WHEN booking_base.status_id = 1 AND booking_base.is_inventory_relevant THEN TRUE END AS is_calculate_tax
      

   FROM  {{ ref('stg_contribution_margin_booking_base') }} booking_base
   LEFT JOIN default.agg_attribution_channel_weights  cw ON booking_base.shopping_cart_id = cw.shopping_cart_id
)

, booking_with_channel AS (
 SELECT *
      -- exclude VAT for Sales Area Americas for Travel dates before 01Jan2021 but include therafter
      , (CASE is_calculate_tax WHEN FALSE THEN 0 WHEN TRUE THEN booking_base.total_tax END)                                                  AS vat_tt
      , (CASE is_calculate_tax WHEN FALSE THEN 0 WHEN TRUE THEN booking_base.total_tax * booking_base.forcasted_cancellation_rate END)       AS vat_tt_forecasted_cancellation
      , (CASE is_calculate_tax WHEN FALSE THEN 0 WHEN TRUE THEN booking_base.total_tax END)*0.07                                             AS vat_adjustment_to_toms
      , (CASE is_calculate_tax WHEN FALSE THEN 0 WHEN TRUE THEN booking_base.total_tax * booking_base.forcasted_cancellation_rate END)*0.07  AS vat_adjustment_to_toms_forecasted_cancellation
      , (CASE is_calculate_tax WHEN FALSE THEN 0 WHEN TRUE THEN booking_base.selling_tax_eur END)                                            AS selling_tax_tt
      , (CASE is_calculate_tax WHEN FALSE THEN 0 WHEN TRUE THEN booking_base.selling_tax_eur * booking_base.forcasted_cancellation_rate END) AS selling_tax_tt_forecasted_cancellation
      , (CASE is_calculate_tax WHEN FALSE THEN 0 WHEN TRUE THEN booking_base.selling_tax_eur END)*0.07                                       AS selling_tax_adjustment_to_toms
      , (CASE is_calculate_tax WHEN FALSE THEN 0 WHEN TRUE THEN booking_base.selling_tax_eur * booking_base.forcasted_cancellation_rate END)*0.07 AS selling_tax_adjustment_to_toms_forecasted_cancellation
      , (CASE is_calculate_tax WHEN FALSE THEN 0 WHEN TRUE THEN booking_base.net_tax_eur END)                                                 AS net_tax_tt
      , (CASE is_calculate_tax WHEN FALSE THEN 0 WHEN TRUE THEN booking_base.net_tax_eur * booking_base.forcasted_cancellation_rate END)      AS net_tax_tt_forecasted_cancellation
      , (CASE is_calculate_tax WHEN FALSE THEN 0 WHEN TRUE THEN booking_base.net_tax_eur END)*0.07                                            AS net_tax_adjustment_to_toms
      , (CASE is_calculate_tax WHEN FALSE THEN 0 WHEN TRUE THEN booking_base.net_tax_eur * booking_base.forcasted_cancellation_rate END)*0.07 AS net_tax_adjustment_to_toms_forecasted_cancellation
   FROM booking_with_channel_base booking_base
)

, budget_goodwill AS (
SELECT DATE
     , SUM(consumer_goodwill) / SUM(nr) AS goodwill_percentage
  FROM {{ source('dwh', 'fact_forecast') }}
 WHERE budget_config_id = (SELECT max(budget_config_id) FROM dwh.fact_forecast)
 GROUP BY 1
)
, contribution_margin_other_costs AS (
SELECT bo.checkout_date
     , bo.date_of_travel_berlin_time
     , bo.date_of_cancelation AS cancellation_date
     , CAST(NULL AS DATE) AS refund_date
     , CAST(NULL AS DATE) AS event_date
     , bo.checkout_date AS actual_date_checkout
     , bo.date_of_travel_berlin_time AS actual_date_travel
     , bo.reseller_id
     , bo.customer_first_checkout_date --newly added
     , bo.channel
     , bo.purchase_type_id
     , bo.destination_city_id
     , bo.source_market_country_id
     , bo.tour_id
     , bo.tour_option_id
     , 'others' AS cost_type
     , bo.is_rnpl
     , bo.is_inventory_relevant
     , SUM(bo.nr) AS nr
     , SUM(bo.gmv) as gmv
     , SUM(case when bo.status_id = 2 THEN bo.gmv END) as gmv_cancelled
     , SUM(case when bo.status_id = 2 THEN bo.nr END) as nr_cancelled
     , SUM(CASE WHEN bo.status_id = 2 AND (cancellation_category IN ('Supplier Self Cancelation') OR cancellation_sub_category IN ('Supplier Cancelation','Supplier Self Cancelation','Supplier On Demand Rejection',
             'Supplier Cancelation Other','supplier_on_demand_rejection')) THEN bo.nr END) as supplier_cancelled_nr
     , SUM(CASE WHEN bo.status_id = 2 AND (cancellation_category IN ('Supplier Self Cancelation') OR cancellation_sub_category IN ('Supplier Cancelation','Supplier Self Cancelation','Supplier On Demand Rejection',
             'Supplier Cancelation Other','supplier_on_demand_rejection')) THEN bo.gmv END) as supplier_cancelled_gmv
     , SUM(CASE WHEN bo.is_supplier_force_majeure = 'YES' THEN bo.gmv END) as supplier_cancelled_gmv_force_majeure
     , SUM(bo.gmv * bud.goodwill_percentage) as goodwill_cost_at_checkout
     , SUM(bo.bookings) AS bookings
     , SUM(bo.transactions) AS transactions
     , SUM(bo.daily_customers) AS daily_unique_customers
     , SUM(bo.monthly_customers) AS monthly_unique_customers
     , SUM(bo.quarterly_customers) AS quarterly_unique_customers
     , SUM(bo.yearly_customers) AS yearly_unique_customers
     , SUM(CASE WHEN bo.status_id = 1 AND !is_inventory_relevant THEN nr END) AS nr_marketplace_active
     , SUM(CASE WHEN bo.status_id = 1 AND !is_inventory_relevant THEN bo.forcasted_cancellation_rate*nr END) AS nr_marketplace_forecasted_cancellation
     , SUM(CASE WHEN bo.status_id = 2 AND !is_inventory_relevant THEN nr END) AS nr_marketplace_cancelled
     , SUM(CASE WHEN bo.status_id = 1 AND is_inventory_relevant THEN gmv END) AS nr_tt_active -- gmv is nr for tt
     , SUM(CASE WHEN bo.status_id = 1 AND is_inventory_relevant THEN bo.forcasted_cancellation_rate*gmv END) AS nr_tt_forecasted_cancellation
     , SUM(CASE WHEN bo.status_id = 2 AND is_inventory_relevant THEN gmv END) AS nr_tt_cancelled
     , SUM(CASE WHEN bo.status_id = 1 AND !is_inventory_relevant THEN (bo.gmv - bo.gmv_supplier) END) AS fx_premium_marketplace
     , SUM(CASE WHEN bo.status_id = 1 AND is_inventory_relevant THEN (bo.gmv - bo.gmv_supplier) END) AS fx_premium_tt
     , SUM(CASE WHEN bo.status_id = 1 AND !is_inventory_relevant THEN bo.forcasted_cancellation_rate*(bo.gmv - bo.gmv_supplier) END) AS fx_premium_marketplace_forecasted_cancellation
     , SUM(CASE WHEN bo.status_id = 1 AND is_inventory_relevant THEN bo.forcasted_cancellation_rate*(bo.gmv - bo.gmv_supplier) END) AS fx_premium_tt_forecasted_cancellation
     , SUM(vat_tt                                                ) AS vat_tt
     , SUM(vat_tt_forecasted_cancellation                        ) AS vat_tt_forecasted_cancellation
     , SUM(vat_adjustment_to_toms                                ) AS vat_adjustment_to_toms
     , SUM(vat_adjustment_to_toms_forecasted_cancellation        ) AS vat_adjustment_to_toms_forecasted_cancellation
     , SUM(selling_tax_tt                                        ) AS selling_tax_tt
     , SUM(selling_tax_tt_forecasted_cancellation                ) AS selling_tax_tt_forecasted_cancellation
     , SUM(selling_tax_adjustment_to_toms                        ) AS selling_tax_adjustment_to_toms
     , SUM(selling_tax_adjustment_to_toms_forecasted_cancellation) AS selling_tax_adjustment_to_toms_forecasted_cancellation
     , SUM(net_tax_tt                                            ) AS net_tax_tt
     , SUM(net_tax_tt_forecasted_cancellation                    ) AS net_tax_tt_forecasted_cancellation
     , SUM(net_tax_adjustment_to_toms                            ) AS net_tax_adjustment_to_toms
     , SUM(net_tax_adjustment_to_toms_forecasted_cancellation    ) AS net_tax_adjustment_to_toms_forecasted_cancellation

     , SUM(CASE WHEN status_id = 1 AND is_inventory_relevant THEN gmv - nr END) AS ticket_cost_tt
     , SUM(CASE WHEN status_id = 1 AND is_inventory_relevant THEN (gmv - nr) * bo.forcasted_cancellation_rate END) AS ticket_cost_tt_forecasted_cancellation
     , SUM(CASE WHEN status_id = 1 AND is_inventory_relevant THEN  gmv - nr - vat_tt - net_tax_tt END) AS ticket_cost_w_tax_tt -- +/- OVISI O PREDZNAKU
     , SUM(CASE WHEN status_id = 1 AND is_inventory_relevant THEN (gmv - nr - vat_tt - net_tax_tt) * bo.forcasted_cancellation_rate END) AS ticket_cost_w_tax_tt_forecasted_cancellation
  FROM booking_with_channel bo
  LEFT JOIN budget_goodwill bud on bo.checkout_date = bud.date
 GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
)

-- reseller cost
, contribution_margin_reseller_cost AS (
SELECT bo.checkout_date
     , bo.date_of_travel_berlin_time
     , CAST(NULL AS DATE) AS cancellation_date
     , CAST(NULL AS DATE) AS refund_date
     , CAST(NULL AS DATE) AS event_date
     , bo.checkout_date AS actual_date_checkout
     , bo.date_of_travel_berlin_time AS actual_date_travel
     , bo.reseller_id
     , bo.customer_first_checkout_date -- newly added
     , b.channel AS channel
     , bo.purchase_type_id
     , bo.destination_city_id
     , bo.source_market_country_id
     , bo.tour_id
     , bo.tour_option_id
     , 'reseller_cost' AS cost_type
     , bo.is_rnpl
     , bo.is_inventory_relevant
     , SUM(CASE WHEN status_id = 1 THEN reseller_share END) AS reseller_cost_active
     , SUM(CASE WHEN status_id = 2 THEN reseller_share END) AS reseller_cost_cancelled
     , SUM(CASE WHEN status_id = 1 THEN reseller_share*bo.forcasted_cancellation_rate END) AS reseller_cost_forecasted_cancellation
  FROM {{ ref('stg_contribution_margin_booking_base') }} bo
  LEFT JOIN reseller_channel b ON bo.reseller_id = b.reseller_id
 WHERE reseller_share != 0
 GROUP BY  1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
)

-- fixed ad spend
, daily_gmv AS (
  SELECT checkout_date
       , SUM(gmv) AS gmv
    FROM {{ ref('stg_contribution_margin_booking_base') }}
   GROUP BY 1
)
, contribution_margin_fixed_ad_spend AS (
SELECT b.checkout_date
     , a.fixed_ad_spend*gmv / SUM(gmv) OVER (PARTITION BY DATE (date_trunc('month',b.checkout_date))) AS fixed_ad_spend
  FROM {{ source('default', 'contribution_margin_fixed_adspend_upload') }} a
  LEFT JOIN daily_gmv b ON a.month = DATE (date_trunc ('month',b.checkout_date))
)
,
for_breakage AS (
SELECT
checkout_date
, tour_option_id
, LAST(is_inventory_relevant) is_inventory_relevant -- Use any aggregated function to get unique value per the grouped dimensions, some dates from 2017/2018 has 2 is_inventory_relevant values.
FROM {{ ref('stg_contribution_margin_booking_base') }}
GROUP BY 1,2
)

SELECT a.checkout_date
     , a.date_of_travel_berlin_time AS travel_date_berlin_time
     , a.cancellation_date
     , a.refund_date
     , a.event_date
     , a.actual_date_checkout
     , a.actual_date_travel
     , a.reseller_id
     , a.customer_first_checkout_date -- newly added
     , a.channel
     , a.purchase_type_id
     , a.destination_city_id
     , a.source_market_country_id
     , a.tour_id
     , a.tour_option_id
     , 'others' AS cost_type
     , a.is_rnpl
     , a.is_inventory_relevant
     , a.nr
     , a.gmv
     , a.gmv_cancelled
     , a.nr_cancelled
     , a.supplier_cancelled_nr
     , a.supplier_cancelled_gmv
     , a.supplier_cancelled_gmv_force_majeure
     , a.goodwill_cost_at_checkout
     , a.bookings
     , a.nr_marketplace_active
     , a.nr_marketplace_forecasted_cancellation
     , a.nr_marketplace_cancelled

     , a.nr_tt_active
     , a.nr_tt_forecasted_cancellation
     , a.nr_tt_cancelled

     , a.fx_premium_marketplace
     , a.fx_premium_tt
     , a.fx_premium_marketplace_forecasted_cancellation
     , a.fx_premium_tt_forecasted_cancellation
     , a.vat_tt
     , a.vat_tt_forecasted_cancellation
     , a.vat_adjustment_to_toms
     , a.vat_adjustment_to_toms_forecasted_cancellation
     
     , a.selling_tax_tt
     , a.selling_tax_tt_forecasted_cancellation
     , a.selling_tax_adjustment_to_toms
     , a.selling_tax_adjustment_to_toms_forecasted_cancellation
     
     , a.net_tax_tt
     , a.net_tax_tt_forecasted_cancellation
     , a.net_tax_adjustment_to_toms
     , a.net_tax_adjustment_to_toms_forecasted_cancellation
     
     , a.ticket_cost_tt
     , a.ticket_cost_tt_forecasted_cancellation
     
     , a.ticket_cost_w_tax_tt
     , a.ticket_cost_w_tax_tt_forecasted_cancellation
     
     , a.transactions --newly added
     , a.daily_unique_customers --newly added
     , a.monthly_unique_customers --newly added
     , a.quarterly_unique_customers --newly added
     , a.yearly_unique_customers --newly added
     , CAST(NULL AS DOUBLE) AS payment_cost
     , CAST(NULL AS DOUBLE) AS ad_spend
     , CAST(NULL AS DOUBLE) AS marketing_coupon_cost
     , CAST(NULL AS DOUBLE) AS non_marketing_coupon_cost
     , CAST(NULL AS DOUBLE) AS coupon_cost_active
     , CAST(NULL AS DOUBLE) AS coupon_cost_forecasted_cancellation

     , CAST(NULL AS DOUBLE) AS gwc_free_cancellation
     , CAST(NULL AS DOUBLE) AS gwc_total
     , CAST(NULL AS DOUBLE) AS rr_total
     , CAST(NULL AS DOUBLE) AS cat_total
     , CAST(NULL AS DOUBLE) AS premium_option_fee_revenue
     , CAST(NULL AS DOUBLE) AS supplier_cancellation_fee
     , CAST(NULL AS DOUBLE) AS fee_tootal
     
     , CAST(NULL AS DOUBLE) AS reseller_cost_active
     , CAST(NULL AS DOUBLE) AS reseller_cost_cancelled
     , CAST(NULL AS DOUBLE) AS reseller_cost_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS breakage
     , CAST(NULL AS DOUBLE) AS fixed_ad_spend

     , CAST(NULL AS DOUBLE) AS cancelation_compensation_amount_eur
     , CAST(NULL AS DOUBLE) AS manual_cancellation_adjustment_amount_eur
     , CAST(NULL AS DOUBLE) AS manual_commission_adjustment_amount_eur
  FROM contribution_margin_other_costs a
UNION ALL
SELECT checkout_date
     , date_of_travel_berlin_time
     , cancellation_date
     , refund_date
     , event_date
     , actual_date_checkout
     , actual_date_travel
     , reseller_id
     , customer_first_checkout_date
     , channel
     , purchase_type_id
     , destination_city_id
     , source_market_country_id
     , tour_id
     , tour_option_id
     , 'gwc' AS cost_type
     , is_rnpl
     , is_inventory_relevant
     , CAST(NULL AS DOUBLE) AS nr
     , CAST(NULL AS DOUBLE) AS gmv
     , CAST(NULL AS DOUBLE) AS gmv_cancelled
     , CAST(NULL AS DOUBLE) AS nr_cancelled
     , CAST(NULL AS DOUBLE) AS supplier_cancelled_nr
     , CAST(NULL AS DOUBLE) AS supplier_cancelled_gmv
     , CAST(NULL AS DOUBLE) AS supplier_cancelled_gmv_force_majeure
     , CAST(NULL AS DOUBLE) AS goodwill_cost_at_checkout
     , CAST(NULL AS DOUBLE) AS bookings
     , CAST(NULL AS DOUBLE) AS nr_marketplace_active
     , CAST(NULL AS DOUBLE) AS nr_marketplace_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS nr_marketplace_cancelled

     , CAST(NULL AS DOUBLE) AS nr_tt_active
     , CAST(NULL AS DOUBLE) AS nr_tt_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS nr_tt_cancelled

     , CAST(NULL AS DOUBLE) AS fx_premium_marketplace
     , CAST(NULL AS DOUBLE) AS fx_premium_tt
     , CAST(NULL AS DOUBLE) AS fx_premium_marketplace_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS fx_premium_tt_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS vat_tt
     , CAST(NULL AS DOUBLE) AS vat_tt_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS vat_adjustment_to_toms
     , CAST(NULL AS DOUBLE) AS vat_adjustment_to_toms_forecasted_cancellation

     , CAST(NULL AS DOUBLE) AS selling_tax_tt
     , CAST(NULL AS DOUBLE) AS selling_tax_tt_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS selling_tax_adjustment_to_toms
     , CAST(NULL AS DOUBLE) AS selling_tax_adjustment_to_toms_forecasted_cancellation

     , CAST(NULL AS DOUBLE) AS net_tax_tt
     , CAST(NULL AS DOUBLE) AS net_tax_tt_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS net_tax_adjustment_to_toms
     , CAST(NULL AS DOUBLE) AS net_tax_adjustment_to_toms_forecasted_cancellation

     , CAST(NULL AS DOUBLE) AS ticket_cost_tt
     , CAST(NULL AS DOUBLE) AS ticket_cost_tt_forecasted_cancellation

     , CAST(NULL AS DOUBLE) AS ticket_cost_w_tax_tt
     , CAST(NULL AS DOUBLE) AS ticket_cost_w_tax_tt_forecasted_cancellation

     , CAST(NULL AS BIGINT) AS transactions --newly added
     , CAST(NULL AS BIGINT) AS daily_unique_customers --newly added
     , CAST(NULL AS BIGINT) AS monthly_unique_customers --newly added
     , CAST(NULL AS BIGINT) AS quarterly_unique_customers --newly added
     , CAST(NULL AS BIGINT) AS yearly_unique_customers --newly added
     , CAST(NULL AS DOUBLE) AS payment_cost
     , CAST(NULL AS DOUBLE) AS ad_spend
     , CAST(NULL AS DOUBLE) AS marketing_coupon_cost
     , CAST(NULL AS DOUBLE) AS non_marketing_coupon_cost
     , CAST(NULL AS DOUBLE) AS coupon_cost_active
     , CAST(NULL AS DOUBLE) AS coupon_cost_forecasted_cancellation
     
     , gwc_free_cancellation
     , gwc_total
     , rr_total
     , cat_total
     , premium_option_fee_revenue
     , supplier_cancellation_fee
     , fee_tootal
     
     , CAST(NULL AS DOUBLE) AS reseller_cost_active
     , CAST(NULL AS DOUBLE) AS reseller_cost_cancelled
     , CAST(NULL AS DOUBLE) AS reseller_cost_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS breakage
     , CAST(NULL AS DOUBLE) AS fixed_ad_spend
     
     , CAST(NULL AS DOUBLE) AS cancelation_compensation_amount_eur
     , CAST(NULL AS DOUBLE) AS manual_cancellation_adjustment_amount_eur
     , CAST(NULL AS DOUBLE) AS manual_commission_adjustment_amount_eur
  FROM gwc
UNION ALL
SELECT checkout_date
     , date_of_travel_berlin_time
     , cancellation_date
     , refund_date
     , event_date
     , actual_date_checkout
     , actual_date_travel
     , reseller_id
     , customer_first_checkout_date
     , channel
     , purchase_type_id
     , destination_city_id
     , source_market_country_id
     , tour_id
     , tour_option_id
     , 'coupon_cost' AS cost_type
     , is_rnpl
     , is_inventory_relevant
     , CAST(NULL AS DOUBLE) AS nr
     , CAST(NULL AS DOUBLE) AS gmv
     , CAST(NULL AS DOUBLE) AS gmv_cancelled
     , CAST(NULL AS DOUBLE) AS nr_cancelled
     , CAST(NULL AS DOUBLE) AS supplier_cancelled_nr
     , CAST(NULL AS DOUBLE) AS supplier_cancelled_gmv
     , CAST(NULL AS DOUBLE) AS supplier_cancelled_gmv_force_majeure
     , CAST(NULL AS DOUBLE) AS goodwill_cost_at_checkout
     , CAST(NULL AS DOUBLE) AS bookings
     , CAST(NULL AS DOUBLE) AS nr_marketplace_active
     , CAST(NULL AS DOUBLE) AS nr_marketplace_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS nr_marketplace_cancelled

     , CAST(NULL AS DOUBLE) AS nr_tt_active
     , CAST(NULL AS DOUBLE) AS nr_tt_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS nr_tt_cancelled

     , CAST(NULL AS DOUBLE) AS fx_premium_marketplace
     , CAST(NULL AS DOUBLE) AS fx_premium_tt
     , CAST(NULL AS DOUBLE) AS fx_premium_marketplace_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS fx_premium_tt_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS vat_tt
     , CAST(NULL AS DOUBLE) AS vat_tt_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS vat_adjustment_to_toms
     , CAST(NULL AS DOUBLE) AS vat_adjustment_to_toms_forecasted_cancellation
     
     , CAST(NULL AS DOUBLE) AS selling_tax_tt
     , CAST(NULL AS DOUBLE) AS selling_tax_tt_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS selling_tax_adjustment_to_toms
     , CAST(NULL AS DOUBLE) AS selling_tax_adjustment_to_toms_forecasted_cancellation

     , CAST(NULL AS DOUBLE) AS net_tax_tt
     , CAST(NULL AS DOUBLE) AS net_tax_tt_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS net_tax_adjustment_to_toms
     , CAST(NULL AS DOUBLE) AS net_tax_adjustment_to_toms_forecasted_cancellation

     , CAST(NULL AS DOUBLE) AS ticket_cost_tt
     , CAST(NULL AS DOUBLE) AS ticket_cost_tt_forecasted_cancellation

     , CAST(NULL AS DOUBLE) AS ticket_cost_w_tax_tt
     , CAST(NULL AS DOUBLE) AS ticket_cost_w_tax_tt_forecasted_cancellation
     
     , CAST(NULL AS BIGINT) AS transactions --newly added
     , CAST(NULL AS BIGINT) AS daily_unique_customers --newly added
     , CAST(NULL AS BIGINT) AS monthly_unique_customers --newly added
     , CAST(NULL AS BIGINT) AS quarterly_unique_customers --newly added
     , CAST(NULL AS BIGINT) AS yearly_unique_customers --newly added
     , CAST(NULL AS DOUBLE) AS payment_cost
     , CAST(NULL AS DOUBLE) AS ad_spend
     , marketing_coupon_cost
     , non_marketing_coupon_cost
     , coupon_cost_active
     , coupon_cost_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS gwc_free_cancellation
     , CAST(NULL AS DOUBLE) AS gwc_total
     , CAST(NULL AS DOUBLE) AS rr_total
     , CAST(NULL AS DOUBLE) AS cat_total
     , CAST(NULL AS DOUBLE) AS premium_option_fee_revenue
     , CAST(NULL AS DOUBLE) AS supplier_cancellation_fee
     , CAST(NULL AS DOUBLE) AS fee_tootal
     
     , CAST(NULL AS DOUBLE) AS reseller_cost_active
     , CAST(NULL AS DOUBLE) AS reseller_cost_cancelled
     , CAST(NULL AS DOUBLE) AS reseller_cost_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS breakage
     , CAST(NULL AS DOUBLE) AS fixed_ad_spend
     
     , CAST(NULL AS DOUBLE) AS cancelation_compensation_amount_eur
     , CAST(NULL AS DOUBLE) AS manual_cancellation_adjustment_amount_eur
     , CAST(NULL AS DOUBLE) AS manual_commission_adjustment_amount_eur
  FROM contribution_margin_coupon_cost
UNION ALL
SELECT checkout_date
     , date_of_travel_berlin_time
     , cancellation_date
     , refund_date
     , event_date
     , actual_date_checkout
     , actual_date_travel
     , reseller_id
     , customer_first_checkout_date
     , channel
     , purchase_type_id
     , destination_city_id
     , source_market_country_id
     , tour_id
     , tour_option_id
     , 'payment_cost' AS cost_type
     , is_rnpl
     , is_inventory_relevant
     , CAST(NULL AS DOUBLE) AS nr
     , CAST(NULL AS DOUBLE) AS gmv
     , CAST(NULL AS DOUBLE) AS gmv_cancelled
     , CAST(NULL AS DOUBLE) AS nr_cancelled
     , CAST(NULL AS DOUBLE) AS supplier_cancelled_nr
     , CAST(NULL AS DOUBLE) AS supplier_cancelled_gmv
     , CAST(NULL AS DOUBLE) AS supplier_cancelled_gmv_force_majeure
     , CAST(NULL AS DOUBLE) AS goodwill_cost_at_checkout
     , CAST(NULL AS DOUBLE) AS bookings
     , CAST(NULL AS DOUBLE) AS nr_marketplace_active
     , CAST(NULL AS DOUBLE) AS nr_marketplace_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS nr_marketplace_cancelled

     , CAST(NULL AS DOUBLE) AS nr_tt_active
     , CAST(NULL AS DOUBLE) AS nr_tt_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS nr_tt_cancelled

     , CAST(NULL AS DOUBLE) AS fx_premium_marketplace
     , CAST(NULL AS DOUBLE) AS fx_premium_tt
     , CAST(NULL AS DOUBLE) AS fx_premium_marketplace_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS fx_premium_tt_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS vat_tt
     , CAST(NULL AS DOUBLE) AS vat_tt_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS vat_adjustment_to_toms
     , CAST(NULL AS DOUBLE) AS vat_adjustment_to_toms_forecasted_cancellation
     
     , CAST(NULL AS DOUBLE) AS selling_tax_tt
     , CAST(NULL AS DOUBLE) AS selling_tax_tt_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS selling_tax_adjustment_to_toms
     , CAST(NULL AS DOUBLE) AS selling_tax_adjustment_to_toms_forecasted_cancellation

     , CAST(NULL AS DOUBLE) AS net_tax_tt
     , CAST(NULL AS DOUBLE) AS net_tax_tt_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS net_tax_adjustment_to_toms
     , CAST(NULL AS DOUBLE) AS net_tax_adjustment_to_toms_forecasted_cancellation

     , CAST(NULL AS DOUBLE) AS ticket_cost_tt
     , CAST(NULL AS DOUBLE) AS ticket_cost_tt_forecasted_cancellation

     , CAST(NULL AS DOUBLE) AS ticket_cost_w_tax_tt
     , CAST(NULL AS DOUBLE) AS ticket_cost_w_tax_tt_forecasted_cancellation

     , CAST(NULL AS BIGINT) AS transactions --newly added
     , CAST(NULL AS BIGINT) AS daily_unique_customers --newly added
     , CAST(NULL AS BIGINT) AS monthly_unique_customers --newly added
     , CAST(NULL AS BIGINT) AS quarterly_unique_customers --newly added
     , CAST(NULL AS BIGINT) AS yearly_unique_customers --newly added
     , cost_of_payment AS payment_cost
     , CAST(NULL AS DOUBLE) AS ad_spend
     , CAST(NULL AS DOUBLE) AS marketing_coupon_cost
     , CAST(NULL AS DOUBLE) AS non_marketing_coupon_cost
     , CAST(NULL AS DOUBLE) AS coupon_cost_active
     , CAST(NULL AS DOUBLE) AS coupon_cost_forecasted_cancellation

     , CAST(NULL AS DOUBLE) AS gwc_free_cancellation
     , CAST(NULL AS DOUBLE) AS gwc_total
     , CAST(NULL AS DOUBLE) AS rr_total
     , CAST(NULL AS DOUBLE) AS cat_total
     , CAST(NULL AS DOUBLE) AS premium_option_fee_revenue
     , CAST(NULL AS DOUBLE) AS supplier_cancellation_fee
     , CAST(NULL AS DOUBLE) AS fee_tootal
     
     , CAST(NULL AS DOUBLE) AS reseller_cost_active
     , CAST(NULL AS DOUBLE) AS reseller_cost_cancelled
     , CAST(NULL AS DOUBLE) AS reseller_cost_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS breakage
     , CAST(NULL AS DOUBLE) AS fixed_ad_spend
     
     , CAST(NULL AS DOUBLE) AS cancelation_compensation_amount_eur
     , CAST(NULL AS DOUBLE) AS manual_cancellation_adjustment_amount_eur
     , CAST(NULL AS DOUBLE) AS manual_commission_adjustment_amount_eur
  FROM contribution_margin_payment_cost

UNION ALL
SELECT checkout_date
     , date_of_travel_berlin_time
     , cancellation_date
     , refund_date
     , event_date
     , actual_date_checkout
     , actual_date_travel
     , reseller_id
     , customer_first_checkout_date
     , channel
     , purchase_type_id
     , destination_city_id
     , source_market_country_id
     , tour_id
     , tour_option_id
     , 'reseller_cost' AS cost_type
     , is_rnpl
     , is_inventory_relevant
     , CAST(NULL AS DOUBLE) AS nr
     , CAST(NULL AS DOUBLE) AS gmv
     , CAST(NULL AS DOUBLE) AS gmv_cancelled
     , CAST(NULL AS DOUBLE) AS nr_cancelled
     , CAST(NULL AS DOUBLE) AS supplier_cancelled_nr
     , CAST(NULL AS DOUBLE) AS supplier_cancelled_gmv
     , CAST(NULL AS DOUBLE) AS supplier_cancelled_gmv_force_majeure
     , CAST(NULL AS DOUBLE) AS goodwill_cost_at_checkout
     , CAST(NULL AS DOUBLE) AS bookings
     , CAST(NULL AS DOUBLE) AS nr_marketplace_active
     , CAST(NULL AS DOUBLE) AS nr_marketplace_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS nr_marketplace_cancelled

     , CAST(NULL AS DOUBLE) AS nr_tt_active
     , CAST(NULL AS DOUBLE) AS nr_tt_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS nr_tt_cancelled

     , CAST(NULL AS DOUBLE) AS fx_premium_marketplace
     , CAST(NULL AS DOUBLE) AS fx_premium_tt
     , CAST(NULL AS DOUBLE) AS fx_premium_marketplace_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS fx_premium_tt_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS vat_tt
     , CAST(NULL AS DOUBLE) AS vat_tt_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS vat_adjustment_to_toms
     , CAST(NULL AS DOUBLE) AS vat_adjustment_to_toms_forecasted_cancellation
     
     , CAST(NULL AS DOUBLE) AS selling_tax_tt
     , CAST(NULL AS DOUBLE) AS selling_tax_tt_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS selling_tax_adjustment_to_toms
     , CAST(NULL AS DOUBLE) AS selling_tax_adjustment_to_toms_forecasted_cancellation

     , CAST(NULL AS DOUBLE) AS net_tax_tt
     , CAST(NULL AS DOUBLE) AS net_tax_tt_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS net_tax_adjustment_to_toms
     , CAST(NULL AS DOUBLE) AS net_tax_adjustment_to_toms_forecasted_cancellation

     , CAST(NULL AS DOUBLE) AS ticket_cost_tt
     , CAST(NULL AS DOUBLE) AS ticket_cost_tt_forecasted_cancellation

     , CAST(NULL AS DOUBLE) AS ticket_cost_w_tax_tt
     , CAST(NULL AS DOUBLE) AS ticket_cost_w_tax_tt_forecasted_cancellation

     , CAST(NULL AS BIGINT) AS transactions --newly added
     , CAST(NULL AS BIGINT) AS daily_unique_customers --newly added
     , CAST(NULL AS BIGINT) AS monthly_unique_customers --newly added
     , CAST(NULL AS BIGINT) AS quarterly_unique_customers --newly added
     , CAST(NULL AS BIGINT) AS yearly_unique_customers --newly added
     , CAST(NULL AS DOUBLE) AS payment_cost
     , CAST(NULL AS DOUBLE) AS ad_spend
     , CAST(NULL AS DOUBLE) AS marketing_coupon_cost
     , CAST(NULL AS DOUBLE) AS non_marketing_coupon_cost
     , CAST(NULL AS DOUBLE) AS coupon_cost_active
     , CAST(NULL AS DOUBLE) AS coupon_cost_forecasted_cancellation
     
     , CAST(NULL AS DOUBLE) AS gwc_free_cancellation
     , CAST(NULL AS DOUBLE) AS gwc_total
     , CAST(NULL AS DOUBLE) AS rr_total
     , CAST(NULL AS DOUBLE) AS cat_total
     , CAST(NULL AS DOUBLE) AS premium_option_fee_revenue
     , CAST(NULL AS DOUBLE) AS supplier_cancellation_fee
     , CAST(NULL AS DOUBLE) AS fee_tootal
     
     , reseller_cost_active
     , reseller_cost_cancelled
     , reseller_cost_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS breakage
     , CAST(NULL AS DOUBLE) AS fixed_ad_spend
     
     , CAST(NULL AS DOUBLE) AS cancelation_compensation_amount_eur
     , CAST(NULL AS DOUBLE) AS manual_cancellation_adjustment_amount_eur
     , CAST(NULL AS DOUBLE) AS manual_commission_adjustment_amount_eur
  FROM contribution_margin_reseller_cost
UNION ALL
SELECT br.checkout_date AS checkout_date
     , br.checkout_date AS date_of_travel_berlin_time
     , CAST(NULL AS DATE) AS cancellation_date
     , CAST(NULL AS DATE) AS refund_date
     , CAST(NULL AS DATE) AS event_date
     , br.checkout_date AS actual_date_checkout
     , br.checkout_date AS actual_date_travel
     , CAST(NULL AS BIGINT) AS reseller_id
     , CAST(NULL AS DATE) AS customer_first_checkout_date
     , channel
     , purchase_type_id
     , destination_city_id
     , source_market_country_id
     , tour_id
     , br.tour_option_id
     , 'breakage' AS cost_type
     , CAST(NULL AS BOOLEAN) is_rnpl
     , fb.is_inventory_relevant
     , CAST(NULL AS DOUBLE) AS nr
     , CAST(NULL AS DOUBLE) AS gmv
     , CAST(NULL AS DOUBLE) AS gmv_cancelled
     , CAST(NULL AS DOUBLE) AS nr_cancelled
     , CAST(NULL AS DOUBLE) AS supplier_cancelled_nr
     , CAST(NULL AS DOUBLE) AS supplier_cancelled_gmv
     , CAST(NULL AS DOUBLE) AS supplier_cancelled_gmv_force_majeure
     , CAST(NULL AS DOUBLE) AS goodwill_cost_at_checkout
     , CAST(NULL AS DOUBLE) AS bookings
     , CAST(NULL AS DOUBLE) AS nr_marketplace_active
     , CAST(NULL AS DOUBLE) AS nr_marketplace_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS nr_marketplace_cancelled

     , CAST(NULL AS DOUBLE) AS nr_tt_active
     , CAST(NULL AS DOUBLE) AS nr_tt_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS nr_tt_cancelled

     , CAST(NULL AS DOUBLE) AS fx_premium_marketplace
     , CAST(NULL AS DOUBLE) AS fx_premium_tt
     , CAST(NULL AS DOUBLE) AS fx_premium_marketplace_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS fx_premium_tt_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS vat_tt
     , CAST(NULL AS DOUBLE) AS vat_tt_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS vat_adjustment_to_toms
     , CAST(NULL AS DOUBLE) AS vat_adjustment_to_toms_forecasted_cancellation
     
     , CAST(NULL AS DOUBLE) AS selling_tax_tt
     , CAST(NULL AS DOUBLE) AS selling_tax_tt_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS selling_tax_adjustment_to_toms
     , CAST(NULL AS DOUBLE) AS selling_tax_adjustment_to_toms_forecasted_cancellation

     , CAST(NULL AS DOUBLE) AS net_tax_tt
     , CAST(NULL AS DOUBLE) AS net_tax_tt_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS net_tax_adjustment_to_toms
     , CAST(NULL AS DOUBLE) AS net_tax_adjustment_to_toms_forecasted_cancellation

     , CAST(NULL AS DOUBLE) AS ticket_cost_tt
     , CAST(NULL AS DOUBLE) AS ticket_cost_tt_forecasted_cancellation

     , CAST(NULL AS DOUBLE) AS ticket_cost_w_tax_tt
     , CAST(NULL AS DOUBLE) AS ticket_cost_w_tax_tt_forecasted_cancellation

     , CAST(NULL AS BIGINT) AS transactions --newly added
     , CAST(NULL AS BIGINT) AS daily_unique_customers --newly added
     , CAST(NULL AS BIGINT) AS monthly_unique_customers --newly added
     , CAST(NULL AS BIGINT) AS quarterly_unique_customers --newly added
     , CAST(NULL AS BIGINT) AS yearly_unique_customers --newly added
     , CAST(NULL AS DOUBLE) AS payment_cost
     , CAST(NULL AS DOUBLE) AS ad_spend
     , CAST(NULL AS DOUBLE) AS marketing_coupon_cost
     , CAST(NULL AS DOUBLE) AS non_marketing_coupon_cost
     , CAST(NULL AS DOUBLE) AS coupon_cost_active
     , CAST(NULL AS DOUBLE) AS coupon_cost_forecasted_cancellation
     
     , CAST(NULL AS DOUBLE) AS gwc_free_cancellation
     , CAST(NULL AS DOUBLE) AS gwc_total
     , CAST(NULL AS DOUBLE) AS rr_total
     , CAST(NULL AS DOUBLE) AS cat_total
     , CAST(NULL AS DOUBLE) AS premium_option_fee_revenue
     , CAST(NULL AS DOUBLE) AS supplier_cancellation_fee
     , CAST(NULL AS DOUBLE) AS fee_tootal
     
     , CAST(NULL AS DOUBLE) AS reseller_cost_active
     , CAST(NULL AS DOUBLE) AS reseller_cost_cancelled
     , CAST(NULL AS DOUBLE) AS reseller_cost_forecasted_cancellation
     , breakage
     , CAST(NULL AS DOUBLE) AS fixed_ad_spend

     , CAST(NULL AS DOUBLE) AS cancelation_compensation_amount_eur
     , CAST(NULL AS DOUBLE) AS manual_cancellation_adjustment_amount_eur
     , CAST(NULL AS DOUBLE) AS manual_commission_adjustment_amount_eur
  FROM {{ source('default', 'agg_contribution_margin_breakage') }} br
  LEFT JOIN for_breakage fb ON br.tour_option_id = fb.tour_option_id AND br.checkout_date = fb.checkout_date
UNION ALL
SELECT checkout_date AS checkout_date
     , checkout_date AS date_of_travel_berlin_time
     , CAST(NULL AS DATE) AS cancellation_date
     , CAST(NULL AS DATE) AS refund_date
     , CAST(NULL AS DATE) AS event_date
     , checkout_date AS actual_date_checkout
     , checkout_date AS actual_date_travel
     , CAST(NULL AS BIGINT) AS reseller_id
     , CAST(NULL AS DATE) AS customer_first_checkout_date
     , CAST(NULL AS STRING) AS channel
     , CAST(NULL AS BIGINT) AS purchase_type_id
     , CAST(NULL AS BIGINT) AS destination_city_id
     , CAST(NULL AS BIGINT) AS source_market_country_id
     , CAST(NULL AS BIGINT) AS tour_id
     , CAST(NULL AS BIGINT) AS tour_option_id
     , 'fixed_ad_spend' AS cost_type
     , CAST(NULL AS BOOLEAN) is_rnpl
     , CAST(NULL AS BOOLEAN) is_inventory_relevant
     , CAST(NULL AS DOUBLE) AS nr
     , CAST(NULL AS DOUBLE) AS gmv
     , CAST(NULL AS DOUBLE) AS gmv_cancelled
     , CAST(NULL AS DOUBLE) AS nr_cancelled
     , CAST(NULL AS DOUBLE) AS supplier_cancelled_nr
     , CAST(NULL AS DOUBLE) AS supplier_cancelled_gmv
     , CAST(NULL AS DOUBLE) AS supplier_cancelled_gmv_force_majeure
     , CAST(NULL AS DOUBLE) AS goodwill_cost_at_checkout
     , CAST(NULL AS DOUBLE) AS bookings
     , CAST(NULL AS DOUBLE) AS nr_marketplace_active
     , CAST(NULL AS DOUBLE) AS nr_marketplace_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS nr_marketplace_cancelled

     , CAST(NULL AS DOUBLE) AS nr_tt_active
     , CAST(NULL AS DOUBLE) AS nr_tt_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS nr_tt_cancelled

     , CAST(NULL AS DOUBLE) AS fx_premium_marketplace
     , CAST(NULL AS DOUBLE) AS fx_premium_tt
     , CAST(NULL AS DOUBLE) AS fx_premium_marketplace_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS fx_premium_tt_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS vat_tt
     , CAST(NULL AS DOUBLE) AS vat_tt_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS vat_adjustment_to_toms
     , CAST(NULL AS DOUBLE) AS vat_adjustment_to_toms_forecasted_cancellation
     
     , CAST(NULL AS DOUBLE) AS selling_tax_tt
     , CAST(NULL AS DOUBLE) AS selling_tax_tt_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS selling_tax_adjustment_to_toms
     , CAST(NULL AS DOUBLE) AS selling_tax_adjustment_to_toms_forecasted_cancellation

     , CAST(NULL AS DOUBLE) AS net_tax_tt
     , CAST(NULL AS DOUBLE) AS net_tax_tt_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS net_tax_adjustment_to_toms
     , CAST(NULL AS DOUBLE) AS net_tax_adjustment_to_toms_forecasted_cancellation

     , CAST(NULL AS DOUBLE) AS ticket_cost_tt
     , CAST(NULL AS DOUBLE) AS ticket_cost_tt_forecasted_cancellation

     , CAST(NULL AS DOUBLE) AS ticket_cost_w_tax_tt
     , CAST(NULL AS DOUBLE) AS ticket_cost_w_tax_tt_forecasted_cancellation

     , CAST(NULL AS BIGINT) AS transactions --newly added
     , CAST(NULL AS BIGINT) AS daily_unique_customers --newly added
     , CAST(NULL AS BIGINT) AS monthly_unique_customers --newly added
     , CAST(NULL AS BIGINT) AS quarterly_unique_customers --newly added
     , CAST(NULL AS BIGINT) AS yearly_unique_customers --newly added
     , CAST(NULL AS DOUBLE) AS payment_cost
     , CAST(NULL AS DOUBLE) AS ad_spend
     , CAST(NULL AS DOUBLE) AS marketing_coupon_cost
     , CAST(NULL AS DOUBLE) AS non_marketing_coupon_cost
     , CAST(NULL AS DOUBLE) AS coupon_cost_active
     , CAST(NULL AS DOUBLE) AS coupon_cost_forecasted_cancellation
     
     , CAST(NULL AS DOUBLE) AS gwc_free_cancellation
     , CAST(NULL AS DOUBLE) AS gwc_total
     , CAST(NULL AS DOUBLE) AS rr_total
     , CAST(NULL AS DOUBLE) AS cat_total
     , CAST(NULL AS DOUBLE) AS premium_option_fee_revenue
     , CAST(NULL AS DOUBLE) AS supplier_cancellation_fee
     , CAST(NULL AS DOUBLE) AS fee_tootal
       
     , CAST(NULL AS DOUBLE) AS reseller_cost_active
     , CAST(NULL AS DOUBLE) AS reseller_cost_cancelled
     , CAST(NULL AS DOUBLE) AS reseller_cost_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS breakage
     , fixed_ad_spend
     
     , CAST(NULL AS DOUBLE) AS cancelation_compensation_amount_eur
     , CAST(NULL AS DOUBLE) AS manual_cancellation_adjustment_amount_eur
     , CAST(NULL AS DOUBLE) AS manual_commission_adjustment_amount_eur
  FROM contribution_margin_fixed_ad_spend
 UNION ALL
SELECT checkout_date
     , date_of_travel_berlin_time
     , cancellation_date
     , refund_date
     , event_date
     , actual_date_checkout
     , actual_date_travel
     , reseller_id
     , customer_first_checkout_date
     , channel
     , purchase_type_id
     , destination_city_id
     , source_market_country_id
     , tour_id
     , tour_option_id
     , 'adjustment_payments' AS cost_type
     , is_rnpl
     , is_inventory_relevant
     , CAST(NULL AS DOUBLE) AS nr
     , CAST(NULL AS DOUBLE) AS gmv
     , CAST(NULL AS DOUBLE) AS gmv_cancelled
     , CAST(NULL AS DOUBLE) AS nr_cancelled
     , CAST(NULL AS DOUBLE) AS supplier_cancelled_nr
     , CAST(NULL AS DOUBLE) AS supplier_cancelled_gmv
     , CAST(NULL AS DOUBLE) AS supplier_cancelled_gmv_force_majeure
     , CAST(NULL AS DOUBLE) AS goodwill_cost_at_checkout
     , CAST(NULL AS DOUBLE) AS bookings
     , CAST(NULL AS DOUBLE) AS nr_marketplace_active
     , CAST(NULL AS DOUBLE) AS nr_marketplace_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS nr_marketplace_cancelled

     , CAST(NULL AS DOUBLE) AS nr_tt_active
     , CAST(NULL AS DOUBLE) AS nr_tt_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS nr_tt_cancelled

     , CAST(NULL AS DOUBLE) AS fx_premium_marketplace
     , CAST(NULL AS DOUBLE) AS fx_premium_tt
     , CAST(NULL AS DOUBLE) AS fx_premium_marketplace_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS fx_premium_tt_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS vat_tt
     , CAST(NULL AS DOUBLE) AS vat_tt_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS vat_adjustment_to_toms
     , CAST(NULL AS DOUBLE) AS vat_adjustment_to_toms_forecasted_cancellation
     
     , CAST(NULL AS DOUBLE) AS selling_tax_tt
     , CAST(NULL AS DOUBLE) AS selling_tax_tt_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS selling_tax_adjustment_to_toms
     , CAST(NULL AS DOUBLE) AS selling_tax_adjustment_to_toms_forecasted_cancellation

     , CAST(NULL AS DOUBLE) AS net_tax_tt
     , CAST(NULL AS DOUBLE) AS net_tax_tt_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS net_tax_adjustment_to_toms
     , CAST(NULL AS DOUBLE) AS net_tax_adjustment_to_toms_forecasted_cancellation

     , CAST(NULL AS DOUBLE) AS ticket_cost_tt
     , CAST(NULL AS DOUBLE) AS ticket_cost_tt_forecasted_cancellation

     , CAST(NULL AS DOUBLE) AS ticket_cost_w_tax_tt
     , CAST(NULL AS DOUBLE) AS ticket_cost_w_tax_tt_forecasted_cancellation

     , CAST(NULL AS BIGINT) AS transactions --newly added
     , CAST(NULL AS BIGINT) AS daily_unique_customers --newly added
     , CAST(NULL AS BIGINT) AS monthly_unique_customers --newly added
     , CAST(NULL AS BIGINT) AS quarterly_unique_customers --newly added
     , CAST(NULL AS BIGINT) AS yearly_unique_customers --newly added
     , CAST(NULL AS DOUBLE) AS payment_cost
     , CAST(NULL AS DOUBLE) AS ad_spend
     , CAST(NULL AS DOUBLE) AS marketing_coupon_cost
     , CAST(NULL AS DOUBLE) AS non_marketing_coupon_cost
     , CAST(NULL AS DOUBLE) AS coupon_cost_active
     , CAST(NULL AS DOUBLE) AS coupon_cost_forecasted_cancellation
     
     , CAST(NULL AS DOUBLE) AS gwc_free_cancellation
     , CAST(NULL AS DOUBLE) AS gwc_total
     , CAST(NULL AS DOUBLE) AS rr_total
     , CAST(NULL AS DOUBLE) AS cat_total
     , CAST(NULL AS DOUBLE) AS premium_option_fee_revenue
     , CAST(NULL AS DOUBLE) AS supplier_cancellation_fee
     , CAST(NULL AS DOUBLE) AS fee_tootal
       
     , CAST(NULL AS DOUBLE) AS reseller_cost_active
     , CAST(NULL AS DOUBLE) AS reseller_cost_cancelled
     , CAST(NULL AS DOUBLE) AS reseller_cost_forecasted_cancellation
     , CAST(NULL AS DOUBLE) AS breakage
     , CAST(NULL AS DOUBLE) AS fixed_ad_spend
     
     , cancelation_compensation_amount_eur
     , manual_cancellation_adjustment_amount_eur
     , manual_commission_adjustment_amount_eur
  FROM adjustment_payments
