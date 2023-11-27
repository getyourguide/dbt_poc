{{ config(schema=var('default_schema')) }}

WITH contribution_margin_ad_spend AS (
SELECT DATE AS checkout_date
     , DATE AS date_of_travel_berlin_time
     , CAST(NULL AS DATE) AS cancellation_date
     , CAST(NULL AS DATE) AS refund_date
     , DATE AS event_date
     , DATE AS actual_date_checkout
     , DATE AS actual_date_travel
     , channel
     , pt.purchase_type_id
     , destination_city_id
     , a.source_market_country_id AS source_market_country_id
     , CAST(NULL AS BIGINT) AS tour_id
     , CAST(NULL AS BIGINT) AS tour_option_id
     , a.first_date_of_checkout AS customer_first_checkout_date
     , CAST(NULL AS BIGINT) AS reseller_id
     , 'ad_spend' AS cost_type
     , CAST(NULL AS BOOLEAN) AS is_rnpl
     , CAST(NULL AS BOOLEAN) AS is_inventory_relevant
     , SUM(total_estimated_cost) AS tracking_cost
  FROM {{ ref ('agg_source_market_cost_allocation') }} a
  LEFT JOIN {{ source('dwh','dim_purchase_type') }} pt ON a.purchase_type = pt.purchase_type_name
 WHERE a.date >= '2017-01-01'
 GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
)
,union_all_components AS (
     SELECT
     *
     FROM {{ source('default','stg_contribution_margin_without_ad_spend') }}

     UNION ALL

     SELECT checkout_date
          , date_of_travel_berlin_time
          , cancellation_date
          , refund_date
          , event_date
          , actual_date_checkout
          , actual_date_travel
          , CAST(NULL AS BIGINT) AS reseller_id
          , customer_first_checkout_date
          , channel
          , purchase_type_id
          , destination_city_id
          , source_market_country_id
          , tour_id
          , tour_option_id
          , 'ad_spend' AS cost_type
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
          , tracking_cost
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
       FROM contribution_margin_ad_spend
),
contribution_margin_budget_upload AS(
SELECT DATE
     , budget_segment_value1 AS channel_group
     , budget_segment_value2 AS country_group
     , budget_segment_value3 AS borders
     , budget_segment_value4 AS purchase_type
     , SUM(fixed_ad_spend) AS fixed_ad_spend_budget
     , SUM(breakage) AS breakage_budget
  FROM {{ source('dwh','fact_forecast') }}
 WHERE budget_config_id = (SELECT max(budget_config_id) FROM {{ source('dwh','fact_forecast') }} )
 GROUP BY 1,2,3,4,5
)

-- union budget  data
SELECT cm.*
     , channel_group.group_display AS channel_group
     , source_market.country_group AS country_group
     , CASE WHEN (coalesce(primary_location.country_name, dg.country_name, 'Other')) = source_market.country_name THEN 'Domestic'
                 WHEN coalesce(primary_location.continent_name, dg.continent_name)  = source_market.continent_name THEN 'Regional'
                 ELSE 'International'
             END AS borders -- need dg.country_name / continent_name as there's no tour_id for performance marketing cost
     , CASE WHEN purchase_type_id = 1 THEN 'Acquisition'
            WHEN purchase_type_id in (2,3) THEN 'Repeat'
       END AS purchase_type
     , CAST(NULL AS DOUBLE) AS fixed_ad_spend_budget
     , CAST(NULL AS DOUBLE) AS breakage_budget
  FROM union_all_components cm
  LEFT JOIN {{ source('dwh','dim_tour') }}   AS tour ON cm.tour_id = tour.tour_id
  LEFT JOIN {{ source('dwh','dim_location') }}   AS primary_location ON tour.location_id = primary_location.location_id
  LEFT JOIN {{ source('dwh','dim_country') }}   AS source_market ON cm.source_market_country_id = source_market.country_id
  LEFT JOIN {{ source('dwh','dim_attribution_channel_group') }}   AS channel_group ON cm.channel = channel_group.channel
  LEFT JOIN {{ source('dwh','dim_destination_group') }}  AS dg ON cm.destination_city_id = dg.city_id


UNION ALL

SELECT
       date AS checkout_date
     , date AS date_of_travel_berlin_time
     , CAST(NULL AS DATE) AS cancellation_date
     , CAST(NULL AS DATE) AS refund_date
     , CAST(NULL AS DATE) AS event_date
     , date AS actual_date_checkout
     , date AS actual_date_travel
     , CAST(NULL AS BIGINT) AS reseller_id
     , CAST(NULL AS DATE) AS customer_first_checkout_date
     , CAST(NULL AS STRING) AS channel
     , CAST(NULL AS BIGINT) AS purchase_type_id
     , CAST(NULL AS BIGINT) AS destination_city_id
     , CAST(NULL AS BIGINT) AS source_market_country_id
     , CAST(NULL AS BIGINT) AS tour_id
     , CAST(NULL AS BIGINT) AS tour_option_id
     , 'budget' AS cost_type
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
     , CAST(NULL AS DOUBLE) AS fixed_ad_spend
     , CAST(NULL AS DOUBLE) AS cancelation_compensation_amount_eur
     , CAST(NULL AS DOUBLE) AS manual_cancellation_adjustment_amount_eur
     , CAST(NULL AS DOUBLE) AS manual_commission_adjustment_amount_eur
     , channel_group
     , country_group
     , borders
     , purchase_type
     , fixed_ad_spend_budget
     , breakage_budget
  FROM contribution_margin_budget_upload
