{{ config(schema=var('reports')) }}




WITH date_config AS (
       SELECT
         {% if is_incremental() %}
         -- fixed Ad Spend for CM can change 3 months retroactivelly
         -- to always cover that period and have correct QTD window
         -- we need to process 2 quarters
         DATE_TRUNC('QUARTER', DATE('{{ var ('start-date') }}')) - INTERVAL 3 MONTHS AS start_date
         {% endif %}
         , a.date AS end_date
       FROM (SELECT
                {% if is_incremental() %}
                '{{ var ('end-date') }}'
                {% endif %}
                {% if target.name == 'dev' and not is_incremental() %}
                CURRENT_DATE()
                {% endif %}
                {% if target.name != 'dev' and not is_incremental() %}
                '2020-03-31'
                {% endif %}
                AS date
       ) a
     )

, dates AS (
    SELECT DISTINCT
        *
        , DATE(DATE_TRUNC('QUARTER', date_id)) AS quarter
    FROM {{ source('default', 'dim_date') }}
    CROSS JOIN date_config ON date_id BETWEEN start_date AND end_date
    WHERE 1=1
    {% if target.name == 'dev' and not is_incremental() %}
    LIMIT 1
    {% endif %}
)

, cm_base AS (
  SELECT date_id AS date
       , channel_group.GROUP
       , cm.BORDERS
       , cm.COUNTRY_GROUP
       , cm.PURCHASE_TYPE
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
      , COALESCE(SUM(-cm.RESELLER_COST_ACTIVE ), 0) + COALESCE(SUM(-cm.RESELLER_COST_CANCELLED ), 0) + COALESCE(SUM(-cm.MARKETING_COUPON_COST ), 0) + COALESCE(SUM(CASE WHEN channel_group.is_roas_relevant THEN -cm.AD_SPEND END ), 0) AS realtime_ad_spend
      , COALESCE(SUM(CASE WHEN channel_group.is_roas_relevant THEN -cm.AD_SPEND END ), 0) AS ad_spend
    FROM dates  
    LEFT JOIN {{ ref('agg_contribution_margin') }} cm ON dates.date_id = date(cm.actual_date_checkout)
    LEFT JOIN {{ source('dwh', 'dim_attribution_channel_group') }} AS channel_group ON (cm.CHANNEL) = channel_group.channel
   CROSS JOIN (SELECT date_add(add_months(date_trunc('month', MAX(month)), 1), -1) AS ad_fix_date_max FROM {{ source('default', 'contribution_margin_fixed_adspend_upload') }}) AS ad_fix_up
   CROSS JOIN (SELECT date_add(add_months(date_trunc('month', MAX(date)), 1), -1) AS brkg_up_date_max FROM {{ source('default', 'cost_allocation_breakage_upload') }}) AS brkg_up
   GROUP BY 1,2,3,4,5
)

, cm AS (
  SELECT date
       , GROUP
       , BORDERS
       , COUNTRY_GROUP
       , PURCHASE_TYPE
       , nr_components + payment_costs_components + ad_spend_components + ticket_cost_tt_adjusted + breakage AS contribution_margin_checkout
       , nr_components
       , fixed_ad_spend
       , realtime_ad_spend
       , ad_spend
    FROM cm_base
)

, summary AS (
   SELECT
       cm.date
     , cm.GROUP AS channel
     , cm.BORDERS
     , cm.COUNTRY_GROUP AS country_group_source
     , cm.PURCHASE_TYPE
     , (cm.PURCHASE_TYPE = 'Acquisition') AS is_acquisition

     , cm.contribution_margin_checkout
     , SUM(cm.contribution_margin_checkout) OVER w_qtd AS contribution_margin_checkout_qtd
     , cm.nr_components
     , SUM(cm.nr_components) OVER w_qtd                AS nr_components_qtd 
     , cm.fixed_ad_spend
     , SUM(cm.fixed_ad_spend) OVER w_qtd               AS fixed_ad_spend_qtd
     , cm.realtime_ad_spend
     , SUM(cm.realtime_ad_spend) OVER w_qtd            AS realtime_ad_spend_qtd 
     , cm.ad_spend
     , SUM(cm.ad_spend) OVER w_qtd                     AS ad_spend_qtd 
   FROM cm
   LEFT JOIN dates ON cm.date = dates.date_id
   
WINDOW w_qtd AS (PARTITION BY
       quarter
     , cm.GROUP
     , cm.BORDERS
     , cm.COUNTRY_GROUP
     , cm.PURCHASE_TYPE
     ORDER BY cm.date )
)

 SELECT
      summary.*
 FROM summary
