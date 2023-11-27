{{ config(schema=var('reports')) }}




WITH date_config AS (
 SELECT DATE(DATE_TRUNC('QUARTER', start_date)) - INTERVAL 15 days AS start_date
      , a.date AS end_date
      , DATE(DATE_TRUNC('QUARTER', start_date)) AS quarter_start_date
 FROM 
    (SELECT {% if is_incremental() %}
            '{{ var ('start-date') }}'
            {% endif %}
            {% if target.name == 'dev' and not is_incremental() %}
            CURRENT_DATE()
            {% endif %}
            {% if target.name != 'dev' and not is_incremental() %}
            '2020-03-31'
            {% endif %}
            AS start_date
          ,
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
 SELECT DISTINCT *
      , DATE(DATE_TRUNC('QUARTER', date_id)) AS quarter
   FROM {{ source('default', 'dim_date') }}
  CROSS JOIN date_config ON date_id BETWEEN start_date AND end_date
  WHERE 1=1
  {% if target.name == 'dev' and not is_incremental() %}
  LIMIT 1
  {% endif %}
)

, agg_mkt_attr AS (
     SELECT
       date(contr_marg.checkout_date) as date
       , group.group_display as channel
       , country.country_group AS country_group_source
       , borders
       , purchase_type = 'Acquisition' as is_acquisition
       --, SUM(gmv_markov_checkout_date) AS gmv
       --, SUM(nr_markov_checkout_date) AS nr
       , SUM(CASE WHEN group.is_roas_relevant THEN ad_spend END) AS ad_spend
       , SUM(coalesce(reseller_cost_active,0) + coalesce(reseller_cost_cancelled,0)) AS reseller_costs
       , SUM(marketing_coupon_cost) AS coupon_costs
       -- TODO fix this, needs to be weighted by channel weight need to double check the solution in customers_n_bookings
       , SUM(transactions) AS transactions
       --, SUM(bookings_markov_by_checkout_date) AS bookings
     FROM {{ ref('agg_contribution_margin') }} AS contr_marg
     LEFT JOIN {{ source('dwh', 'dim_country') }} AS country ON country.country_id = contr_marg.source_market_country_id
     LEFT JOIN {{ source('dwh', 'dim_attribution_channel_group') }} AS group ON group.channel = contr_marg.channel
     JOIN dates ON dates.date_id = date(contr_marg.checkout_date)
     GROUP BY 1,2,3,4,5
)

, summary as (
 SELECT
       a.date
     , a.country_group_source
     , a.borders
     , a.channel
     , a.is_acquisition

     , a.ad_spend
     , SUM(a.ad_spend) OVER w_qtd AS ad_spend_qtd
     , a.reseller_costs
     , SUM(a.reseller_costs) OVER w_qtd AS reseller_costs_qtd
     , a.coupon_costs
     , SUM(a.coupon_costs) OVER w_qtd AS coupon_costs_qtd
     , a.transactions AS transactions
     , SUM(a.transactions) OVER w_qtd AS transactions_qtd
 FROM agg_mkt_attr a
 WHERE 1=1

 WINDOW w_qtd AS (PARTITION BY
       DATE(DATE_TRUNC('QUARTER', a.date))
     , a.country_group_source
     , a.borders
     , a.channel
     , a.is_acquisition
     ORDER BY a.date)

 )

 SELECT summary.* FROM summary
 CROSS JOIN date_config ON date BETWEEN quarter_start_date AND end_date
