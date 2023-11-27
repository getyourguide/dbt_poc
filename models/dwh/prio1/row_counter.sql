{{ config(schema=var('reports')) }}




-- ******************************************************** --
-- ***  GOOGLE REPORTING  ********************************* --
-- ******************************************************** --
WITH attribution_touchpoints AS (
  SELECT
    date AS report_date
    , CONCAT("attribution_touchpoints.", LOWER(COALESCE(cost.cost_source, "<none>"))) AS source
    , MAP(
        "rows", COUNT(*)
        , "total_cost", SUM((cost.unit_cost + cost.remaining_cost) / cost_weight)
        , "total_visitors", COUNT(DISTINCT user.visitor_id)
        , "total_customers", COUNT(DISTINCT customer_id)
        , "total_nr", SUM(nr * markov_chains_weight)
      ) AS metrics
  FROM {{ source('default', 'fact_attribution') }}
  WHERE date BETWEEN "{{ var ('start-date') }}" AND "{{ var ('end-date') }}"
  GROUP BY 1, 2
)

, marketing_metrics AS (
  SELECT
    date AS report_date
    , "marketing_metrics" AS source
    , MAP(
      "nr", SUM(nr)
      , "ad_spend", SUM(ad_spend)
      , "reseller_costs", SUM(reseller_costs)
      , "coupon_costs", SUM(coupon_costs)
    ) AS metrics
  FROM {{ ref('agg_metrics_marketing') }}
  WHERE date BETWEEN "{{ var ('start-date') }}" AND "{{ var ('end-date') }}"
  GROUP BY 1, 2
)

SELECT
  report_date
  , source
  , EXPLODE(metrics) AS (metric, value)
FROM (
  SELECT * FROM attribution_touchpoints
  UNION ALL SELECT * FROM marketing_metrics
)