{{ config(schema=var('default')) }}




-- This is the transformation by using marketing.fact_touchpoint_source_market as the source for source market cost.
-- The main part of the query is to allocate the null destination cost, which cannot be performed in fact_touchpoint_source_market
-- ETL due to performance issue and large output.

WITH customer_first_checkout AS (
  SELECT
    customer_id
    , first_date_of_checkout
  FROM {{ source('dwh', 'dim_transaction_cohort') }}
  WHERE customer_id > 0
  GROUP BY 1, 2
)
,
touchpoint_base AS (
SELECT *
FROM {{ source('marketing', 'dim_touchpoint') }}
WHERE 1=1
{% if target.name == 'dev' and not is_incremental() %}
  AND date = '2022-01-01'
{% endif %}
{% if is_incremental() %}
  AND date BETWEEN '{{ var ('source-market-cost-allocation-start-date') }}' AND '{{ var ('cost-allocation-marketing-end-date') }}'
{% endif %}
)
,
touchpoint_source_market_base AS (
SELECT *
FROM {{ source('marketing', 'fact_touchpoint_source_market') }}
WHERE 1=1
{% if target.name == 'dev' and not is_incremental() %}
  AND date = '2022-01-01'
{% endif %}
{% if is_incremental() %}
  AND date BETWEEN '{{ var ('source-market-cost-allocation-start-date') }}' AND '{{ var ('cost-allocation-marketing-end-date') }}'
{% endif %}
)
,
touchpoint_info AS (
SELECT a.date
, a.touchpoint_id
, a.customer_id
, a.channel
, NULLIF(b.city_id, 0) AS destination_city_id
, cf.first_date_of_checkout
, pt.purchase_type_name AS purchase_type
FROM touchpoint_base a
LEFT JOIN {{ source('dwh', 'dim_location') }} b ON a.location_id = b.location_id
LEFT JOIN customer_first_checkout cf ON a.customer_id = cf.customer_id
LEFT JOIN {{ source('dwh', 'dim_purchase_type') }} pt ON a.purchase_type_id = pt.purchase_type_id
)
, allocated_cost AS (
SELECT fs.date
       , fs.source_market_country_id
       , fc.country_group AS source_market_country_group
       , md.destination_city_id
       , md.first_date_of_checkout
       , md.customer_id
       , md.purchase_type
       , md.channel
       , sum(fs.cost) AS cost_estimated
FROM touchpoint_source_market_base fs
LEFT JOIN touchpoint_info md ON fs.touchpoint_id = md.touchpoint_id AND fs.date = md.date
LEFT JOIN {{ source('dwh', 'dim_country') }} fc ON fs.source_market_country_id = fc.country_id
GROUP BY 1,2,3,4,5,6,7,8
)
, daily_no_destination_cost AS (
SELECT date
       , source_market_country_group
       , purchase_type
       , channel
       , sum(cost_estimated) AS cost_estimated
FROM allocated_cost
WHERE destination_city_id IS NULL
GROUP BY 1, 2, 3, 4
)
, daily_no_destination_cost_allocated AS (
SELECT a.date
       , a.source_market_country_id
       , a.source_market_country_group
       , a.destination_city_id
       , a.first_date_of_checkout
       , a.customer_id
       , a.purchase_type
       , a.channel
       , a.cost_estimated + coalesce(b.cost_estimated, 0) * coalesce((a.cost_estimated / sum(a.cost_estimated) OVER (PARTITION BY a.date, a.source_market_country_group, a.purchase_type, a.channel)), 1) AS cost_estimated
   FROM
     (SELECT *
      FROM allocated_cost
      WHERE destination_city_id IS NOT NULL) a
   LEFT JOIN daily_no_destination_cost b ON a.date = b.date
   AND a.source_market_country_group = b.source_market_country_group
   AND a.purchase_type = b.purchase_type
   AND a.channel= b.channel
)
-- It could happen that there's still a few cost leftover that cannot be allocated at not_indestination_cost_allocated step (no matching),
-- following logic is to capture that cost.
, daily_no_destination_cost_unallocated AS (
SELECT a.date
       , a.source_market_country_id
       , a.source_market_country_group
       , a.destination_city_id
       , a.first_date_of_checkout
       , a.customer_id
       , a.purchase_type
       , a.channel
       , a.cost_estimated
   FROM
     (SELECT *
      FROM allocated_cost
      WHERE destination_city_id IS NULL) a
   LEFT anti JOIN
     (SELECT DISTINCT date
             , source_market_country_group
             , purchase_type
             , channel
      FROM allocated_cost
      WHERE destination_city_id IS NOT NULL) b ON a.date = b.date
   AND a.source_market_country_group = b.source_market_country_group
   AND a.purchase_type = b.purchase_type
   AND a.channel= b.channel
)

, daily_destination_cost AS (
SELECT date
       , source_market_country_group
       , destination_city_id
       , sum(cost_estimated) AS cost_estimated
FROM daily_no_destination_cost_allocated
GROUP BY 1, 2, 3
)
, daily_destination_weight AS (
SELECT date as date_temp
       , source_market_country_group AS source_market_country_group_temp -- resolve attribute error in spark2
       , destination_city_id as destination_city_id_temp
       , cost_estimated / sum(cost_estimated) OVER (PARTITION BY date, source_market_country_group) AS destination_weight
 FROM daily_destination_cost
 )
,
unioned_cost AS (
SELECT a.date
       , a.source_market_country_id
       , b.destination_city_id_temp as destination_city_id
       , a.first_date_of_checkout
       , a.customer_id
       , a.purchase_type
       , a.channel
       , a.cost_estimated * coalesce(b.destination_weight, 1) AS total_estimated_cost
FROM daily_no_destination_cost_unallocated a
LEFT JOIN daily_destination_weight b ON a.date = b.date_temp AND a.source_market_country_group = b.source_market_country_group_temp
UNION ALL
SELECT a.date
       , a.source_market_country_id
       , a.destination_city_id
       , a.first_date_of_checkout
       , a.customer_id
       , a.purchase_type
       , a.channel
       , a.cost_estimated AS total_estimated_cost
FROM daily_no_destination_cost_allocated a
)

SELECT date
       , source_market_country_id
       , destination_city_id
       , COALESCE(first_date_of_checkout, date) AS first_date_of_checkout
       , customer_id
       , purchase_type
       , channel
       , sum(total_estimated_cost) AS total_estimated_cost
FROM unioned_cost
GROUP BY 1,2,3,4,5,6,7