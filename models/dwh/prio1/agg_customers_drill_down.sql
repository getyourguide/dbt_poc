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

, agg_customers AS (
  SELECT DISTINCT
   	     checkout_date AS date
       , fact_booking.country_group_source
       , fact_booking.borders
       , CASE WHEN fact_booking.purchase_type_id = 1 THEN true ELSE false END AS is_acquisition
       , group.group_display as channel
       -- TODO fix this, needs to be weighted by channel weight
       , size(collect_set(fact_booking.customer_id) OVER w_c_qtd) AS customers_qtd
    FROM {{ ref('stg_contribution_margin_booking_base') }} AS fact_booking
   LEFT JOIN {{ source('default', 'agg_attribution_channel_weights') }} AS wgh ON fact_booking.shopping_cart_id = wgh.shopping_cart_id
   LEFT JOIN {{ source('dwh', 'dim_attribution_channel_group') }} AS group ON group.channel = wgh.channel -- cw.channel_weights.channel    
    JOIN dates ON dates.date_id = fact_booking.checkout_date
   WHERE 1=1
     AND status_id IN (1,2)

  WINDOW w_c_qtd AS (PARTITION BY quarter, country_group_source, borders, CASE WHEN fact_booking.purchase_type_id = 1  THEN 'Yes' ELSE 'No' END, group.group_display ORDER BY date_id)
 )

 SELECT a.*
   FROM agg_customers a
  CROSS JOIN date_config ON date BETWEEN quarter_start_date AND end_date
