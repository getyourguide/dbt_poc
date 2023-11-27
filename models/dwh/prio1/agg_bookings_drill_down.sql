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

, customers_n_bookings as (
   SELECT DISTINCT
   	     checkout_date AS date
       , fact_booking.country_group_source
       , fact_booking.borders
       , CASE WHEN fact_booking.purchase_type_id = 1 THEN true ELSE false END AS is_acquisition
       , group.group_display as channel
       -- TODO fix this, needs to be weighted by channel weight
       , COUNT(DISTINCT fact_booking.customer_id ) AS customers
       -- This can probably fix the customer count, need to validate it, it's used in CM so it should work here
       -- to have QTD customers we will probably have to sum up quarterly_customers
       , SUM(COALESCE(wgh.channel_weight, 1) / daily_customer_bookings) AS daily_customers -- for getting unique customers on daily level
       , SUM(COALESCE(wgh.channel_weight, 1) / quarterly_customer_bookings) AS quarterly_customers -- for getting unique customers on quarterly level
       , SUM(fact_booking.nr  * COALESCE(wgh.channel_weight, 1)) AS nr
       , SUM(fact_booking.gmv * COALESCE(wgh.channel_weight, 1)) AS gmv
       , SUM(COALESCE(wgh.channel_weight, 1) / fact_booking.shopping_cart_bookings) AS transactions
       , SUM(COALESCE(wgh.channel_weight, 1)) AS bookings
   FROM {{ ref('stg_contribution_margin_booking_base') }} AS fact_booking
   --FULL OUTER JOIN {{ source('dwh', 'dim_tour') }} AS tour ON fact_booking.tour_id  = tour.tour_id
   --LEFT JOIN {{ source('dwh', 'dim_location') }} AS primary_location ON tour.location_id  = primary_location.location_id
   LEFT JOIN {{ source('default', 'agg_attribution_channel_weights') }} AS wgh ON fact_booking.shopping_cart_id = wgh.shopping_cart_id
   LEFT JOIN {{ source('dwh', 'dim_attribution_channel_group') }} AS group ON group.channel = wgh.channel -- cw.channel_weights.channel
   JOIN dates ON dates.date_id = checkout_date --TO_DATE(fact_booking.date_of_checkout)
   WHERE 1=1
   --AND fact_booking.status_id IN (1,2) -- already filter in stg_contribution_margin_booking_base
   GROUP BY 1,2,3,4,5
   )

, summary as (
  SELECT
         c.date
       , c.country_group_source
       , c.borders
       , c.channel
       , c.is_acquisition

       , ROUND(c.bookings, 2) bookings
       , ROUND( (SUM(c.bookings) OVER w_qtd) , 2) AS bookings_qtd
       , c.customers
       , c.daily_customers
       , c.quarterly_customers
       , c.gmv
       , SUM(c.gmv) OVER w_qtd AS gmv_qtd
       , c.nr
       , SUM(c.nr) OVER w_qtd AS nr_qtd
       , c.transactions
       , SUM(c.transactions) OVER w_qtd AS transactions_qtd
    FROM customers_n_bookings c
   WHERE 1=1
  WINDOW w_qtd AS (PARTITION BY
         DATE(DATE_TRUNC('QUARTER', c.date))
       , c.country_group_source
       , c.borders
       , c.channel
       , c.is_acquisition
       )
   ORDER BY 1
)

SELECT summary.*
  FROM summary
 CROSS JOIN date_config ON date BETWEEN quarter_start_date AND end_date
