{{ config(schema=var('reports')) }}




WITH date_config AS (
       SELECT
         {% if is_incremental() %}
         DATE(DATE_TRUNC('QUARTER', '{{ var ('start-date') }}')) - INTERVAL 15 days AS start_date
         {% endif %}
         , a.date AS end_date
         {% if is_incremental() %}
         , DATE(DATE_TRUNC('QUARTER', '{{ var ('start-date') }}')) AS quarter_start_date
         {% endif %}
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
     ),

dates AS (
    SELECT DISTINCT
        *
        , DATE(DATE_TRUNC('QUARTER', date_id)) AS quarter
    FROM {{ source('default', 'dim_date') }}
    CROSS JOIN date_config ON date_id BETWEEN start_date AND end_date
    WHERE 1=1
    {% if target.name == 'dev' and not is_incremental() %}
    LIMIT 1
    {% endif %}
),

 agg_mkt_attr AS (
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
   ),

 attribution_channel_weights AS (
   SELECT
     shopping_cart_id,
     explode(channel_weights) as channel_weights
   FROM {{ source('default', 'fact_attribution_channel_weights') }}
   ),

 customers_n_bookings as (
   SELECT DISTINCT
   	     checkout_date AS date --TO_DATE(fact_booking.date_of_checkout) AS date
       , source_market.country_group AS country_group_source
       , b.borders --fact_booking.borders
       , CASE WHEN shopping_cart.purchase_type_id = 1 THEN true ELSE false END AS is_acquisition
       , group.group_display as channel
       -- TODO fix this, needs to be weighted by channel weight
       , COUNT(DISTINCT customer.customer_id ) AS customers
       -- This can probably fix the customer count, need to validate it, it's used in CM so it should work here
       -- to have QTD customers we will probably have to sum up quarterly_customers
       -- , SUM(COALESCE(wgh.channel_weight, 1) / daily_customer_bookings) AS daily_customers -- for getting unique customers on daily level
       -- , SUM(COALESCE(wgh.channel_weight, 1) / quarterly_customer_bookings) AS quarterly_customers -- for getting unique customers on quarterly level
       , SUM(fact_booking.nr  * COALESCE(wgh.channel_weight, 1)) AS nr
       , SUM(fact_booking.gmv * COALESCE(wgh.channel_weight, 1)) AS gmv
       , SUM(COALESCE(wgh.channel_weight, 1) / fact_booking.shopping_cart_bookings) AS transactions
       , SUM(COALESCE(wgh.channel_weight, 1)) AS bookings
   FROM {{ ref('stg_contribution_margin_booking_base') }} AS fact_booking
   INNER JOIN {{ source('dwh', 'fact_booking') }} AS b ON fact_booking.booking_id = b.booking_id
   FULL OUTER JOIN {{ source('dwh', 'dim_tour') }} AS tour ON fact_booking.tour_id  = tour.tour_id
   LEFT JOIN {{ source('dwh', 'dim_location') }} AS primary_location ON tour.location_id  = primary_location.location_id
   LEFT JOIN {{ source('dwh', 'dim_customer') }} AS customer ON fact_booking.customer_id  = customer.customer_id
   LEFT JOIN {{ source('dwh', 'dim_country') }} AS source_market ON fact_booking.source_market_country_id  = source_market.country_id --customer_country_id
   LEFT JOIN {{ source('dwh', 'fact_shopping_cart') }} AS shopping_cart ON fact_booking.shopping_cart_id  = shopping_cart.shopping_cart_id
   --LEFT JOIN attribution_channel_weights AS cw ON fact_booking.shopping_cart_id = cw.shopping_cart_id
   LEFT JOIN {{ source('default', 'agg_attribution_channel_weights') }} AS wgh ON fact_booking.shopping_cart_id = wgh.shopping_cart_id
   LEFT JOIN {{ source('dwh', 'dim_attribution_channel_group') }} AS group ON group.channel = wgh.channel -- cw.channel_weights.channel
   JOIN dates ON dates.date_id = checkout_date --TO_DATE(fact_booking.date_of_checkout)
   WHERE 1=1
   AND fact_booking.status_id IN (1,2)
   GROUP BY 1,2,3,4,5
   ),

 agg_customers AS (
   SELECT DISTINCT
     	TO_DATE(fact_booking.date_of_checkout) AS date
         , source_market.country_group AS country_group_source
         , fact_booking.borders
         , CASE WHEN shopping_cart.purchase_type_id = 1 THEN true ELSE false END AS is_acquisition
         , group.group_display as channel
         -- TODO fix this, needs to be weighted by channel weight
         , size(collect_set(fact_booking.customer_id) OVER w_c_qtd) AS customers_qtd
     FROM {{ source('dwh', 'fact_booking') }} AS fact_booking
     FULL OUTER JOIN {{ source('dwh', 'dim_tour') }} AS tour ON fact_booking.tour_id  = tour.tour_id
     LEFT JOIN {{ source('dwh', 'dim_location') }} AS primary_location ON tour.location_id  = primary_location.location_id
     LEFT JOIN {{ source('dwh', 'dim_customer') }} AS customer ON fact_booking.customer_id  = customer.customer_id
     LEFT JOIN {{ source('dwh', 'dim_country') }} AS source_market ON fact_booking.customer_country_id  = source_market.country_id
     LEFT JOIN {{ source('dwh', 'fact_shopping_cart') }} AS shopping_cart ON fact_booking.shopping_cart_id  = shopping_cart.shopping_cart_id
     LEFT JOIN attribution_channel_weights ON fact_booking.shopping_cart_id = attribution_channel_weights.shopping_cart_id
     LEFT JOIN {{ source('dwh', 'dim_attribution_channel_group') }} AS group ON group.channel = attribution_channel_weights.channel_weights.channel
     JOIN dates ON dates.date_id = TO_DATE(fact_booking.date_of_checkout)
     WHERE 1=1
     AND status_id IN (1,2)

     WINDOW w_c_qtd AS (PARTITION BY quarter, country_group, borders, CASE WHEN shopping_cart.purchase_type_id = 1  THEN 'Yes' ELSE 'No' END, group.group_display ORDER BY date_id)
 ),

 summary as (
 SELECT
     COALESCE(a.date, c.date) AS date
     , COALESCE(a.country_group_source, c.country_group_source) AS country_group_source
     , COALESCE(a.borders, c.borders) AS borders
     , COALESCE(a.channel, c.channel) AS channel
     , COALESCE(a.is_acquisition, c.is_acquisition) AS is_acquisition

     , a.ad_spend
     , SUM(a.ad_spend) OVER w_qtd AS ad_spend_qtd
     , a.reseller_costs
     , SUM(a.reseller_costs) OVER w_qtd AS reseller_costs_qtd
     , a.coupon_costs
     , SUM(a.coupon_costs) OVER w_qtd AS coupon_costs_qtd
     , ROUND(c.bookings, 2) bookings
     , ROUND( (SUM(c.bookings) OVER w_qtd) , 2) AS bookings_qtd
     , c.customers
     , ac.customers_qtd
     , c.gmv
     , SUM(c.gmv) OVER w_qtd AS gmv_qtd
     , c.nr
     , SUM(c.nr) OVER w_qtd AS nr_qtd
     --, c.transactions
     --, SUM(c.transactions) OVER w_qtd AS transactions_qtd
     ---
     --, a.bookings AS bookings_m
     --, SUM(a.bookings) OVER w_qtd AS bookings_qtd_m
     --, a.gmv AS gmv_m
     --, SUM(c.gmv) OVER w_qtd AS gmv_qtd_m
     --, a.nr AS nr_m
     --, SUM(c.nr) OVER w_qtd AS nr_qtd_m
     , a.transactions AS transactions
     , SUM(a.transactions) OVER w_qtd AS transactions_qtd
 FROM agg_mkt_attr a
 FULL OUTER JOIN customers_n_bookings c
     ON  c.borders = a.borders
     AND c.date = a.date
     AND c.country_group_source = a.country_group_source
     AND c.channel = a.channel
     AND c.is_acquisition = a.is_acquisition
 LEFT JOIN agg_customers ac
     ON  c.borders = ac.borders
     AND c.date = ac.date
     AND c.country_group_source = ac.country_group_source
     AND c.channel = ac.channel
     AND c.is_acquisition = ac.is_acquisition
 WHERE 1=1

 WINDOW w_qtd AS (PARTITION BY
     DATE(DATE_TRUNC('QUARTER', COALESCE(a.date, c.date)))
     , COALESCE(a.country_group_source, c.country_group_source)
     , COALESCE(a.borders, c.borders)
     , COALESCE(a.channel, c.channel)
     , COALESCE(a.is_acquisition, c.is_acquisition)
     ORDER BY COALESCE(a.date, c.date))

 )

 SELECT summary.* FROM summary
 CROSS JOIN date_config ON date BETWEEN quarter_start_date AND end_date
