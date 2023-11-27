{{ config(schema=var('reports')) }}




WITH date_config AS (
       SELECT
         DATE(DATE_TRUNC('QUARTER', a.date)) - INTERVAL 15 days AS start_date
         , a.end_date AS end_date
         , DATE(DATE_TRUNC('QUARTER', a.date)) AS quarter_start_date
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
    FROM {{ source('default', 'dim_date') }}
    CROSS JOIN date_config ON date_id BETWEEN start_date AND end_date
    WHERE 1=1
    {% if target.name == 'dev' and not is_incremental() %}
    LIMIT 1
    {% endif %}
),

summary as (
SELECT
	TO_DATE(fact_booking.date_of_checkout) AS date
    , tour.category  AS tour_category
    , primary_location.sales_area
    , CASE
        WHEN primary_location.top_250_destination_rank < 51 THEN 'Top 50'
        WHEN primary_location.top_250_destination_rank BETWEEN 51 AND 250 THEN '51-250'
        ELSE 'Longtail'
      END AS top_250_destination_rank_groupings
    , CASE WHEN supplier.is_gyg_supplier=1 THEN 'T&T' ELSE 'Marketplace' END AS supplier_is_gyg_supplier
    , SUM(fact_booking.nr) AS nr
    , SUM(fact_booking.gmv) AS gmv
    , COUNT(DISTINCT booking_id) AS bookings
FROM {{ source('dwh', 'fact_booking') }} AS fact_booking
FULL OUTER JOIN {{ source('dwh', 'dim_tour') }} AS tour ON fact_booking.tour_id  = tour.tour_id
LEFT JOIN {{ source('dwh', 'dim_location') }} AS primary_location ON tour.location_id  = primary_location.location_id
LEFT JOIN {{ source('dwh', 'dim_user') }} AS supplier ON tour.user_id  = supplier.user_id
JOIN dates ON dates.date_id = TO_DATE(fact_booking.date_of_checkout)
WHERE 1=1
AND status_id IN (1,2)
GROUP BY TO_DATE(fact_booking.date_of_checkout ),2,3,4,5
ORDER BY 1 DESC
)

SELECT
    date
    , tour_category
    , sales_area
    , top_250_destination_rank_groupings
    , supplier_is_gyg_supplier
    , nr
    , SUM(nr) OVER w_qtd AS nr_qtd
    , gmv
    , SUM(gmv) OVER w_qtd AS gmv_qtd
    , bookings
FROM summary
{% if is_incremental() %}
CROSS JOIN date_config ON date BETWEEN quarter_start_date AND end_date
{% endif %}

WINDOW w_qtd AS (PARTITION BY
        DATE(DATE_TRUNC('QUARTER', date)),
        tour_category,
        sales_area,
        top_250_destination_rank_groupings,
        supplier_is_gyg_supplier
        ORDER BY date)
