{{ config(schema=var('reports')) }}

WITH dates AS (
  SELECT  base.date_id AS date
        , base.yoy_date_id AS yoy_date
        , next.yoy_date_id AS yo2y_date
        , y3.yoy_date_id AS yo3y_date
     FROM {{ source('public', 'dim_date_deprecated') }}  base
     LEFT JOIN {{ source('public', 'dim_date_deprecated') }} next ON base.yoy_date_id = next.date_id
     LEFT JOIN {{ source('public', 'dim_date_deprecated') }} y3   ON next.yoy_date_id = y3.date_id
    WHERE base.date_id BETWEEN '2019-01-01' AND CURRENT_DATE
)
, metrics_sales_prep AS (
  SELECT date
       , tour_category
       , sales_area
       , top_250_destination_rank_groupings
       , supplier_is_gyg_supplier
       , nr
       , nr_qtd
       , gmv
       , gmv_qtd
       , bookings
    FROM {{ ref('agg_metrics_sales') }}
)

, dims as (
  SELECT DISTINCT dates.date
       , yoy_date
       , yo2y_date
       , yo3y_date
       , ms.tour_category
       , ms.sales_area
       , ms.top_250_destination_rank_groupings
       , ms.supplier_is_gyg_supplier
    FROM metrics_sales_prep ms
   CROSS JOIN dates
)

, metrics_sales AS (
  SELECT dims.date
       , dims.tour_category
       , dims.sales_area
       , dims.top_250_destination_rank_groupings
       , dims.supplier_is_gyg_supplier
       , nr
       , nr_qtd
       , gmv
       , gmv_qtd
       , bookings
       
       , SUM(nr)       OVER(wow) AS nr_last_week
       , SUM(gmv)      OVER(wow) AS gmv_last_week
       , SUM(bookings) OVER(wow) AS bookings_last_week
  FROM dims
  LEFT JOIN metrics_sales_prep  AS base
         ON dims.date = base.date
        AND COALESCE(dims.tour_category, 'key')                             = COALESCE(base.tour_category, 'key')
        AND COALESCE(dims.sales_area, 'key')                                = COALESCE(base.sales_area, 'key')
        AND COALESCE(dims.top_250_destination_rank_groupings, 'key')        = COALESCE(base.top_250_destination_rank_groupings, 'key')
        AND COALESCE(CAST(dims.supplier_is_gyg_supplier  AS string), 'key') = COALESCE(CAST(base.supplier_is_gyg_supplier AS string), 'key')
        
WINDOW wow AS ( PARTITION BY dims.tour_category, dims.sales_area, dims.top_250_destination_rank_groupings, dims.supplier_is_gyg_supplier ORDER BY dims.date ROWS BETWEEN  7 PRECEDING AND 7 PRECEDING)

)

SELECT COALESCE(dims.date, ly.date, py.date) AS report_date
     , dims.tour_category
     , dims.sales_area
     , dims.top_250_destination_rank_groupings
     , dims.supplier_is_gyg_supplier

     , base.nr
     , base.nr_qtd
     , base.gmv
     , base.gmv_qtd
     , base.bookings
     
     , base.nr_last_week
     , base.gmv_last_week
     , base.bookings_last_week


     , ly.nr       AS nr_last_year
     , ly.nr_qtd   AS nr_qtd_last_year
     , ly.gmv      AS gmv_last_year
     , ly.gmv_qtd  AS gmv_qtd_last_year
     , ly.bookings AS bookings_last_year

     , ly.nr_last_week       AS nr_last_week_last_year
     , ly.gmv_last_week      AS gmv_last_week_last_year
     , ly.bookings_last_week AS bookings_last_week_last_year

     , py.nr       AS nr_penultimate_year
     , py.nr_qtd   AS nr_qtd_penultimate_year
     , py.gmv      AS gmv_penultimate_year
     , py.gmv_qtd  AS gmv_qtd_penultimate_year
     , py.bookings AS bookings_penultimate_year

     , y3.nr       AS nr_3_years_ago
     , y3.nr_qtd   AS nr_qtd_3_years_ago
     , y3.gmv      AS gmv_3_years_ago
     , y3.gmv_qtd  AS gmv_qtd_3_years_ago
     , y3.bookings AS bookings_3_years_ago
  FROM dims
  LEFT JOIN metrics_sales AS base
         ON dims.date = base.date
        AND dims.tour_category = base.tour_category
        AND dims.sales_area = base.sales_area
        AND dims.top_250_destination_rank_groupings = base.top_250_destination_rank_groupings
        AND dims.supplier_is_gyg_supplier = base.supplier_is_gyg_supplier
  LEFT JOIN metrics_sales AS ly
         ON dims.yoy_date = ly.date
        AND dims.tour_category = ly.tour_category
        AND dims.sales_area = ly.sales_area
        AND dims.top_250_destination_rank_groupings = ly.top_250_destination_rank_groupings
        AND dims.supplier_is_gyg_supplier = ly.supplier_is_gyg_supplier
  LEFT JOIN metrics_sales AS py
         ON dims.yo2y_date = py.date
        AND dims.tour_category = py.tour_category
        AND dims.sales_area = py.sales_area
        AND dims.top_250_destination_rank_groupings = py.top_250_destination_rank_groupings
        AND dims.supplier_is_gyg_supplier = py.supplier_is_gyg_supplier
  LEFT JOIN metrics_sales AS y3
         ON dims.yo3y_date = y3.date
        AND dims.tour_category = y3.tour_category
        AND dims.sales_area = y3.sales_area
        AND dims.top_250_destination_rank_groupings = y3.top_250_destination_rank_groupings
        AND dims.supplier_is_gyg_supplier = y3.supplier_is_gyg_supplier
