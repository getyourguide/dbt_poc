{{ config(schema=var('reports')) }}

---
-- For 2022 revision of prio 1 reports we need to display top cities
-- which make 50% of NR per each area based on 2019 data
--
WITH nr_base AS (
SELECT COALESCE(sales_area, 'OTHER') sales_area, city_name, city_id
     , sum(nr) AS nr
  FROM {{ source('dwh', 'dim_location') }} l
  LEFT JOIN {{ source('dwh', 'fact_booking') }} b USING (location_id)
 WHERE b.status_id IN (1,2)
   AND YEAR(b.date_of_checkout) = 2022
 GROUP BY 1,2,3
 )
, area_nr AS (
SELECT sales_area, sum(nr) AS nr_area, row_number() OVER(ORDER BY sum(nr) DESC) AS area_order
  FROM nr_base
 GROUP BY 1
 )
, rolling AS (
SELECT sales_area, b.city_name, b.city_id
     , b.nr
     , a.nr_area
     , SUM(b.nr) OVER(PARTITION BY sales_area ORDER BY b.nr DESC) AS nr_rolling
     , SUM(b.nr) OVER(PARTITION BY sales_area ORDER BY b.nr DESC) / a.nr_area AS nr_pcnt_rolling
     , row_number() OVER(PARTITION BY sales_area ORDER BY b.nr DESC) AS order_num
     , a.area_order
  FROM nr_base b
 INNER JOIN area_nr a USING (sales_area)
)
SELECT sales_area, city_name, city_id, nr, nr_area, nr_rolling, nr_pcnt_rolling, order_num, area_order
     , nr_pcnt_rolling < 0.501 AS is_top_destination
     , CASE WHEN nr_pcnt_rolling < 0.501 THEN city_name ELSE 'Longtail' END AS city_name_display
  FROM rolling
