{{ config(schema=var('reports')) }}



WITH metrics_global AS (
 SELECT date
      , g.BOOKINGS , g.BOOKINGS_LAST_7_DAYS , g.BOOKINGS_LAST_14_DAYS , g.BOOKINGS_QTD , g.BOOKINGS_TRIP_REPEAT , g.BOOKINGS_TRIP_REPEAT_LAST_7_DAYS , g.BOOKINGS_TRIP_REPEAT_LAST_14_DAYS , g.BOOKINGS_TRIP_REPEAT_QTD
      , g.NR_TRIP_REPEAT , g.NR_TRIP_REPEAT_LAST_7_DAYS , g.NR_TRIP_REPEAT_LAST_14_DAYS , g.NR_TRIP_REPEAT_QTD , g.NR , g.NR_LAST_7_DAYS , g.NR_LAST_14_DAYS , g.NR_QTD
      , g.NR_PAID_SEARCH , g.NR_PAID_SEARCH_LAST_7_DAYS , g.NR_PAID_SEARCH_LAST_14_DAYS , g.NR_PAID_SEARCH_QTD , g.NR_NON_PAID_CHANNELS , g.NR_NON_PAID_CHANNELS_LAST_7_DAYS , g.NR_NON_PAID_CHANNELS_LAST_14_DAYS , g.NR_NON_PAID_CHANNELS_QTD
      , g.NR_PERFORMANCE_MARKETING , g.NR_PERFORMANCE_MARKETING_LAST_7_DAYS , g.NR_PERFORMANCE_MARKETING_LAST_14_DAYS , g.NR_PERFORMANCE_MARKETING_QTD , g.NR_STR , g.NR_STR_LAST_7_DAYS , g.NR_STR_LAST_14_DAYS , g.NR_STR_QTD

      , g.GMV , g.GMV_LAST_7_DAYS , g.GMV_LAST_14_DAYS , g.GMV_QTD
      , g.ROAS_PERFORMANCE_MARKETING , g.ROAS_PERFORMANCE_MARKETING_LAST_7_DAYS , g.ROAS_PERFORMANCE_MARKETING_LAST_14_DAYS , g.ROAS_PERFORMANCE_MARKETING_QTD
      , g.ROAS_PAID_SEARCH , g.ROAS_PAID_SEARCH_LAST_7_DAYS , g.ROAS_PAID_SEARCH_LAST_14_DAYS , g.ROAS_PAID_SEARCH_QTD
      --
      , g.TRANSACTIONS , g.TRANSACTIONS_LAST_7_DAYS , g.TRANSACTIONS_LAST_14_DAYS , g.TRANSACTIONS_QTD , g.GUIDED_TOURS_ORIGINALS_NR_SHARE , g.GUIDED_TOURS_ORIGINALS_NR_SHARE_LAST_7_DAYS , g.GUIDED_TOURS_ORIGINALS_NR_SHARE_LAST_14_DAYS , g.GUIDED_TOURS_ORIGINALS_NR_SHARE_QTD
      , g.GUIDED_TOURS_ORIGINALS_EXPERIENCE_SHARE , g.GUIDED_TOURS_ORIGINALS_EXPERIENCE_SHARE_LAST_7_DAYS , g.GUIDED_TOURS_ORIGINALS_EXPERIENCE_SHARE_LAST_14_DAYS , g.GUIDED_TOURS_ORIGINALS_EXPERIENCE_SHARE_QTD , g.VISITORS , g.VISITORS_LAST_7_DAYS , g.VISITORS_PREV_7_DAYS , g.VISITORS_LAST_14_DAYS , g.VISITORS_QTD
      , g.QUOTERS , g.QUOTERS_LAST_7_DAYS , g.QUOTERS_PREV_7_DAYS , g.QUOTERS_LAST_14_DAYS , g.QUOTERS_QTD , g.CUSTOMERS , g.CUSTOMERS_LAST_7_DAYS, g.CUSTOMERS_PREV_7_DAYS , g.CUSTOMERS_LAST_14_DAYS , g.CUSTOMERS_QTD
      , g.nr_app -- more nr app columns exist in the table
      --, g.ROAS_REAL_TIME , g.ROAS_REAL_TIME_LAST_7_DAYS , g.ROAS_REAL_TIME_LAST_14_DAYS , g.ROAS_REAL_TIME_QTD
      , g.NR / NULLIF(abs(c.REALTIME_AD_SPEND), 0)                           AS ROAS_REAL_TIME
      , g.NR_LAST_7_DAYS / NULLIF(abs(c.REALTIME_AD_SPEND_LAST_7_DAYS), 0)   AS ROAS_REAL_TIME_LAST_7_DAYS
      , g.NR_LAST_14_DAYS / NULLIF(abs(c.REALTIME_AD_SPEND_LAST_14_DAYS), 0) AS ROAS_REAL_TIME_LAST_14_DAYS
      , g.NR_QTD / NULLIF(abs(c.REALTIME_AD_SPEND_QTD), 0)                   AS ROAS_REAL_TIME_QTD
      --
      , c.CONTRIBUTION_MARGIN_CHECKOUT , c.CONTRIBUTION_MARGIN_CHECKOUT_LAST_7_DAYS , c.CONTRIBUTION_MARGIN_CHECKOUT_LAST_14_DAYS , c.CONTRIBUTION_MARGIN_CHECKOUT_QTD
      , c.NR_COMPONENTS , c.NR_COMPONENTS_LAST_7_DAYS , c.NR_COMPONENTS_LAST_14_DAYS , c.NR_COMPONENTS_QTD , c.FIXED_AD_SPEND , c.FIXED_AD_SPEND_LAST_7_DAYS , c.FIXED_AD_SPEND_LAST_14_DAYS , c.FIXED_AD_SPEND_QTD
      , conr.contacts_total
      , tc.trip_customers_acq, tc.trip_customers_next_trip_acq, tc.trip_customers_trip_repeat -- more trip customers columns exist in the table
   FROM {{ ref('agg_metrics_global') }} g
   LEFT JOIN {{ ref('agg_metrics_contribution_margin') }} c USING(date)
   LEFT JOIN reports.agg_metrics_conr conr USING(date) -- this is in dap_load_loags and we want to avoid waiting for it as it would have a negative impact on SLA
   LEFT JOIN {{ ref('agg_metrics_trip_customers') }} tc USING(date)
  )
SELECT
       base.*

     , prev_quarter.bookings_qtd                                  AS bookings_prev_qtr
     , prev_quarter.bookings_trip_repeat_qtd                      AS bookings_trip_repeat_prev_qtr
     , prev_quarter.customers_qtd                                 AS customers_prev_qtr
     , prev_quarter.gmv_qtd                                       AS gmv_prev_qtr
     , prev_quarter.nr_qtd                                        AS nr_prev_qtr
     , prev_quarter.nr_paid_search_qtd                            AS nr_paid_search_prev_qtr
     , prev_quarter.nr_performance_marketing_qtd                  AS nr_performance_marketing_prev_qtr
     , prev_quarter.nr_str_qtd                                    AS nr_str_prev_qtr
     , prev_quarter.roas_paid_search_qtd                          AS roas_paid_search_prev_qtr
     , prev_quarter.roas_performance_marketing_qtd                AS roas_performance_marketing_prev_qtr
     , prev_quarter.roas_real_time_qtd                            AS roas_real_time_prev_qtr
     , prev_quarter.quoters_qtd                                   AS quoters_prev_qtr
     , prev_quarter.visitors_qtd                                  AS visitors_prev_qtr
     , prev_quarter.transactions_qtd                              AS transactions_prev_qtr
     , prev_quarter.guided_tours_originals_nr_share_qtd           AS guided_tours_originals_nr_share_prev_qtr
     , prev_quarter.guided_tours_originals_experience_share_qtd   AS guided_tours_originals_experience_share_prev_qtr
     , prev_quarter.nr_trip_repeat_qtd                            AS nr_trip_repeat_prev_qtr
     , prev_quarter.nr_non_paid_channels_qtd                      AS nr_non_paid_channels_prev_qtr
     , prev_quarter.contribution_margin_checkout_qtd              AS contribution_margin_checkout_prev_qtr
     , prev_quarter.nr_components_qtd                             AS nr_components_prev_qtr
     , prev_quarter.fixed_ad_spend_qtd                            AS fixed_ad_spend_prev_qtr
     
     , prev_quarter_ly.bookings_qtd                                  AS bookings_prev_qtr_last_year
     , prev_quarter_ly.bookings_trip_repeat_qtd                      AS bookings_trip_repeat_prev_qtr_last_year
     , prev_quarter_ly.customers_qtd                                 AS customers_prev_qtr_last_year
     , prev_quarter_ly.gmv_qtd                                       AS gmv_prev_qtr_last_year
     , prev_quarter_ly.nr_qtd                                        AS nr_prev_qtr_last_year
     , prev_quarter_ly.nr_paid_search_qtd                            AS nr_paid_search_prev_qtr_last_year
     , prev_quarter_ly.nr_performance_marketing_qtd                  AS nr_performance_marketing_prev_qtr_last_year
     , prev_quarter_ly.nr_str_qtd                                    AS nr_str_prev_qtr_last_year
     , prev_quarter_ly.roas_paid_search_qtd                          AS roas_paid_search_prev_qtr_last_year
     , prev_quarter_ly.roas_performance_marketing_qtd                AS roas_performance_marketing_prev_qtr_last_year
     , prev_quarter_ly.roas_real_time_qtd                            AS roas_real_time_prev_qtr_last_year
     , prev_quarter_ly.quoters_qtd                                   AS quoters_prev_qtr_last_year
     , prev_quarter_ly.visitors_qtd                                  AS visitors_prev_qtr_last_year
     , prev_quarter_ly.transactions_qtd                              AS transactions_prev_qtr_last_year
     , prev_quarter_ly.guided_tours_originals_nr_share_qtd           AS guided_tours_originals_nr_share_prev_qtr_last_year
     , prev_quarter_ly.guided_tours_originals_experience_share_qtd   AS guided_tours_originals_experience_share_prev_qtr_last_year
     , prev_quarter_ly.nr_trip_repeat_qtd                            AS nr_trip_repeat_prev_qtr_last_year
     , prev_quarter_ly.nr_non_paid_channels_qtd                      AS nr_non_paid_channels_prev_qtr_last_year
     , prev_quarter_ly.contribution_margin_checkout_qtd              AS contribution_margin_checkout_prev_qtr_last_year
     , prev_quarter_ly.nr_components_qtd                             AS nr_components_prev_qtr_last_year
     , prev_quarter_ly.fixed_ad_spend_qtd                            AS fixed_ad_spend_prev_qtr_last_year

     , last_week_day.bookings                                 AS bookings_last_week
     , last_week_day.bookings_trip_repeat                     AS bookings_trip_repeat_last_week
     , last_week_day.customers                                AS customers_last_week
     , last_week_day.gmv                                      AS gmv_last_week
     , last_week_day.nr                                       AS nr_last_week
     , last_week_day.nr_paid_search                           AS nr_paid_search_last_week
     , last_week_day.nr_performance_marketing                 AS nr_performance_marketing_last_week
     , last_week_day.nr_str                                   AS nr_str_last_week
     , last_week_day.roas_paid_search                         AS roas_paid_search_last_week
     , last_week_day.roas_performance_marketing               AS roas_performance_marketing_last_week
     , last_week_day.roas_real_time                           AS roas_real_time_last_week
     , last_week_day.quoters                                  AS quoters_last_week
     , last_week_day.visitors                                 AS visitors_last_week
     , last_week_day.transactions                             AS transactions_last_week
     , last_week_day.guided_tours_originals_nr_share          AS guided_tours_originals_nr_share_last_week
     , last_week_day.guided_tours_originals_experience_share  AS guided_tours_originals_experience_share_last_week

     , last_week_day.nr_trip_repeat                           AS nr_trip_repeat_last_week
     , last_week_day.nr_non_paid_channels                     AS nr_non_paid_channels_last_week
     , last_week_day.contribution_margin_checkout             AS contribution_margin_checkout_last_week
     , last_week_day.nr_components                            AS nr_components_last_week
     , last_week_day.fixed_ad_spend                           AS fixed_ad_spend_last_week
     , last_week_day.fixed_ad_spend_last_7_days               AS fixed_ad_spend_last_7_days_last_week
     , last_week_day.fixed_ad_spend_last_14_days              AS fixed_ad_spend_last_14_days_last_week
     , last_week_day.fixed_ad_spend_qtd                       AS fixed_ad_spend_qtd_last_week
     , last_week_day.contacts_total                           AS contacts_total_last_week

     , last_week_day.trip_customers_acq                       AS trip_customers_acq_last_week
     , last_week_day.trip_customers_next_trip_acq             AS trip_customers_next_trip_acq_last_week
     , last_week_day.trip_customers_trip_repeat               AS trip_customers_trip_repeat_last_week
     , last_week_day.nr_app                                   AS nr_app_last_week

     , last_year.bookings                                 AS bookings_last_year
     , last_year.bookings_trip_repeat                     AS bookings_trip_repeat_last_year
     , last_year.customers                                AS customers_last_year
     , last_year.gmv                                      AS gmv_last_year
     , last_year.nr                                       AS nr_last_year
     , last_year.nr_paid_search                           AS nr_paid_search_last_year
     , last_year.nr_performance_marketing                 AS nr_performance_marketing_last_year
     , last_year.nr_str                                   AS nr_str_last_year
     , last_year.roas_paid_search                         AS roas_paid_search_last_year
     , last_year.roas_performance_marketing               AS roas_performance_marketing_last_year
     , last_year.roas_real_time                           AS roas_real_time_last_year
     , last_year.quoters                                  AS quoters_last_year
     , last_year.visitors                                 AS visitors_last_year
     , last_year.transactions                             AS transactions_last_year
     , last_year.guided_tours_originals_nr_share          AS guided_tours_originals_nr_share_last_year
     , last_year.guided_tours_originals_experience_share  AS guided_tours_originals_experience_share_last_year

     , last_year.nr_trip_repeat                           AS nr_trip_repeat_last_year
     , last_year.nr_non_paid_channels                     AS nr_non_paid_channels_last_year
     , last_year.contribution_margin_checkout             AS contribution_margin_checkout_last_year
     , last_year.nr_components                            AS nr_components_last_year

     , last_year.bookings_last_7_days                                  AS bookings_last_7_days_last_year
     , last_year.bookings_trip_repeat_last_7_days                      AS bookings_trip_repeat_last_7_days_last_year
     , last_year.customers_last_7_days                                 AS customers_last_7_days_last_year
     , last_year.customers_prev_7_days                                 AS customers_prev_7_days_last_year
     , last_year.gmv_last_7_days                                       AS gmv_last_7_days_last_year
     , last_year.nr_last_7_days                                        AS nr_last_7_days_last_year
     , last_year.nr_paid_search_last_7_days                            AS nr_paid_search_last_7_days_last_year
     , last_year.nr_performance_marketing_last_7_days                  AS nr_performance_marketing_last_7_days_last_year
     , last_year.nr_str_last_7_days                                    AS nr_str_last_7_days_last_year
     , last_year.roas_paid_search_last_7_days                          AS roas_paid_search_last_7_days_last_year
     , last_year.roas_performance_marketing_last_7_days                AS roas_performance_marketing_last_7_days_last_year
     , last_year.roas_real_time_last_7_days                            AS roas_real_time_last_7_days_last_year
     , last_year.quoters_last_7_days                                   AS quoters_last_7_days_last_year
     , last_year.quoters_prev_7_days                                   AS quoters_prev_7_days_last_year
     , last_year.visitors_last_7_days                                  AS visitors_last_7_days_last_year
     , last_year.visitors_prev_7_days                                  AS visitors_prev_7_days_last_year
     , last_year.transactions_last_7_days                              AS transactions_last_7_days_last_year
     , last_year.guided_tours_originals_nr_share_last_7_days           AS guided_tours_originals_nr_share_last_7_days_last_year
     , last_year.guided_tours_originals_experience_share_last_7_days   AS guided_tours_originals_experience_share_last_7_days_last_year

     , last_year.nr_trip_repeat_last_7_days                            AS nr_trip_repeat_last_7_days_last_year
     , last_year.nr_trip_repeat_last_14_days                           AS nr_trip_repeat_last_14_days_last_year
     , last_year.nr_non_paid_channels_last_7_days                      AS nr_non_paid_channels_last_7_days_last_year
     , last_year.nr_non_paid_channels_last_14_days                     AS nr_non_paid_channels_last_14_days_last_year
     , last_year.contribution_margin_checkout_last_7_days              AS contribution_margin_checkout_last_7_days_last_year
     , last_year.contribution_margin_checkout_last_14_days             AS contribution_margin_checkout_last_14_days_last_year
     , last_year.nr_components_last_7_days                             AS nr_components_last_7_days_last_year
     , last_year.nr_components_last_14_days                            AS nr_components_last_14_days_last_year

     , last_year_quarter.bookings_qtd                                  AS bookings_qtd_last_year
     , last_year_quarter.bookings_trip_repeat_qtd                      AS bookings_trip_repeat_qtd_last_year
     , last_year_quarter.customers_qtd                                 AS customers_qtd_last_year
     , last_year_quarter.gmv_qtd                                       AS gmv_qtd_last_year
     , last_year_quarter.nr_qtd                                        AS nr_qtd_last_year
     , last_year_quarter.nr_paid_search_qtd                            AS nr_paid_search_qtd_last_year
     , last_year_quarter.nr_performance_marketing_qtd                  AS nr_performance_marketing_qtd_last_year
     , last_year_quarter.nr_str_qtd                                    AS nr_str_qtd_last_year
     , last_year_quarter.roas_paid_search_qtd                          AS roas_paid_search_qtd_last_year
     , last_year_quarter.roas_performance_marketing_qtd                AS roas_performance_marketing_qtd_last_year
     , last_year_quarter.roas_real_time_qtd                            AS roas_real_time_qtd_last_year
     , last_year_quarter.quoters_qtd                                   AS quoters_qtd_last_year
     , last_year_quarter.visitors_qtd                                  AS visitors_qtd_last_year
     , last_year_quarter.transactions_qtd                              AS transactions_qtd_last_year
     , last_year_quarter.guided_tours_originals_nr_share_qtd           AS guided_tours_originals_nr_share_qtd_last_year
     , last_year_quarter.guided_tours_originals_experience_share_qtd   AS guided_tours_originals_experience_share_qtd_last_year

     , last_year_quarter.nr_trip_repeat_qtd                            AS nr_trip_repeat_qtd_last_year
     , last_year_quarter.nr_non_paid_channels_qtd                      AS nr_non_paid_channels_qtd_last_year
     , last_year_quarter.contribution_margin_checkout_qtd              AS contribution_margin_checkout_qtd_last_year
     , last_year_quarter.nr_components_qtd                             AS nr_components_qtd_last_year
     , last_year.fixed_ad_spend                                AS fixed_ad_spend_last_year
     , last_year.fixed_ad_spend_last_7_days                    AS fixed_ad_spend_last_7_days_last_year
     , last_year.fixed_ad_spend_last_14_days                   AS fixed_ad_spend_last_14_days_last_year
     , last_year.fixed_ad_spend_qtd                            AS fixed_ad_spend_qtd_last_year
     , last_year.contacts_total                                AS contacts_total_last_year

     , last_year.trip_customers_acq                            AS trip_customers_acq_last_year
     , last_year.trip_customers_next_trip_acq                  AS trip_customers_next_trip_acq_last_year
     , last_year.trip_customers_trip_repeat                    AS trip_customers_trip_repeat_last_year
     , last_year.nr_app                                        AS nr_app_last_year

     , penultimate_year.bookings                                 AS bookings_penultimate_year
     , penultimate_year.bookings_trip_repeat                     AS bookings_trip_repeat_penultimate_year
     , penultimate_year.customers                                AS customers_penultimate_year
     , penultimate_year.gmv                                      AS gmv_penultimate_year
     , penultimate_year.nr                                       AS nr_penultimate_year
     , penultimate_year.nr_paid_search                           AS nr_paid_search_penultimate_year
     , penultimate_year.nr_performance_marketing                 AS nr_performance_marketing_penultimate_year
     , penultimate_year.nr_str                                   AS nr_str_penultimate_year
     , penultimate_year.roas_paid_search                         AS roas_paid_search_penultimate_year
     , penultimate_year.roas_performance_marketing               AS roas_performance_marketing_penultimate_year
     , penultimate_year.roas_real_time                           AS roas_real_time_penultimate_year
     , penultimate_year.quoters                                  AS quoters_penultimate_year
     , penultimate_year.visitors                                 AS visitors_penultimate_year
     , penultimate_year.transactions                             AS transactions_penultimate_year
     , penultimate_year.guided_tours_originals_nr_share          AS guided_tours_originals_nr_share_penultimate_year
     , penultimate_year.guided_tours_originals_experience_share  AS guided_tours_originals_experience_share_penultimate_year

     , penultimate_year.nr_trip_repeat                           AS nr_trip_repeat_penultimate_year
     , penultimate_year.nr_non_paid_channels                     AS nr_non_paid_channels_penultimate_year
     , penultimate_year.contribution_margin_checkout             AS contribution_margin_checkout_penultimate_year
     , penultimate_year.nr_components                            AS nr_components_penultimate_year

     , penultimate_year.bookings_last_7_days                                  AS bookings_last_7_days_penultimate_year
     , penultimate_year.bookings_trip_repeat_last_7_days                      AS bookings_trip_repeat_last_7_days_penultimate_year
     , penultimate_year.customers_last_7_days                                 AS customers_last_7_days_penultimate_year
     , penultimate_year.customers_prev_7_days                                 AS customers_prev_7_days_penultimate_year
     , penultimate_year.gmv_last_7_days                                       AS gmv_last_7_days_penultimate_year
     , penultimate_year.nr_last_7_days                                        AS nr_last_7_days_penultimate_year
     , penultimate_year.nr_paid_search_last_7_days                            AS nr_paid_search_last_7_days_penultimate_year
     , penultimate_year.nr_performance_marketing_last_7_days                  AS nr_performance_marketing_last_7_days_penultimate_year
     , penultimate_year.nr_str_last_7_days                                    AS nr_str_last_7_days_penultimate_year
     , penultimate_year.roas_paid_search_last_7_days                          AS roas_paid_search_last_7_days_penultimate_year
     , penultimate_year.roas_performance_marketing_last_7_days                AS roas_performance_marketing_last_7_days_penultimate_year
     , penultimate_year.roas_real_time_last_7_days                            AS roas_real_time_last_7_days_penultimate_year
     , penultimate_year.quoters_last_7_days                                   AS quoters_last_7_days_penultimate_year
     , penultimate_year.quoters_prev_7_days                                   AS quoters_prev_7_days_penultimate_year
     , penultimate_year.visitors_last_7_days                                  AS visitors_last_7_days_penultimate_year
     , penultimate_year.visitors_prev_7_days                                  AS visitors_prev_7_days_penultimate_year
     , penultimate_year.transactions_last_7_days                              AS transactions_last_7_days_penultimate_year
     , penultimate_year.guided_tours_originals_nr_share_last_7_days           AS guided_tours_originals_nr_share_last_7_days_penultimate_year
     , penultimate_year.guided_tours_originals_experience_share_last_7_days   AS guided_tours_originals_experience_share_last_7_days_penultimate_year

     , penultimate_year.nr_trip_repeat_last_7_days                            AS nr_trip_repeat_last_7_days_penultimate_year
     , penultimate_year.nr_non_paid_channels_last_7_days                      AS nr_non_paid_channels_last_7_days_penultimate_year
     , penultimate_year.contribution_margin_checkout_last_7_days              AS contribution_margin_checkout_last_7_days_penultimate_year
     , penultimate_year.nr_components_last_7_days                             AS nr_components_last_7_days_penultimate_year

     , two_years_quarter.bookings_qtd                                  AS bookings_qtd_penultimate_year
     , two_years_quarter.bookings_trip_repeat_qtd                      AS bookings_trip_repeat_qtd_penultimate_year
     , two_years_quarter.customers_qtd                                 AS customers_qtd_penultimate_year
     , two_years_quarter.gmv_qtd                                       AS gmv_qtd_penultimate_year
     , two_years_quarter.nr_qtd                                        AS nr_qtd_penultimate_year
     , two_years_quarter.nr_paid_search_qtd                            AS nr_paid_search_qtd_penultimate_year
     , two_years_quarter.nr_performance_marketing_qtd                  AS nr_performance_marketing_qtd_penultimate_year
     , two_years_quarter.nr_str_qtd                                    AS nr_str_qtd_penultimate_year
     , two_years_quarter.roas_paid_search_qtd                          AS roas_paid_search_qtd_penultimate_year
     , two_years_quarter.roas_performance_marketing_qtd                AS roas_performance_marketing_qtd_penultimate_year
     , two_years_quarter.roas_real_time_qtd                            AS roas_real_time_qtd_penultimate_year
     , two_years_quarter.quoters_qtd                                   AS quoters_qtd_penultimate_year
     , two_years_quarter.visitors_qtd                                  AS visitors_qtd_penultimate_year
     , two_years_quarter.transactions_qtd                              AS transactions_qtd_penultimate_year
     , two_years_quarter.guided_tours_originals_nr_share_qtd           AS guided_tours_originals_nr_share_qtd_penultimate_year
     , two_years_quarter.guided_tours_originals_experience_share_qtd   AS guided_tours_originals_experience_share_qtd_penultimate_year

     , two_years_quarter.nr_trip_repeat_qtd                            AS nr_trip_repeat_qtd_penultimate_year
     , two_years_quarter.nr_non_paid_channels_qtd                      AS nr_non_paid_channels_qtd_penultimate_year
     , two_years_quarter.contribution_margin_checkout_qtd              AS contribution_margin_checkout_qtd_penultimate_year
     , two_years_quarter.nr_components_qtd                             AS nr_components_qtd_penultimate_year
     , penultimate_year.fixed_ad_spend                                AS fixed_ad_spend_penultimate_year
     , penultimate_year.fixed_ad_spend_last_7_days                    AS fixed_ad_spend_last_7_days_penultimate_year
     , penultimate_year.fixed_ad_spend_last_14_days                   AS fixed_ad_spend_last_14_days_penultimate_year
     , penultimate_year.fixed_ad_spend_qtd                            AS fixed_ad_spend_qtd_penultimate_year
     , penultimate_year.contacts_total                                AS contacts_total_penultimate_year

     , penultimate_year.trip_customers_acq                            AS trip_customers_acq_penultimate_year
     , penultimate_year.trip_customers_next_trip_acq                  AS trip_customers_next_trip_acq_penultimate_year
     , penultimate_year.trip_customers_trip_repeat                    AS trip_customers_trip_repeat_penultimate_year
     , penultimate_year.nr_app                                        AS nr_app_penultimate_year

     , metrics_3year.bookings                                 AS bookings_3_years_ago
     , metrics_3year.bookings_trip_repeat                     AS bookings_trip_repeat_3_years_ago
     , metrics_3year.customers                                AS customers_3_years_ago
     , metrics_3year.gmv                                      AS gmv_3_years_ago
     , metrics_3year.nr                                       AS nr_3_years_ago
     , metrics_3year.nr_paid_search                           AS nr_paid_search_3_years_ago
     , metrics_3year.nr_performance_marketing                 AS nr_performance_marketing_3_years_ago
     , metrics_3year.nr_str                                   AS nr_str_3_years_ago
     , metrics_3year.roas_paid_search                         AS roas_paid_search_3_years_ago
     , metrics_3year.roas_performance_marketing               AS roas_performance_marketing_3_years_ago
     , metrics_3year.roas_real_time                           AS roas_real_time_3_years_ago
     , metrics_3year.quoters                                  AS quoters_3_years_ago
     , metrics_3year.visitors                                 AS visitors_3_years_ago
     , metrics_3year.transactions                             AS transactions_3_years_ago
     , metrics_3year.guided_tours_originals_nr_share          AS guided_tours_originals_nr_share_3_years_ago
     , metrics_3year.guided_tours_originals_experience_share  AS guided_tours_originals_experience_share_3_years_ago

     , metrics_3year.nr_trip_repeat                           AS nr_trip_repeat_3_years_ago
     , metrics_3year.nr_non_paid_channels                     AS nr_non_paid_channels_3_years_ago
     , metrics_3year.contribution_margin_checkout             AS contribution_margin_checkout_3_years_ago
     , metrics_3year.nr_components                            AS nr_components_3_years_ago

     , metrics_3year.bookings_last_7_days                                  AS bookings_last_7_days_3_years_ago
     , metrics_3year.bookings_trip_repeat_last_7_days                      AS bookings_trip_repeat_last_7_days_3_years_ago
     , metrics_3year.customers_last_7_days                                 AS customers_last_7_days_3_years_ago
     , metrics_3year.customers_prev_7_days                                 AS customers_prev_7_days_3_years_ago
     , metrics_3year.gmv_last_7_days                                       AS gmv_last_7_days_3_years_ago
     , metrics_3year.nr_last_7_days                                        AS nr_last_7_days_3_years_ago
     , metrics_3year.nr_paid_search_last_7_days                            AS nr_paid_search_last_7_days_3_years_ago
     , metrics_3year.nr_performance_marketing_last_7_days                  AS nr_performance_marketing_last_7_days_3_years_ago
     , metrics_3year.nr_str_last_7_days                                    AS nr_str_last_7_days_3_years_ago
     , metrics_3year.roas_paid_search_last_7_days                          AS roas_paid_search_last_7_days_3_years_ago
     , metrics_3year.roas_performance_marketing_last_7_days                AS roas_performance_marketing_last_7_days_3_years_ago
     , metrics_3year.roas_real_time_last_7_days                            AS roas_real_time_last_7_days_3_years_ago
     , metrics_3year.quoters_last_7_days                                   AS quoters_last_7_days_3_years_ago
     , metrics_3year.quoters_prev_7_days                                   AS quoters_prev_7_days_3_years_ago
     , metrics_3year.visitors_last_7_days                                  AS visitors_last_7_days_3_years_ago
     , metrics_3year.visitors_prev_7_days                                  AS visitors_prev_7_days_3_years_ago
     , metrics_3year.transactions_last_7_days                              AS transactions_last_7_days_3_years_ago
     , metrics_3year.guided_tours_originals_nr_share_last_7_days           AS guided_tours_originals_nr_share_last_7_days_3_years_ago
     , metrics_3year.guided_tours_originals_experience_share_last_7_days   AS guided_tours_originals_experience_share_last_7_days_3_years_ago

     , metrics_3year.nr_trip_repeat_last_7_days                            AS nr_trip_repeat_last_7_days_3_years_ago
     , metrics_3year.nr_non_paid_channels_last_7_days                      AS nr_non_paid_channels_last_7_days_3_years_ago
     , metrics_3year.contribution_margin_checkout_last_7_days              AS contribution_margin_checkout_last_7_days_3_years_ago
     , metrics_3year.nr_components_last_7_days                             AS nr_components_last_7_days_3_years_ago

     , three_years_quarter.bookings_qtd                                  AS bookings_qtd_3_years_ago
     , three_years_quarter.bookings_trip_repeat_qtd                      AS bookings_trip_repeat_qtd_3_years_ago
     , three_years_quarter.customers_qtd                                 AS customers_qtd_3_years_ago
     , three_years_quarter.gmv_qtd                                       AS gmv_qtd_3_years_ago
     , three_years_quarter.nr_qtd                                        AS nr_qtd_3_years_ago
     , three_years_quarter.nr_paid_search_qtd                            AS nr_paid_search_qtd_3_years_ago
     , three_years_quarter.nr_performance_marketing_qtd                  AS nr_performance_marketing_qtd_3_years_ago
     , three_years_quarter.nr_str_qtd                                    AS nr_str_qtd_3_years_ago
     , three_years_quarter.roas_paid_search_qtd                          AS roas_paid_search_qtd_3_years_ago
     , three_years_quarter.roas_performance_marketing_qtd                AS roas_performance_marketing_qtd_3_years_ago
     , three_years_quarter.roas_real_time_qtd                            AS roas_real_time_qtd_3_years_ago
     , three_years_quarter.quoters_qtd                                   AS quoters_qtd_3_years_ago
     , three_years_quarter.visitors_qtd                                  AS visitors_qtd_3_years_ago
     , three_years_quarter.transactions_qtd                              AS transactions_qtd_3_years_ago
     , three_years_quarter.guided_tours_originals_nr_share_qtd           AS guided_tours_originals_nr_share_qtd_3_years_ago
     , three_years_quarter.guided_tours_originals_experience_share_qtd   AS guided_tours_originals_experience_share_qtd_3_years_ago

     , three_years_quarter.nr_trip_repeat_qtd                            AS nr_trip_repeat_qtd_3_years_ago
     , three_years_quarter.nr_non_paid_channels_qtd                      AS nr_non_paid_channels_qtd_3_years_ago
     , three_years_quarter.contribution_margin_checkout_qtd              AS contribution_margin_checkout_qtd_3_years_ago
     , three_years_quarter.nr_components_qtd                             AS nr_components_qtd_3_years_ago
     , metrics_3year.fixed_ad_spend                                AS fixed_ad_spend_3_years_ago
     , metrics_3year.fixed_ad_spend_last_7_days                    AS fixed_ad_spend_last_7_days_3_years_ago
     , metrics_3year.fixed_ad_spend_last_14_days                   AS fixed_ad_spend_last_14_days_3_years_ago
     , metrics_3year.fixed_ad_spend_qtd                            AS fixed_ad_spend_qtd_3_years_ago
     , metrics_3year.contacts_total                                AS contacts_total_3_years_ago

     , metrics_3year.trip_customers_acq                            AS trip_customers_acq_3_years_ago
     , metrics_3year.trip_customers_next_trip_acq                  AS trip_customers_next_trip_acq_3_years_ago
     , metrics_3year.trip_customers_trip_repeat                    AS trip_customers_trip_repeat_3_years_ago
     , metrics_3year.nr_app                                        AS nr_app_3_years_ago

     , last_year.bookings_last_14_days                                  AS bookings_last_14_days_last_year
     , last_year.bookings_trip_repeat_last_14_days                      AS bookings_trip_repeat_last_14_days_last_year
     , last_year.customers_last_14_days                                 AS customers_last_14_days_last_year
     , last_year.gmv_last_14_days                                       AS gmv_last_14_days_last_year
     , last_year.nr_last_14_days                                        AS nr_last_14_days_last_year
     , last_year.nr_paid_search_last_14_days                            AS nr_paid_search_last_14_days_last_year
     , last_year.nr_performance_marketing_last_14_days                  AS nr_performance_marketing_last_14_days_last_year
     , last_year.nr_str_last_14_days                                    AS nr_str_last_14_days_last_year
     , last_year.roas_paid_search_last_14_days                          AS roas_paid_search_last_14_days_last_year
     , last_year.roas_performance_marketing_last_14_days                AS roas_performance_marketing_last_14_days_last_year
     , last_year.roas_real_time_last_14_days                            AS roas_real_time_last_14_days_last_year
     , last_year.quoters_last_14_days                                   AS quoters_last_14_days_last_year
     , last_year.visitors_last_14_days                                  AS visitors_last_14_days_last_year
     , last_year.transactions_last_14_days                              AS transactions_last_14_days_last_year
     , last_year.guided_tours_originals_nr_share_last_14_days           AS guided_tours_originals_nr_share_last_14_days_last_year
     , last_year.guided_tours_originals_experience_share_last_14_days   AS guided_tours_originals_experience_share_last_14_days_last_year

  FROM metrics_global AS base
  LEFT JOIN public.dim_date_deprecated AS date ON base.date = date.date_id
  LEFT JOIN metrics_global AS last_week_day ON last_week_day.date = date_add(base.date, -7)
  LEFT JOIN metrics_global AS last_year ON last_year.date = date.yoy_date_id
  LEFT JOIN public.dim_date_deprecated AS date_last_year ON last_year.date = date_last_year.date_id
  LEFT JOIN metrics_global AS penultimate_year ON penultimate_year.date = date_last_year.yoy_date_id
  LEFT JOIN public.dim_date_deprecated AS date_2year ON date_last_year.yoy_date_id = date_2year.date_id
  LEFT JOIN metrics_global AS metrics_3year ON metrics_3year.date = date_2year.yoy_date_id
  LEFT JOIN metrics_global AS last_year_quarter   ON last_year_quarter.date   = date(base.date - INTERVAL 1 YEAR)
  LEFT JOIN metrics_global AS two_years_quarter   ON two_years_quarter.date   = date(base.date - INTERVAL 2 YEAR)
  LEFT JOIN metrics_global AS three_years_quarter ON three_years_quarter.date = date(base.date - INTERVAL 3 YEAR)
  LEFT JOIN metrics_global AS prev_quarter        ON prev_quarter.date        = date(date_sub(date_trunc('QUARTER', base.date), 1))
  LEFT JOIN metrics_global AS prev_quarter_ly     ON prev_quarter_ly.date     = date(date_sub(date_trunc('QUARTER', base.date), 1) - INTERVAL 1 YEAR)
