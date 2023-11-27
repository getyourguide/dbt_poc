{{ config(schema=var('reports')) }}




WITH sessions AS (
  SELECT *
  FROM
      {{ ref('agg_discovery_session') }}
  WHERE 1=1
    {% if is_incremental() %}
      AND date BETWEEN "{{ var ('based-on-events-start-date') }}" AND "{{ var ('end-date') }}"
    {% endif %}
    {% if target.name == 'dev' %}
      AND date BETWEEN date_add(current_date, -7) and date_add(current_date,-1)
    {% endif %}
)

, dimension_values as (
--assign 1 dimension value per visitor
SELECT s.date
      , s.visitor_id
      , s.attribution_session_id
      , has_lp_bounce
      , has_adp_bounce
      , has_search_bounce
      , has_homepage_bounce
      , has_adp_view
      , has_search_request
      , has_search
      , has_adp_checked_availability
      , has_addtocart
      , has_checkout_request
      , has_added_wishlist
      , has_bookaction
      , s.sd_entry_time
      --choose logged in value if user was logged in once during the day
      , MAX(fs.is_logged_in) OVER visitor_daily AS is_logged_in
      --first platform value per visitor
      , first_value(platform, true) OVER visitor_daily AS platform_first
      --first locale_code value per visitor
      , first_value(s.locale_code, true) OVER visitor_daily AS locale_code_first
      --first country_id value per visitor
      , first_value(fs.current_touchpoint.user.country_id, true) OVER visitor_daily AS source_country_id_first
      --first domain value per visitor
      , first_value(fs.current_touchpoint.header.domain, true) OVER visitor_daily AS domain_first
      , first_value(s.marketing_channel, true) OVER visitor_daily AS marketing_channel_first
      , ARRAY_SORT(COLLECT_SET(s.marketing_channel) OVER visitor_daily) as marketing_channel_list
  FROM sessions s
    LEFT JOIN {{ ref('fact_session') }} fs
      ON s.attribution_session_id = fs.attribution_session_id
      AND s.date = fs.date
  WINDOW visitor_daily AS (PARTITION BY s.date, s.visitor_id
                           ORDER BY s.sd_entry_time ASC
                           ROWS BETWEEN unbounded preceding AND unbounded following)
)
SELECT date
    , platform_first as platform
    , locale_code_first as locale_code
    , is_logged_in
    , CAST(source_country_id_first AS BIGINT) AS source_country_id
    , domain_first AS domain
    , marketing_channel_first as marketing_channel
    , marketing_channel_list
    , COUNT(DISTINCT visitor_id) AS visitors
    , COUNT(DISTINCT CASE WHEN has_lp_bounce + has_adp_bounce + has_search_bounce + has_homepage_bounce > 0 THEN visitor_id END) AS bouncers
    , COUNT(DISTINCT CASE WHEN has_adp_view > 0 THEN visitor_id END) AS quoters
    , COUNT(DISTINCT CASE WHEN has_search_request > 0 OR has_search > 0 THEN visitor_id END) AS visitors_search_page
    , COUNT(DISTINCT CASE WHEN has_adp_checked_availability > 0 THEN visitor_id END) AS visitors_checked_availability
    , COUNT(DISTINCT CASE WHEN has_addtocart > 0 THEN visitor_id END) AS visitors_added_to_cart
    , COUNT(DISTINCT CASE WHEN has_checkout_request > 0 THEN visitor_id END) AS visitors_checkout
      -- need this field because now visitors can skip cart and do checkouts directly
    , COUNT(DISTINCT CASE WHEN  has_addtocart > 0 and has_checkout_request > 0 THEN visitor_id END) AS visitors_checkout_from_cart
    , COUNT(DISTINCT CASE WHEN has_added_wishlist > 0 THEN visitor_id END) AS visitors_added_to_wishlist
    , COUNT(DISTINCT CASE WHEN has_bookaction > 0 THEN visitor_id END) AS customers
    , COUNT(DISTINCT attribution_session_id) AS sessions
    , COUNT(DISTINCT CASE WHEN has_lp_bounce + has_adp_bounce + has_search_bounce + has_homepage_bounce > 0 THEN attribution_session_id END) AS sessions_bounced
FROM dimension_values
GROUP BY 1,2,3,4,5,6,7,8