{{ config(schema=var('marketplace_reports')) }}




WITH bookings AS (
    SELECT date
         , attribution_session_id
         , max(has_addtocart)        AS has_addtocart
         , max(has_checkout_request) AS has_checkout_request
         , max(has_bookaction)       AS has_bookaction
    FROM {{ ref('sessions_booking_interactions') }}
    GROUP BY 1,2
)

, touchpoints AS (
    -- still need this dependency because touchpoint can be null for some events
    SELECT
             date
           , attribution_session_id
           , current_touchpoint.channel                AS marketing_channel
           , current_touchpoint.user.country_id        AS source_country_id
           , current_touchpoint.event_properties.uuid  AS touchpoint_id
    FROM {{ ref('fact_session') }}
    WHERE 1=1
)

, location AS (
    SELECT * FROM {{ source('dwh', 'dim_location') }}
)

, country AS (
    SELECT *
    FROM {{ source('dwh', 'dim_location') }}
    WHERE location_name NOT IN (SELECT location_name FROM {{ source('dwh', 'dim_location') }} WHERE country_id = 35 AND location_name <> 'Britain') -- Britain country_id 35 is mapped to five location_ids of type country (Scotland, Wales etc). If user visited on Britain country page, we only want the Britain location_id 169017 https://www.getyourguide.com/united-kingdom-n35/
      AND location_type = 'country'
)

SELECT
        dsc.date
      , dsc.attribution_session_id
      , dsc.visitor_id
      , dsc.sd_entry_time
      , dsc.sd_entry_point
      , if(dsc.sd_entry_point LIKE 'LP%',
           concat_ws('_', dsc.location_type, CASE WHEN tour_cat.primary_category_name IS NULL THEN 'NOT_TC' ELSE 'TC' END),
           NULL) AS sd_entry_lp_type
      , touchpoints.touchpoint_id
      , touchpoints.marketing_channel
      , dsc.platform
      , dsc.is_logged_in
      , dsc.locale_code
      , dsc.domain
      , cast(dsc.source_country_id as int)  AS source_country_id
      , dsc.location_id AS location_id
      , dsc.tour_category_id
      , cast(dsc.country_id as int)         AS country_id
      , coalesce(loc.location_name, country.location_name) AS destination_location_name
      , coalesce(loc.country_name, country.country_name)   AS destination_country
      , dsc.has_adp_view
      , dsc.has_adp_bounce
      , dsc.first_adp_view_time
      , dsc.last_adp_view_time
      , dsc.has_lp_view
      , dsc.has_lp_bounce
      , dsc.first_lp_view_time
      , dsc.last_lp_view_time
      , dsc.has_search
      , dsc.has_search_request
      , dsc.has_search_bounce
      , dsc.first_search_time
      , dsc.last_search_time
      , dsc.has_homepage_view
      , dsc.has_homepage_bounce
      , dsc.first_homepage_view_time
      , dsc.last_homepage_view_time
      , dsc.url_count
      , dsc.unique_search_url_count
      , dsc.SADPs_seen

        --bookings
      , coalesce(bookings.has_addtocart, 0) AS has_addtocart
      , coalesce(bookings.has_checkout_request, 0) AS has_checkout_request
      , coalesce(bookings.has_bookaction, 0) AS has_bookaction

        --adp
      , coalesce(adp.has_adp_checked_availability, 0) AS has_adp_checked_availability
      , coalesce(adp.has_adp_hard_unavailability, 0)  AS has_adp_hard_unavailability
      , adp_configurator_interactions
      , coalesce(adp.has_added_wishlist_activity, 0) AS has_added_wishlist_activity
      , coalesce(adp.has_added_wishlist_location, 0) AS has_added_wishlist_location
      , coalesce(adp.has_added_wishlist_search, 0)   AS has_added_wishlist_search
      , coalesce(adp.has_added_wishlist_homepage, 0) AS has_added_wishlist_homepage
      , coalesce(adp.has_added_wishlist, 0)          AS has_added_wishlist
      , adp.first_wishlist_time
      , adp.last_wishlist_time
      , coalesce(adp.has_adp_sorted_reviews, 0)         AS has_adp_sorted_reviews
      , coalesce(adp.has_adp_filtered_reviews, 0)       AS has_adp_filtered_reviews
      , coalesce(adp.has_adp_see_more_reviews_click, 0) AS has_adp_see_more_reviews_click
      , coalesce(adp.adp_reviews_sort_types, 'none')    AS adp_reviews_sort_types
      , coalesce(adp.adp_reviews_filter_types, 'none')  AS adp_reviews_filter_types
      , cast(coalesce(adp.max_reviews_seen_in_single_adp, 0) as INT) AS max_reviews_seen_in_single_adp
      , coalesce(adp.has_adp_scrolled_to_reviews, 0)    AS has_adp_scrolled_to_reviews
      , coalesce(adp.has_adp_slided_photos, 0)          AS has_adp_slided_photos
      , coalesce(adp.has_lp_card_click, 0)              AS has_lp_card_click
      , coalesce(adp.has_lp_see_all_click, 0)           AS has_lp_see_all_click
      , coalesce(adp.has_search_result_click, 0)        AS has_search_result_click

        --search
      , search.first_search_source
      , search.first_url
      , search.search_sources
      , search.srp_unique_tour_count
      , search.srp_category_count
      , coalesce(search.has_originals_srp, 0) AS has_originals_srp
      , search.max_results_shown_in_srp
      , coalesce(search.has_srp_seemore_clicks, 0) AS has_srp_seemore_clicks
      , coalesce(search.has_sort_results_srp, 0)   AS has_sort_results_srp
      , search.sort_types
      , search.sort_types_distinct_count
      , coalesce(search.has_filter_applied, 0) AS has_filter_applied
      , search.applied_filter_types
      , search.applied_filter_distinct_count

      , CASE
             WHEN dsc.sd_entry_point = 'ADP' THEN 'activity'
             WHEN dsc.sd_entry_point = 'LP-POI' AND dsc.platform = 'mweb' THEN 'activity'
             WHEN dsc.sd_entry_point = 'LP-POI' AND dsc.platform = 'desktop' AND n_sessions > 1 THEN 'activity'
             ELSE 'destination'
        END AS visitor_intent
      -- must implement backfilling logic (both as CTE and in the JOINs list) and replace the test table with the rivulus one.

FROM {{ ref('sessions_discovery') }} dsc
    LEFT JOIN bookings
        ON dsc.attribution_session_id = bookings.attribution_session_id
        AND dsc.date = bookings.date
    LEFT JOIN {{ ref('sessions_search_interactions') }} search
        ON dsc.attribution_session_id = search.attribution_session_id
        AND dsc.date = search.date
    LEFT JOIN {{ ref('sessions_activity_interactions') }} adp
        ON dsc.attribution_session_id = adp.attribution_session_id
        AND dsc.date = adp.date
    LEFT JOIN touchpoints
        ON touchpoints.attribution_session_id = dsc.attribution_session_id
        AND touchpoints.date = dsc.date
    LEFT JOIN {{ ref('count_sessions_60_days') }} AS sessions_count
        ON dsc.attribution_session_id = sessions_count.attribution_session_id
        AND dsc.date = sessions_count.date
    LEFT JOIN location loc
        ON loc.location_id = dsc.location_id
    LEFT JOIN country
        ON country.country_id = dsc.country_id
    LEFT JOIN {{ source('dwh', 'dim_tour_category') }} tour_cat
        ON tour_cat.tour_category_id = dsc.tour_category_id
WHERE 1=1
  AND dsc.platform IS NOT NULL
  {% if is_incremental() %}
  --5 days time window to grab touchpoints
  AND dsc.date BETWEEN DATE_SUB("{{ var ('based-on-events-start-date') }}", 5) AND "{{ var ('end-date') }}"
  {% endif %}
