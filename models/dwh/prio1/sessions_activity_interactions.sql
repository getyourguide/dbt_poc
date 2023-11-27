{{ config(schema=var('marketplace_reports')) }}




/*
    Model to see user interactions on ADP.
    PK — attribution_session_id.
*/

WITH events AS (
    SELECT *
    FROM  {{ source('default', 'events') }}
    WHERE      1=1
    {% if is_incremental() %}
    AND        date BETWEEN "{{ var ('based-on-events-start-date') }}" AND "{{ var ('end-date') }}"
    {% endif %}
)

, events_clicks AS (
    SELECT     date
             , event_name
             , event_properties.timestamp AS event_time
             , kafka_timestamp
             , attribution_session_id
             , is_bounce
             , user.visitor_id AS visitor_id
             , header.platform AS platform
             , container_name

               -- ui
             , ui.target
             , get_json_object(ui.metadata,"$.filter") AS filter
             , concat(get_json_object(ui.metadata, '$.sortBy'), '-', get_json_object(ui.metadata, '$.direction')) AS sort_reviews_details
             , get_json_object(ui.metadata, '$.page') AS reviews_seen -- there's a page property in SPRR but only in the json_event

    FROM events
    WHERE      1=1
    AND        (event_name IN ('UIClick') --web
    OR          event_name IN ('UITap', 'MobileAppUITap')) --apps

)

, events_availability AS (
    SELECT     date
             , event_name
             , event_properties.timestamp AS event_time
             , kafka_timestamp
             , attribution_session_id

               -- json_event
             , get_json_object(json_event, "$.update_trigger")      AS update_trigger
             , get_json_object(json_event, "$.tour_id")             AS event_tour_id

               -- availability
             , availability.available_options[0]   AS available_options
             , availability.unavailable_options[0] AS unavailable_options

    FROM events
    WHERE      1=1
    AND        event_name IN ('CheckTourAvailabilityAction')

)

, sessions_availability as (
    SELECT date
         , attribution_session_id
         , max(if(update_trigger = "pageRequest", 0, 1)) AS has_adp_checked_availability -- REDEFINED TO CONSIDER VAs
         , max(CASE
                 WHEN event_tour_id > 0 AND available_options IS NULL THEN 1
                 ELSE 0
               END) AS has_adp_hard_unavailability -- REDEFINED TO CONSIDER VAs
         , array_except(collect_set(update_trigger), array(NULL)) AS adp_configurator_interactions
    FROM events_availability
    GROUP BY 1,2
)

, sessions_ui_slide AS (
    SELECT     date,
               attribution_session_id,
               1 AS has_adp_slided_photos
    FROM events
    WHERE      1=1
    AND        event_name = 'UISlide'
    AND        ui.target IN ('photo-gallery','PhotoViewer')
    GROUP BY 1,2

)

, sessions_reviews_scroll AS (
    SELECT  date
          , attribution_session_id
          , 1 AS has_adp_scrolled_to_reviews
    FROM events
    WHERE      1=1
    AND        event_name IN ('UIEnteredDisplayPort',
                              'MobileAppUIShow')
    AND        (ui.id = 'reviews' OR ui.target = 'reviews')
    GROUP BY 1,2
)

SELECT
         dsc.date
       , dsc.attribution_session_id

         --wishlist
       , max(if(clicks.container_name IN ('Activity') AND target IN ('wishlist-add', 'activity_card_wishlist', 'wishlist-card'), 1, 0))                AS has_added_wishlist_activity
       , max(if(clicks.container_name IN ('Location', 'Destination') AND target IN ('wishlist-add', 'activity_card_wishlist', 'wishlist-card'), 1, 0)) AS has_added_wishlist_location
       , max(if(clicks.container_name IN ('Search', 'TFESearch') AND target IN ('wishlist-add', 'activity_card_wishlist', 'wishlist-card'), 1, 0))     AS has_added_wishlist_search
       , max(if(clicks.container_name IN ('Home', 'homeRedesigned') AND target IN ('wishlist-add', 'activity_card_wishlist', 'wishlist-card'), 1, 0))  AS has_added_wishlist_homepage
       , max(if(clicks.target IN ('wishlist-add', 'activity_card_wishlist', 'wishlist-card'), 1, 0))             AS has_added_wishlist
       , min(if(clicks.target IN ('wishlist-add', 'activity_card_wishlist', 'wishlist-card'), event_time, NULL)) AS first_wishlist_time
       , max(if(clicks.target IN ('wishlist-add', 'activity_card_wishlist', 'wishlist-card'), event_time, NULL)) AS last_wishlist_time

         --activity reviews and photo slides
       , max(if(clicks.container_name IN ('Activity') AND target = 'SortReviews', 1, 0))    AS has_adp_sorted_reviews
       , max(if(clicks.container_name IN ('Activity') AND target = 'FilterReviews', 1, 0))  AS has_adp_filtered_reviews
       , max(if(clicks.container_name IN ('Activity') AND target = 'GetMoreReviews', 1, 0)) AS has_adp_see_more_reviews_click

       , string(collect_set(if(container_name IN ('Activity'), sort_reviews_details, NULL))) AS adp_reviews_sort_types
       , string(collect_set(if(container_name IN ('Activity'), filter, NULL)))               AS adp_reviews_filter_types
       , max(if(container_name IN ('Activity') AND target IN ('SortReviews', 'FilterReviews', 'GetMoreReviews'),  reviews_seen, NULL)) AS max_reviews_seen_in_single_adp

       , max(coalesce(reviews.has_adp_scrolled_to_reviews,0)) AS has_adp_scrolled_to_reviews
       , max(coalesce(slide.has_adp_slided_photos,0))         AS has_adp_slided_photos

         -- landing page clicks
       , max(if(container_name IN ('Destination') AND target IN ('activity-card','activity'), 1, 0)) AS has_lp_card_click
       , max(if(container_name IN ('Destination') AND target = 'see-all-button', 1, 0))              AS has_lp_see_all_click

         -- search page clicks
       , max(if(container_name IN ('Search', 'TFESearch') AND target IN ('activity-card','activity'), 1, 0)) AS has_search_result_click

         --check availability flags
       , max(has_adp_checked_availability)  AS has_adp_checked_availability
       , max(has_adp_hard_unavailability)   AS has_adp_hard_unavailability
       , max(adp_configurator_interactions) AS adp_configurator_interactions
FROM {{ ref('sessions_discovery') }} dsc
    LEFT JOIN events_clicks clicks
        ON  dsc.attribution_session_id = clicks.attribution_session_id
        AND dsc.date = clicks.date
    LEFT JOIN sessions_ui_slide slide
        ON dsc.attribution_session_id = slide.attribution_session_id
        AND dsc.date = slide.date
    LEFT JOIN sessions_reviews_scroll reviews
        ON dsc.attribution_session_id = reviews.attribution_session_id
        AND dsc.date = reviews.date
    LEFT JOIN sessions_availability availability
        ON dsc.attribution_session_id = availability.attribution_session_id
        AND dsc.date = availability.date
WHERE 1=1
      {% if is_incremental() %}
        AND dsc.date BETWEEN "{{ var ('based-on-events-start-date') }}" AND "{{ var ('end-date') }}"
      {% endif %}
GROUP BY 1,2
