{{ config(schema=var('dwh')) }}





WITH evts AS (
    SELECT *
    FROM {{ source('default', 'events') }}
    WHERE      1=1
      AND DATE BETWEEN "{{ var ('based-on-events-start-date') }}" AND "{{ var ('end-date') }}"
)

, search_enriched as (
    select date
         , session_search_id
         , visitor_id
         , session_id
         , search_started_timestamp
         , lead(search_started_timestamp) over(partition by date, session_id order by search_started_timestamp) as next_search_timestamp
    from {{ ref('fact_session_search') }}
    where
      DATE BETWEEN "{{ var ('based-on-events-start-date') }}" AND "{{ var ('end-date') }}"
      AND number_results_shown_first > 0 -- we attribute ADPs only to Searches with at least one result (ticket: MMI-885)
)

, events_adp AS (
--filter ADP related events and get event properties
    SELECT     date
              , event_name
              , event_properties.timestamp AS event_time
              , lead(event_properties.timestamp) over(partition by date,  user.visitor_id, pageview_properties.tour_ids[0] order by event_properties.timestamp) as next_event_time
              , event_properties.uuid AS session_adp_id --event_id
              , current_touchpoint.event_properties.uuid AS touchpoint_id --should rather use it from a fact_session table
              , attribution_session_id AS session_id
              , kafka_timestamp

              , user.visitor_id AS visitor_id

               --url
              , header.url AS url

               --pageview properties
              , pageview_properties.tour_ids[0] AS tour_id

              -- json_event
              , get_json_object(json_event, '$.view_id') AS view_id
    FROM evts
    WHERE      1=1
    AND        event_name IN ('ActivityDetailPageRequest',
                              'ActivityView')
    AND        pageview_properties.tour_ids[0] > 0
)

, adp_availability AS (
    SELECT     distinct
               date
             , pageview_properties.tour_ids[0] AS tour_id
             , user.visitor_id AS visitor_id
             , collect_list(named_struct(
                              "event_id", event_properties.uuid
                            , "timestamp", event_properties.timestamp
                            , "date", get_json_object(json_event, "$.date_selected")
                            , "language", get_json_object(ui.metadata,'$.language.selected.name')
                            , "participants", aggregate(map_values(from_json(get_json_object(json_event, "$.participants_count"), "map<string,integer>")), cast(0 as double), (x, y) -> x + y) --this is a hardcoded for now, ideally we should have it in event itself availability.participants_count
                            , "avaialable_tour_options", availability.available_options.tour_option_id
               )) over w_order AS availability_checks
             , max(event_properties.timestamp) over w_order as latest_timestamp
    FROM evts
    WHERE      1=1
    AND        event_name = 'CheckTourAvailabilityAction'
    window
        w_order as (
        partition by date, user.visitor_id, pageview_properties.tour_ids[0]
        order by event_properties.timestamp asc, event_properties.uuid
        rows between unbounded preceding and unbounded following
     )
)

, adp_checkout_tours AS (
/*
    There is no options to link checkouts and addtocarts via view_id as it's a new page request happens
    We will link checkout to to exact ADP view via tour_id,  timestamp and session_id
    Need to explode all tours in case there are > 1 checkouts for the same tour during the session.
*/
    SELECT
               date
             , event_name
             , user.visitor_id
             , attribution_session_id AS session_id
             , explode(coalesce(booking.cart.cart_items.activities.tour_id, pageview_properties.tour_ids)) AS tour_id
             , event_properties.timestamp as event_time
    FROM evts
    WHERE      1=1
    AND        event_name IN ('AddToCartAction',
                              'CheckoutPageRequest',
                              'CheckoutView')
)

, adp_checkout AS (
    SELECT
               date
             , visitor_id
             , tour_id
             , count(*) AS event_cnt
             , max(case when event_name = 'AddToCartAction' then event_time end) as latest_timestamp_addtocart
             , max(case when event_name IN ('CheckoutPageRequest', 'CheckoutView') then event_time end) as latest_timestamp_checkout
    FROM adp_checkout_tours
    GROUP BY 1,2,3
)

, widgets_direct AS ( -- combining the rules where the tour ID is taken from ui.metadata
  SELECT
    date
    , user.visitor_id AS visitor_id
    , get_json_object(ui.metadata, "$.tour_id") AS tour_id
    , get_json_object(json_event, '$.view_id') AS view_id
    , MAX(IF(event_name = 'UISlide' AND ui.target IN ('photo-gallery', 'PhotoViewer'), 1, 0)) AS has_adp_slided_photos
    , MAX(IF(event_name IN ('UIEnteredDisplayPort', 'MobileAppUIShow') AND (ui.id = 'reviews' OR ui.target = 'reviews'), 1, 0)) AS has_adp_scrolled_to_reviews
    , MAX(IF(event_name IN ('UIClick', 'UITap', 'MobileAppUITap') AND container_name = 'Activity' AND ui.target = 'SortReviews', 1, 0)) AS has_adp_sorted_reviews
    , MAX(IF(event_name IN ('UIClick', 'UITap', 'MobileAppUITap') AND container_name = 'Activity' AND ui.target = 'FilterReviews', 1, 0)) AS has_adp_filtered_reviews
    , MAX(IF(event_name IN ('UIClick', 'UITap', 'MobileAppUITap') AND container_name = 'Activity' AND ui.target = 'GetMoreReviews', 1, 0)) AS has_adp_see_more_reviews_click
    , NULLIF(MAX(IF(event_name IN ('UIClick', 'UITap', 'MobileAppUITap') AND container_name = 'Activity' AND ui.target IN ('SortReviews', 'FilterReviews', 'GetMoreReviews'), get_json_object(ui.metadata, '$.page'), 0)), 0) AS max_reviews_seen
    , COLLECT_SET(IF(event_name IN ('UIClick', 'UITap', 'MobileAppUITap') AND container_name = 'Activity' AND ui.target = 'FilterReviews', get_json_object(ui.metadata, '$.filter'), NULL)) AS adp_reviews_filter_types
    , COLLECT_SET(IF(event_name IN ('UIClick', 'UITap', 'MobileAppUITap') AND container_name = 'Activity' AND ui.target = 'SortReviews', concat(get_json_object(ui.metadata, '$.sortBy'), '-', get_json_object(ui.metadata, '$.direction')), NULL)) AS adp_reviews_sort_types
  FROM evts
  WHERE event_name IN ("UISlide", "UIEnteredDisplayPort", "MobileAppUIShow", "UIClick", "UITap", "MobileAppUITap")
  GROUP BY 1, 2, 3, 4
)

, widgets_referral AS ( -- combining the rules where the tour ID is taken from referral_pageview_properties.tour_ids
  SELECT
    date
    , user.visitor_id AS visitor_id
    , referral_pageview_properties.tour_ids[0] AS tour_id
    , get_json_object(json_event, '$.view_id') AS view_id
    , MAX(IF(
        (event_name = 'UIClick' and ui.target = 'BookingAssistantCalendarOpen' and ui.id = 'datepicker')
          OR (event_name = 'TravelerMobileAppBACalendarShowAction' and container_name = 'BookingOptions')
        , 1, 0
      )) AS has_adp_open_calendar
    , MAX(IF(
        (event_name = 'UIClick' and ui.target = 'peoplepicker' and ui.id = 'PeoplePickerOpen')
          OR (event_name = 'MobileAppUITap' and ui.target = 'PeoplePickerOpen' and ui.id = 'peoplepicker')
        , 1, 0
      )) AS has_adp_open_people_picker
    , MAX(IF(
        (event_name = 'MobileAppRecommendationImpression' and ui.target = 'similar-recommendations' and ui.id = 'recommendations' and container_name = 'Activity')
          OR (event_name = 'UIEnteredDisplayPort' and ui.target = 'recommendations' and container_name = 'Activity')
          OR (event_name = 'UIEnteredDisplayPort' and ui.target = 'component' and ui.id = 'related-activities')
        , 1, 0
      )) AS has_adp_seen_recommendations
  FROM evts
  WHERE event_name IN ("UIClick", "TravelerMobileAppBACalendarShowAction", "MobileAppUITap", "MobileAppRecommendationImpression", "UIEnteredDisplayPort")
  GROUP BY 1, 2, 3, 4
)

SELECT adp.date
     , adp.session_adp_id
     , search.session_search_id
     , adp.session_id
     , adp.visitor_id
     , adp.view_id
     , adp.event_time AS visit_started_timestamp
     , adp.tour_id

      --availability
     , greatest(size(avail.availability_checks), 0) AS number_check_availability
     , avail.availability_checks
     --, 'null' AS number_unavailbility --don't include now as it's still raw

     , cart.visitor_id > "" AS has_add_to_cart
     , checkout.visitor_id > "" AS has_checkout

      --photo slides, calendars, and other widgets
     , coalesce(wd.has_adp_slided_photos, 0) = 1 AS has_slided_photos
     , coalesce(wd.has_adp_scrolled_to_reviews, 0) = 1 AS has_scrolled_to_reviews
     , coalesce(wd.has_adp_sorted_reviews, 0) = 1   AS has_sorted_reviews
     , coalesce(wd.has_adp_filtered_reviews, 0) = 1   AS has_filtered_reviews
     , coalesce(wd.has_adp_see_more_reviews_click, 0) = 1   AS has_seen_more_reviews
     , case when size(wd.adp_reviews_sort_types) > 0 then wd.adp_reviews_sort_types end as reviews_sort_types
     , case when size(wd.adp_reviews_filter_types) > 0 then wd.adp_reviews_filter_types end as reviews_filter_types
     , wd.max_reviews_seen
     , coalesce(wr.has_adp_open_calendar, 0) = 1 AS has_opened_calendar
     , coalesce(wr.has_adp_open_people_picker, 0) = 1 AS has_opened_people_picker
     , coalesce(wr.has_adp_seen_recommendations, 0) = 1 AS has_seen_recommendations
FROM events_adp adp
    LEFT JOIN adp_availability avail
        ON adp.date = avail.date
        AND adp.visitor_id = avail.visitor_id
        AND adp.tour_id = avail.tour_id
        AND avail.latest_timestamp >= adp.event_time
    LEFT JOIN adp_checkout checkout
        ON  adp.date = checkout.date
        AND adp.visitor_id = checkout.visitor_id
        AND adp.tour_id = checkout.tour_id --checkout was for the same tour_id as ADP view
        AND checkout.latest_timestamp_checkout IS NOT NULL
        AND checkout.latest_timestamp_checkout >= adp.event_time
    LEFT JOIN adp_checkout cart
        ON  adp.date = cart.date
        AND adp.visitor_id = cart.visitor_id
        AND adp.tour_id = cart.tour_id
        AND cart.latest_timestamp_addtocart IS NOT NULL
        AND cart.latest_timestamp_addtocart >= adp.event_time
    LEFT JOIN search_enriched search
        ON adp.date = search.date
        AND adp.session_id  = search.session_id
        AND cast(adp.event_time AS timestamp) >= cast(search.search_started_timestamp AS timestamp)
        AND cast(adp.event_time AS timestamp) < cast(coalesce(search.next_search_timestamp, current_timestamp()) AS timestamp)
    LEFT JOIN widgets_direct wd
        ON adp.date = wd.date
        AND adp.visitor_id = wd.visitor_id
        AND adp.tour_id = wd.tour_id
        AND adp.view_id = wd.view_id
    LEFT JOIN widgets_referral wr
        ON adp.date = wr.date
        AND adp.visitor_id = wr.visitor_id
        AND adp.tour_id = wr.tour_id
        AND adp.view_id = wr.view_id