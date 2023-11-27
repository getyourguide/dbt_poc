{{ config(schema=var('marketplace_reports')) }}





WITH events AS (
    SELECT *
    FROM {{ source('default', 'events') }}
    WHERE 1=1
      {% if is_incremental() %}
        AND date BETWEEN "{{ var ('based-on-events-start-date') }}" AND "{{ var ('end-date') }}"
      {% endif %}
)

, events_discovery AS (
    --get all needed event properties
    SELECT  date
          , event_name
          , event_properties.timestamp AS event_time
          , kafka_timestamp
          , attribution_session_id
          , is_bounce
          , user.visitor_id     AS visitor_id
          , user.is_logged_in
          , header.platform     AS platform
          , coalesce(user.locale_code, current_touchpoint.user.locale_code) AS locale_code
          , coalesce(user.country_id, current_touchpoint.user.country_id)   AS source_country_id
          , coalesce(header.domain, current_touchpoint.header.domain)       AS DOMAIN
          , current_touchpoint.event_properties.uuid AS touchpoint_uuid --not always filled in

            --url
          , header.url AS url
          , regexp_extract(header.url, "(?<=-l)([0-9]+)((?=/)|(?!\.)|(?=\\?))") AS url_location_id --for DestinationPageRequest only
          , regexp_extract(header.url, "(?<=-n)([0-9]+)((?=/)|(?!\.)|(?=/?)|(?=\\?))") AS url_country_id --for DestinationPageRequest only
             --- extract the digit pattern preceded by -tc pattern and followed by /. For example extract 54 from "-tc54/"
             --- these is the pattern for category id in destionation pages, for search pages we can pull the categoy id from pageview_properties
          , regexp_extract(header.url, "-tc([0-9]+)/", 1) AS url_tour_cat_id

            --ui
          , ui.id AS uuid
          , get_json_object(ui.metadata, "$.filter") AS filter

            -- json_event
          , get_json_object(json_event, '$.location_type') AS location_type
          , get_json_object(json_event, "$.tour_id") AS event_tour_id

            --pageview properties
          , pageview_properties.location_ids AS location_ids
          , pageview_properties.category_ids
          , pageview_properties.tour_ids

            -- referral pageview properties
          , referral_pageview_properties.location_ids AS referral_location_ids
          , referral_pageview_properties.category_ids AS referral_category_ids

    FROM events
    WHERE      1=1
    AND        (event_name IN ('DestinationPageRequest',
                              'ActivityDetailPageRequest',
                              'SearchPageResultsRequest',
                              'HomePageRequest') -- definition of discovery session in web
    OR         event_name IN ('DiscoveryView',
                              'HomeView',
                              'DestinationView',
                              'ActivityView',
                              'SearchResultsView')) -- definition of discovery session for apps
    AND        NOT COALESCE(is_office_ip, false)
)

, events_discovery_flags AS (
    --create flags based on event name and event property
    SELECT events_discovery.*

         , CASE WHEN event_name IN ('ActivityDetailPageRequest','ActivityView') THEN 'ADP'
              WHEN event_name IN ('SearchPageResultsRequest','SearchResultsView') THEN 'Search'
              WHEN event_name IN ('HomePageRequest','HomeView') THEN 'HomePage'
              WHEN event_name IN ('DestinationPageRequest','DestinationView','DiscoveryView') THEN concat('LP-', coalesce(location_type, 'unknown'))
           END AS short_event_name

        , CASE WHEN event_name IN ('DestinationPageRequest')
                    THEN if(url_location_id="", NULL, url_location_id) -- this pattern matches -l followed by digits (which it captures), followed by /, nothing or ?
               WHEN event_name IN ('DestinationView','DiscoveryView')
                    THEN location_ids[0] -- location_id not tracked in apps yet
               WHEN event_name IN ('SearchPageResultsRequest', 'SearchResultsView')
                    THEN location_ids[0] -- pageview_properties.location_ids[0] ON SEARCH ONLY CONTAINS non-country location_ids. Apps don't have location_id tracking yet
               WHEN event_name IN ('ActivityDetailPageRequest','ActivityView')
                    THEN coalesce(nullif(tour.location_id1, 0), tour.location_id) -- prioritising $-sign attribution location over location_id
               ELSE NULL
          END AS location_id

        , CASE WHEN event_name IN ('DestinationPageRequest')
                    THEN if(url_country_id="", NULL, url_country_id) -- this pattern matches -n followed by digits (which it captures), followed by /, nothing or ?
               WHEN event_name IN ('SearchPageResultsRequest')
                    THEN from_json(get_json_object(filter, '$.country_ids'), 'array<string>')[0]
               ELSE NULL
          END AS country_id

        , CASE WHEN event_name IN ('DestinationPageRequest','DestinationView','DiscoveryView')
                  THEN category_ids[0]
               WHEN event_name IN ('SearchPageResultsRequest','SearchResultsView')
                  THEN coalesce(category_ids[0], referral_category_ids[0], if(url_tour_cat_id = "", NULL, url_tour_cat_id))
               ELSE NULL
          END AS tour_category_id

        , if(event_name IN ('ActivityDetailPageRequest','ActivityView'), 1, 0)                                     AS has_adp_view
        , if(event_name IN ('ActivityDetailPageRequest','ActivityView') AND is_bounce = TRUE, 1,0)                 AS has_adp_bounce
        , if(event_name IN ('DestinationPageRequest','DestinationView','DiscoveryView'), 1, 0)                     AS has_lp_view
        , if(event_name IN ('DestinationPageRequest','DestinationView','DiscoveryView') AND is_bounce = TRUE, 1,0) AS has_lp_bounce
        , if(event_name IN ('SearchPageResultsRequest','SearchResultsView'), 1, 0)                                 AS has_search
        , if(event_name IN ('SearchPageResultsRequest','SearchResultsView') AND is_bounce = TRUE, 1,0)             AS has_search_bounce
        , if(event_name IN ('HomePageRequest','HomeView'), 1, 0)                                                   AS has_homepage_view
        , if(event_name IN ('HomePageRequest','HomeView') AND is_bounce = TRUE, 1,0)                               AS has_homepage_bounce

    FROM events_discovery
      LEFT JOIN {{ source('dwh', 'dim_tour') }} tour
          ON tour.tour_id = events_discovery.tour_ids[0]
)

, events_discovery_ranked AS (
    SELECT date
         , attribution_session_id
         , visitor_id
         , url
         , min(event_time) over w_sesison                       AS sd_entry_time
         , first_value(short_event_name) over w_sesison         AS sd_entry_point
         , first_value(touchpoint_uuid, TRUE) over w_sesison    AS touchpoint_uuid
         , max(is_logged_in) over w_sesison                     AS is_logged_in
         , first_value(platform, TRUE) over w_sesison           AS platform
         , first_value(locale_code, TRUE) over w_sesison        AS locale_code
         , first_value(DOMAIN, TRUE) over w_sesison             AS DOMAIN
         , first_value(source_country_id, TRUE) over w_sesison  AS source_country_id
         , first_value(location_id) over w_sesison              AS location_id
         , first_value(country_id, TRUE) over w_sesison         AS country_id
         , first_value(tour_category_id) over w_sesison         AS tour_category_id
         , first_value(location_type, true) over w_sesison      AS location_type

         , has_adp_view
         , has_adp_bounce
         , min(if(has_adp_view=1, event_time, NULL)) over w_sesison AS first_adp_view_time
         , max(if(has_adp_view=1, event_time, NULL)) over w_sesison AS last_adp_view_time
         , has_lp_view
         , has_lp_bounce
         , min(if(has_lp_view=1, event_time, NULL)) over w_sesison AS first_lp_view_time
         , max(if(has_lp_view=1, event_time, NULL)) over w_sesison AS last_lp_view_time
         , has_search
         , has_search_bounce
         , min(if(has_search=1, event_time, NULL)) over w_sesison AS first_search_time
         , max(if(has_search=1, event_time, NULL)) over w_sesison AS last_search_time
         , has_homepage_view
         , has_homepage_bounce
         , min(if(has_homepage_view=1, event_time, NULL)) over w_sesison AS first_homepage_view_time
         , max(if(has_homepage_view=1, event_time, NULL)) over w_sesison AS last_homepage_view_time

         , event_tour_id
    FROM events_discovery_flags
    WINDOW w_sesison AS (PARTITION BY attribution_session_id
                               ORDER BY event_time, kafka_timestamp ASC
                               ROWS BETWEEN unbounded preceding AND unbounded following)
)

, event_search AS (
    --this part can be improved and cleaned if we include SearchPageRequest in discovery events
    SELECT
            attribution_session_id
          , max(1) AS has_search_request
          , max(if(is_bounce=TRUE, 1,0)) AS has_search_request_bounce
    FROM events
    WHERE      1=1
        AND    event_name IN ('SearchPageRequest')
    GROUP BY 1
)

--grouping values to make sure final table is unique by session_id, date
SELECT
       date
     , ranked.attribution_session_id
     , visitor_id
     , min(sd_entry_time)   AS sd_entry_time
     , min(sd_entry_point)  AS sd_entry_point
     , min(touchpoint_uuid) AS touchpoint_uuid
     , min(platform)        AS platform
     , max(is_logged_in)    AS is_logged_in
     , min(locale_code)     AS locale_code
     , min(DOMAIN)          AS domain
     , min(source_country_id) AS source_country_id
     , min(location_id)       AS location_id
     , min(tour_category_id)  AS tour_category_id
     , min(country_id)        AS country_id
     , min(location_type)     AS location_type

       --adp
     , max(has_adp_view)    AS has_adp_view
     , max(has_adp_bounce)  AS has_adp_bounce
     , min(first_adp_view_time) AS first_adp_view_time
     , max(last_adp_view_time)  AS last_adp_view_time
       --lp
     , max(has_lp_view)         AS has_lp_view
     , max(has_lp_bounce)       AS has_lp_bounce
     , min(first_lp_view_time)  AS first_lp_view_time
     , max(last_lp_view_time)   AS last_lp_view_time
       --search
     , max(has_search)                            AS has_search
     , max(coalesce(search.has_search_request,0)) AS has_search_request
     , max(greatest(coalesce(search.has_search_request_bounce,0), ranked.has_search_bounce)) AS has_search_bounce
     , min(first_search_time)   AS first_search_time
     , max(last_search_time)    AS last_search_time
      --homepage
     , max(has_homepage_view)        AS has_homepage_view
     , max(has_homepage_bounce)      AS has_homepage_bounce
     , min(first_homepage_view_time) AS first_homepage_view_time
     , max(last_homepage_view_time)  AS last_homepage_view_time
      --urls
     , count(DISTINCT url) AS url_count
     , count(DISTINCT if(has_search=1, url, NULL)) AS unique_search_url_count

     , size(collect_set(event_tour_id)) AS SADPs_seen

FROM events_discovery_ranked ranked
    LEFT JOIN event_search search
        ON ranked.attribution_session_id = search.attribution_session_id
GROUP BY 1,2,3
