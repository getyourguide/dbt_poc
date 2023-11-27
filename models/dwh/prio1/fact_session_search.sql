{{ config(schema=var('dwh')) }}





WITH evts AS (
    SELECT *
    FROM {{ source('default', 'events') }}
    WHERE      1=1
      AND DATE BETWEEN "{{ var ('based-on-events-start-date') }}" AND "{{ var ('end-date') }}"
)

, events_search AS (
--filter Search related events and get event properties
    SELECT    evts.date
            , evts.event_name
            , evts.event_properties.timestamp AS event_time
            , evts.event_properties.uuid AS session_search_id --event_id
            , evts.current_touchpoint.event_properties.uuid AS touchpoint_id --should rather use it from a fact_session table -yes
            , evts.attribution_session_id AS session_id

            , evts.user.visitor_id AS visitor_id

               --url
            , evts.header.url AS url
            , parse_url(replace(evts.header.url, '|', '%7C'), 'QUERY', 'searchSource') AS url_search_source_param
            , parse_url(replace(evts.header.url, '|', '%7C'), 'QUERY', 'partner_id') AS url_partner_param
            , parse_url(replace(evts.header.url, '|', '%7C'), 'QUERY', 'et') AS url_et_param

               -- json_event
            , get_json_object(evts.json_event, '$.view_id') AS view_id
            , get_json_object(evts.json_event, '$.location_id') AS location_id
            , get_json_object(evts.json_event, '$.location_type') AS location_type
            , nullif(get_json_object(evts.json_event, '$.category_id'), 0) AS category_id
            , cast(get_json_object(evts.json_event, "$.result_count_visible") AS int) AS srp_results_count
            , cast(get_json_object(evts.json_event, "$.results_count") AS int) AS dpr_results_count
            , get_json_object(evts.json_event,'$.results') AS results
            , get_json_object(evts.json_event,'$.query') AS search_text --for Seearch
            , get_json_object(evts.json_event,'$.header_title') AS header_title --for DPR
            , get_json_object(evts.json_event,'$.search_id') AS search_log_id --for Search to connect to the `SearchLog` event

            , named_struct("id", loc_1.location_id, "name", loc_1.location_name) as location_parsed_property
            , named_struct("id", loc_2.location_id, "name", loc_2.location_name) as location_parsed_url

               --pageview properties
            , evts.pageview_properties.tour_ids
    FROM
      evts
      LEFT JOIN {{ source('dwh', 'dim_location') }} AS loc_1 ON evts.pageview_properties.location_ids[0] = loc_1.location_id
      LEFT JOIN {{ source('dwh', 'dim_location') }} AS loc_2 ON replace(parse_url(replace(evts.header.url, '|', '%7C'), 'QUERY', 'lc'), 'l', '') = loc_2.location_id
    WHERE      1=1
    AND        evts.event_name IN ('SearchPageResultsRequest', 'SearchResultsView',
                              'DestinationPageRequest', 'DestinationView', 'DiscoveryView')
)

, click_event AS (
    SELECT     date
              , event_name
              , event_properties.timestamp AS event_time
              , event_properties.uuid AS event_id
              , container_name

              , user.visitor_id AS visitor_id

                -- ui
              , ui.target
              , ui.id
              , get_json_object(ui.metadata,"$.filter")     AS filter
              , get_json_object(ui.metadata,"$.date_from")   AS calendar_date_from
              , get_json_object(ui.metadata,"$.date_to")     AS calendar_date_to
              --, get_json_object(ui.metadata,'$.time_range') AS time_range

              -- json_event
              , get_json_object(json_event, '$.view_id') AS view_id
    FROM evts
    WHERE      1=1
    AND        event_name IN ('UIClick',
                              'UIChange', --works when you click on fast filters on top (not filter button itself)
                              'UITap','MobileAppUITap'
                              )
)

,  click_filters_applied AS (
    SELECT date
        , view_id
        , visitor_id
        , min(event_time) AS first_click_timestamp
        , max(event_time) AS last_click_timestamp
        , 1 AS has_filters_applied
        , collect_set(id) AS filter_ids
        , count(distinct event_id) AS number_clicks_filter
        , max(if(id = 'locations' OR split(id, '-')[0] = 'lc' OR get_json_object(filter,"$.locations") IS NOT NULL, 'locations', NULL)) AS locations
        , max(if(id IN ('categories','travelerCategory', 'activityType') OR split(id, '-')[0] = 'ct' OR get_json_object(filter,"$.categories") IS NOT NULL OR get_json_object(filter,"$.travelerCategory") IS NOT NULL OR get_json_object(filter,"$.activityType") IS NOT NULL, 'categories', NULL)) AS categories
        , max(if(id IN ('sheet-overlay-time-range-filter', 'timeRanges') OR get_json_object(filter,"$.timeRanges") IS NOT NULL, 'time_ranges', NULL)) AS time
        , max(if(id = 'durationRanges' OR split(id, '-')[0] = 'dur' OR get_json_object(filter,"$.durationRanges") IS NOT NULL, 'duration_ranges', NULL)) AS duration
        , max(if(id = 'languages' OR split(id, '-')[0] = 'lng' OR get_json_object(filter,"$.languages") IS NOT NULL, 'languages', NULL)) AS languages
        , max(if(id IN ('priceRanges', 'price') OR split(id, '-')[0] = 'price_range' OR get_json_object(filter,"$.priceRanges") IS NOT NULL OR get_json_object(filter,"$.price") IS NOT NULL, 'price_ranges', NULL)) AS price
        , max(if(id = 'services' OR split(id, '-')[0] = 'srv' OR get_json_object(filter,"$.services") IS NOT NULL, 'services', NULL)) AS services
        , max(if(id = 'interests-themes' OR get_json_object(filter,"$.themes") IS NOT NULL, 'interests_themes',NULL)) AS interests_themes
        , max(if(id = 'interests-poiTypes' OR get_json_object(filter,"$.poiTypes") IS NOT NULL, 'interests_poi_types',NULL)) AS interests_poiTypes
        , max(if(id = 'interests-audiences' OR get_json_object(filter,"$.audiences") IS NOT NULL, 'interests_audiences',NULL)) AS interests_audiences
        , max(if(id = 'interests-transportationTypes' OR get_json_object(filter,"$.transportationTypes") IS NOT NULL, 'interests_transportation_types',NULL)) AS interests_transportationTypes
        , max(if(id= 'departsFrom' OR get_json_object(filter,"$.departsFrom") IS NOT NULL, 'departs_from', NULL)) AS departsFrom
    FROM click_event
    WHERE 1=1
    AND   target in ('filter_applied', 'filter_selected')
    GROUP BY 1,2,3
)

,  click_sorted_results AS (
    SELECT date
        , view_id
        , visitor_id
        , 1 AS has_sorted_result
        , array_sort(collect_set(id)) AS sort_types
    FROM click_event
    WHERE 1=1
    AND   target in ('sortFilter')
    GROUP BY 1,2,3
)

,  click_calendar AS (
    SELECT date
        , view_id
        , visitor_id
        , 1 AS has_date_filter_applied
        , collect_set(case when calendar_date_from > "" is not null then named_struct(
                                  "date_from", calendar_date_from,
                                  "date_to", calendar_date_to,
                                  "days", datediff(calendar_date_to, calendar_date_from) + 1
                                ) end) AS date_filters
    FROM click_event
    WHERE 1=1
    AND   target in ('CalendarSelection', 'select-a-date')
    GROUP BY 1, 2, 3
)

,  click_more AS (
    SELECT date
        , view_id
        , visitor_id
        , 1 AS has_clicked_view_more
    FROM click_event
    WHERE 1=1
    AND   target in ('LoadMoreActivitiesButton')
    GROUP BY 1, 2, 3
)

, fetches AS (
    SELECT date
        , view_id
        , user.visitor_id
        , ARRAY_SORT(COLLECT_LIST(NAMED_STRUCT(
            "event_name", event_name
            , "event_id", event_properties.uuid
            , "timestamp", event_properties.timestamp
            , "filter", nullif(get_json_object(json_event, "$.filter"), "{}")
            , "result_count", int(get_json_object(json_event, "$.result_count"))
            , "result_count_visible", int(get_json_object(json_event, "$.result_count_visible"))
            , "page", int(get_json_object(json_event, "$.page"))
            , "results", from_json(get_json_object(json_event, "$.results"), "array<int>")
            , "search_results", from_json(get_json_object(json_event, "$.search_results"), "array<struct<booking_fee: float, currency: string, discounted_price: float, original_price: float, total_price: float, tour_id: int, additional_properties: array<struct<type: string, value: string>>>>")
          )), (left, right) -> -- sorting the final array using a comparative function (https://spark.apache.org/docs/latest/api/sql/index.html#array_sort)
            CASE
              WHEN (left.timestamp < right.timestamp) OR (left.timestamp = right.timestamp AND left.event_id < right.event_id) THEN -1
              WHEN (left.timestamp > right.timestamp) OR (left.timestamp = right.timestamp AND left.event_id > right.event_id) THEN 1
              ELSE 0
            END
          ) AS fetches
    FROM evts
    WHERE 1=1
    AND   event_name IN ('DestinationPageResultsRequest', 'DestinationResultsView', 'SearchPageResultsRequest', 'SearchResultsView')
    GROUP BY 1, 2, 3
)

select
       search.date
     , search.session_search_id --event_id
     , search.session_id
     , search.visitor_id
     , search.view_id --save for debugging
     , search.event_time AS search_started_timestamp
     , search.location_id --take only from Destination page
     , coalesce(search.location_id, location_parsed_property.id, location_parsed_url.id) as location_id_parsed
     , search.category_id
     , if(search.event_name IN ('SearchPageResultsRequest','SearchResultsView'),
            'Search',
            concat('LP-', coalesce(search.location_type, 'unknown'))) AS search_type --Search/destination
     , search.url AS search_page_url
     , concat(parse_url(replace(search.url, "|", "%7C"), "HOST"), parse_url(replace(search.url, "|", "%7C"), "PATH")) AS search_page_url_norm --remove all params after '?' in the URL
     , get_url_parameters(search.url, lower(search.location_type)).canonical_url AS search_page_url_canonical
     , pt.landing_page_type_id AS search_page_url_type_id

     , coalesce(
         nullif(search.search_text, ""),
         nullif(location_parsed_property.name, ""),
         nullif(location_parsed_url.name, ""),
         nullif(search.header_title, "")
       ) AS search_text
     , case
         when search.search_text > "" then "SEARCH_QUERY"
         when location_parsed_property.name > "" then "LOCATION_PROVIDED"
         when location_parsed_url.name > "" then "LOCATION_PARSED_URL"
         when search.header_title > "" then "PAGE_TITLE"
       end as search_text_source

     , CASE
         WHEN search.event_name IN ('SearchPageResultsRequest','SearchResultsView') THEN
           CASE
             WHEN url_search_source_param BETWEEN 1 AND 8 THEN url_search_source_param
             WHEN url_partner_param IS NOT NULL AND url_et_param IS NOT NULL THEN 9
             WHEN url_partner_param IS NOT NULL THEN 10
             WHEN url_et_param IS NOT NULL THEN 11
             WHEN url_search_source_param IS NULL THEN 12
           END
       END AS search_source_id

     , search.search_log_id

     , coalesce(search.srp_results_count, search.dpr_results_count) AS number_results_shown_first -- only first visible results for user
     --, search_page_number --only for search, no analog for DPR

     , coalesce(more.has_clicked_view_more = 1, false) AS has_clicked_view_more --only for desktop


     --filtering
     , coalesce(filters.has_filters_applied = 1, false) AS has_filters_applied
     , coalesce(filters.number_clicks_filter, 0) AS number_clicks_on_filter
     , size(array_except(array(locations,categories,time,duration,languages,price,services,interests_themes,
      interests_poiTypes,interests_audiences,interests_transportationTypes, departsFrom),array(NULL))) AS max_filters_applied
     , coalesce(array_except(array(locations,categories,time,duration,languages,price,services,interests_themes,
      interests_poiTypes,interests_audiences,interests_transportationTypes, departsFrom),array(NULL)), array())  AS filters_applied

     --sorting
     , coalesce(sorts.has_sorted_result = 1, false) AS has_sorted_results
     , coalesce(array_except(sorts.sort_types, array(null)), array()) as sort_types

     --calendar interactions
     , coalesce(calendar.has_date_filter_applied = 1, false) AS has_date_filter_applied
     , coalesce(array_except(calendar.date_filters, array(null)), array()) as date_filters
     , fetches.fetches
FROM events_search search
    LEFT JOIN {{ source('dwh', 'dim_landing_page_type') }} pt ON get_url_parameters(search.url, lower(search.location_type)).page_type = pt.landing_page_type
    LEFT JOIN click_filters_applied filters
        ON search.date = filters.date
        AND search.visitor_id = filters.visitor_id
        AND search.view_id = filters.view_id
    LEFT JOIN click_sorted_results sorts
        ON search.date = sorts.date
        AND search.visitor_id = sorts.visitor_id
        AND search.view_id = sorts.view_id
    LEFT JOIN click_calendar calendar
        ON search.date = calendar.date
        AND search.visitor_id = calendar.visitor_id
        AND search.view_id = calendar.view_id
    LEFT JOIN click_more more
        ON search.date = more.date
        AND search.visitor_id = more.visitor_id
        AND search.view_id = more.view_id
    LEFT JOIN fetches
        ON search.date = fetches.date
        AND search.visitor_id = fetches.visitor_id
        AND search.view_id = fetches.view_id
