{{ config(schema=var('marketplace_reports')) }}




/*
    Model to see user interactions on Search pages.
    PK — attribution_session_id.
*/

--all additional flags related to search events (as URL, tour_ids, search filters applied)
WITH events AS (
    SELECT *
    FROM {{ source('default', 'events') }}
    WHERE      1=1
    {% if is_incremental() %}
        AND date BETWEEN "{{ var ('based-on-events-start-date') }}" AND "{{ var ('end-date') }}"
    {% endif %}
)

, events_search AS (
--filter Search related events and get event properties
    SELECT     date,
               event_name,
               event_properties.timestamp AS event_time,
               kafka_timestamp,
               attribution_session_id,

               user.visitor_id AS visitor_id,

               --url
               header.url AS url,
               parse_url(header.url, 'QUERY', 'searchSource') AS search_source_param,
               parse_url(header.url, 'QUERY', 'partner_id') AS partner_id_param,
               parse_url(header.url, 'QUERY', 'et') AS et_param,

               -- json_event
               get_json_object(json_event, '$.location_type') AS location_type,
               cast(get_json_object(json_event, "$.result_count_visible") AS int) AS result_count_visible,
               get_json_object(json_event,'$.results') AS results,
               get_json_object(json_event,'$.page') AS srp_page_number,

               --pageview properties
               pageview_properties.tour_ids
    FROM events
    WHERE      1=1
    AND        event_name IN ('SearchPageResultsRequest',
                              'SearchResultsView')
)

, search_flags AS (
    SELECT events_search.*

        , CASE
              WHEN search_source_param = 1 THEN 'date picker' -- LP date picker
              WHEN search_source_param = 2 THEN 'search box homepage' -- Search Box on HP with or without dates but inputing search term
              WHEN search_source_param = 3 THEN 'search box LP'
              WHEN search_source_param = 4 THEN 'see all' -- See all LP
              WHEN search_source_param = 5 THEN 'deals' -- not sure what that means, it is written in TF code. investigate if volume increases
              WHEN search_source_param = 6 THEN 'explore_collection_LP' -- collection LP Explore All button
              WHEN search_source_param = 7 THEN 'Search box ADP or other' -- Search box ADP + other pages (help...)
              WHEN search_source_param = 8 THEN 'Search box SRP' -- Search box SRP (changed only when new search term is inputed, date input without new search word won't change it)
              WHEN partner_id_param IS NOT NULL AND et_param IS NOT NULL THEN 'partner+et'
              WHEN partner_id_param IS NOT NULL THEN 'partner'
              WHEN et_param IS NOT NULL THEN 'et'
              WHEN search_source_param IS NULL THEN 'no search source'
              ELSE 'other'
          END AS search_source

         , if(srp_page_number<=1, 0, 1) AS more_than_first_srp

         , rank() OVER (PARTITION BY attribution_session_id ORDER BY event_time, kafka_timestamp ASC) AS srp_rank

    FROM events_search

)

, search_tours AS (
    --UNPIVOT all tour ids in the search events
    SELECT attribution_session_id
         , explode(tour_ids) AS search_tour_id
    FROM {{ source('test', 'events_search') }}
)

, search_tours_flags AS (
    --get tour flags in search
    SELECT search.attribution_session_id
         , count(DISTINCT search.search_tour_id)        AS srp_unique_tour_count
         , count(DISTINCT tour.category)                AS srp_category_count
         , max(if(tour.is_gyg_originals = TRUE, 1, 0))  AS has_originals_srp
    FROM search_tours SEARCH
        LEFT JOIN {{ source('dwh', 'dim_tour') }} tour
            ON tour.tour_id = SEARCH.search_tour_id
    GROUP BY 1
)

, events_search_interactions AS (
    -- clicks related to Search
    SELECT     date
              , event_name
              , event_properties.timestamp AS event_time
              , kafka_timestamp
              , attribution_session_id

                -- ui
              , ui.target
              , ui.id
              , get_json_object(ui.metadata,"$.filter")     AS filter
              , get_json_object(ui.metadata,'$.time_range') AS time_range
    FROM events
    WHERE      1=1
    AND        event_name IN ('UIClick',
                              'UIChange',
                              'MobileAppUITap')
    AND        container_name  IN ('Search', 'TFESearch')
)

, search_flags_filters_base AS (
    SELECT
         attribution_session_id
        , max(if(event_name = 'UIClick' AND target = 'sortFilter',1,0)) AS has_sort_results_srp -- Sort exists only on desktop/mweb, always UIClick
        , array_sort(collect_set(if(target = 'sortFilter',id,NULL))) AS sort_types
        , max(if((event_name IN ('UIClick','UIChange') AND (target = 'filter_applied'OR (target = 'TimeRangeFilterSheetClosed' AND  if(time_range IS NULL OR time_range = '',0,1)=1))) OR (event_name IN ('UITap','MobileAppUITap') AND target LIKE 'filter%'),1,0)) AS has_filter_applied
        , max(if(id = 'locations' OR split(id, '-')[0] = 'lc' OR get_json_object(filter,"$.locations") IS NOT NULL, 'locations', NULL)) AS locations
        , max(if(id IN ('categories','travelerCategory', 'activityType') OR split(id, '-')[0] = 'ct' OR get_json_object(filter,"$.categories") IS NOT NULL OR get_json_object(filter,"$.travelerCategory") IS NOT NULL OR get_json_object(filter,"$.activityType") IS NOT NULL, 'categories', NULL)) AS categories
        , max(if(id = 'sheet-overlay-time-range-filter' OR get_json_object(filter,"$.time") IS NOT NULL, 'time', NULL)) AS time
        , max(if(id = 'durationRanges' OR split(id, '-')[0] = 'dur' OR get_json_object(filter,"$.durationRanges") IS NOT NULL, 'durationRanges', NULL)) AS duration
        , max(if(id = 'languages' OR split(id, '-')[0] = 'lng' OR get_json_object(filter,"$.languages") IS NOT NULL, 'languages', NULL)) AS languages
        , max(if(id IN ('priceRanges', 'price') OR split(id, '-')[0] = 'price_range' OR get_json_object(filter,"$.priceRanges") IS NOT NULL OR get_json_object(filter,"$.price") IS NOT NULL, 'priceRanges', NULL)) AS price
        , max(if(id = 'services' OR split(id, '-')[0] = 'srv' OR get_json_object(filter,"$.services") IS NOT NULL, 'services', NULL)) AS services
        , max(if(id = 'interests-themes' OR get_json_object(filter,"$.themes") IS NOT NULL, 'interests-themes',NULL)) AS interests_themes -- Expand to mweb soon
        , max(if(id = 'interests-poiTypes' OR get_json_object(filter,"$.poiTypes") IS NOT NULL, 'interests-poiTypes',NULL)) AS interests_poiTypes -- Expand to mweb soon
        , max(if(id = 'interests-audiences' OR get_json_object(filter,"$.audiences") IS NOT NULL, 'interests-audiences',NULL)) AS interests_audiences -- Expand to mweb soon
        , max(if(id = 'interests-transportationTypes' OR get_json_object(filter,"$.transportationTypes") IS NOT NULL, 'interests-transportationTypes',NULL)) AS interests_transportationTypes -- Expand to mweb soon
        , max(if(id= 'departsFrom' OR get_json_object(filter,"$.departsFrom") IS NOT NULL, 'departsFrom', NULL)) AS departsFrom
    FROM events_search_interactions
    WHERE 1=1
    AND (target IN ('filter_applied','sortFilter')
            OR (target = 'TimeRangeFilterSheetClosed'
                AND if(time_range IS NULL OR time_range = '',0,1)=1))
    GROUP BY 1
)

, search_flags_filters_final AS (
  SELECT
      attribution_session_id
    , has_sort_results_srp
    , string(sort_types) AS sort_types
    , size(sort_types) AS sort_types_distinct_count
    , has_filter_applied
    , string(array_except(array(locations,categories,time,duration,languages,price,services,interests_themes,
      interests_poiTypes,interests_audiences,interests_transportationTypes, departsFrom),array(NULL))) AS applied_filter_types
    , size(array_except(array(locations,categories,time,duration,languages,price,services,interests_themes,
      interests_poiTypes,interests_audiences,interests_transportationTypes, departsFrom),array(NULL))) AS applied_filter_distinct_count
  FROM search_flags_filters_base
)

--grouping values to make sure final table is unique by session_id, date
SELECT
        search_flags.date
      , search_flags.attribution_session_id
      , search_flags.visitor_id
      , max(if(srp_rank = 1, search_source, NULL)) AS first_search_source
      , max(if(srp_rank = 1, url, NULL))           AS first_url
      , string(collect_set(search_source))         AS search_sources

      , max(search_tours_flags.srp_unique_tour_count) AS srp_unique_tour_count
      , max(search_tours_flags.srp_category_count)    AS srp_category_count
      , max(search_tours_flags.has_originals_srp)     AS has_originals_srp
      , max(result_count_visible)                     AS max_results_shown_in_srp
      , max(search_flags.more_than_first_srp)         AS has_srp_seemore_clicks

        -- filter flags
      , max(coalesce(srp_filters.has_sort_results_srp, 0)) AS has_sort_results_srp
      , max(srp_filters.sort_types)                        AS sort_types
      , max(srp_filters.sort_types_distinct_count)         AS sort_types_distinct_count
      , max(coalesce(srp_filters.has_filter_applied, 0))   AS has_filter_applied
      , max(srp_filters.applied_filter_types)              AS applied_filter_types
      , max(srp_filters.applied_filter_distinct_count)     AS applied_filter_distinct_count

FROM search_flags
    LEFT JOIN search_tours_flags
        ON search_flags.attribution_session_id = search_tours_flags.attribution_session_id
    LEFT JOIN search_flags_filters_final srp_filters
        ON search_flags.attribution_session_id = srp_filters.attribution_session_id
GROUP BY 1,2,3
