{{ config(schema=var('dwh')) }}




-- collecting all events with a known session ID
WITH session_events AS (
    SELECT *
    FROM {{ source('default', 'events') }}
    WHERE 1 = 1
      AND DATE BETWEEN "{{ var ('based-on-events-start-date') }}" AND "{{ var ('end-date') }}"
      AND attribution_session_id IS NOT NULL
      -- filtering out massive events that don't add anything to the user journey
      AND event_name NOT IN (
        "UIEnteredDisplayPort", "ExperimentImpression", "SearchLog", "RankingServed", "BrowserPerformance",
        "PartnerAnalyzerPageRequest", "RecommendationsServed", "ActivityCardImpression", "ConnectivityRequest", "WidgetLoadRequest",
        "EmailSwimlaneSent", "NotificationReceived", "OpenMessage", "ProcessedMessage", "SendMessage"
      )
      AND event_name not rlike "^(Braze|Communication).*"
      -- filtering our event groups that are rather technical and not an indication of visitor journey
      AND event_properties.sent_by NOT IN (
        "PartnerAnalyzer", "boxoffice", "braze", "communication-preferences", "email",
        "email-templates", "experimentation", "fishfarm-gygadmin", "partner-widget", "partner-widgets",
        "ranking", "reco", "search", "supplier_portal"
      )
      AND user.visitor_id not like "fake-%"
    )

  -- computing session properties
  -- a specific technique is used here for obtaining the first non-empty property per session
  -- for example, if we want to take the first non-empty header.platform per session:
  -- -- we concatenate it with event timestamp using double-colon, for example: "08:38:11.923::desktop""
  -- -- then we take the minimal value -> because of a timestamp prefix, the minimal value is also the first one
  -- -- then we split the result by double-colon and take the second part ("desktop")
  -- -- TODO: encapsulate this logic in a UDF to make the code less cumbersome
  , session_properties as (
      select
        date
        , attribution_session_id as session_id
        , split(min(case when user.visitor_id > "" then concat(date_format(event_properties.timestamp, 'HH:mm:ss.SSS'), "::", user.visitor_id) end), "::")[1] as visitor_id
        , split(min(case when current_touchpoint.event_properties.uuid > "" then concat(date_format(event_properties.timestamp, 'HH:mm:ss.SSS'), "::", current_touchpoint.event_properties.uuid) end), "::")[1] as touchpoint_id
        , min(event_properties.timestamp) AS started_at
        , max(event_properties.timestamp) AS ended_at
        , split(min(case when header.platform > "" then concat(date_format(event_properties.timestamp, 'HH:mm:ss.SSS'), "::", CASE
            WHEN header.platform = "mweb"
              THEN "mobile"
            WHEN header.platform IN ("android", "ios")
              THEN "app"
            WHEN header.platform IN ("desktop")
              THEN header.platform
            END) end), "::")[1] as temp_platform_name
        , split(min(case when header.platform > "" then concat(date_format(event_properties.timestamp, 'HH:mm:ss.SSS'), "::", CASE
            WHEN header.platform = "mweb"
              THEN CASE
                  WHEN header.device IN ("tablet")
                    THEN header.device
                  ELSE "mobile"
                  END
            WHEN header.platform IN ("desktop", "android", "ios")
              THEN header.platform
            END) end), "::")[1] as temp_device_name
        , split(min(case when mobile_header.device > "" then concat(date_format(event_properties.timestamp, 'HH:mm:ss.SSS'), "::", mobile_header.device) end), "::")[1] as device_model
        , split(min(case when mobile_header.system_version > "" or header.os > "" then concat(date_format(event_properties.timestamp, 'HH:mm:ss.SSS'), "::", coalesce(mobile_header.system_version, header.os)) end), "::")[1] as device_system_version
        , split(min(case when mobile_header.app_version > "" then concat(date_format(event_properties.timestamp, 'HH:mm:ss.SSS'), "::", mobile_header.app_version) end), "::")[1] as device_app_version
        , split(min(case when header.domain > "" then concat(date_format(event_properties.timestamp, 'HH:mm:ss.SSS'), "::", header.domain) end), "::")[1] as domain
        , split(min(case when header.user_agent > "" then concat(date_format(event_properties.timestamp, 'HH:mm:ss.SSS'), "::", header.user_agent) end), "::")[1] as user_agent
        , split(min(case when user.locale_code > "" then concat(date_format(event_properties.timestamp, 'HH:mm:ss.SSS'), "::", user.locale_code) end), "::")[1] as user_locale_code
        , split(min(case when size(header.accept_language) > 0 and header.accept_language[0] > "" then concat(date_format(event_properties.timestamp, 'HH:mm:ss.SSS'), "::", header.accept_language[0]) end), "::")[1] as accept_language
        , split(min(case when user.currency > "" then concat(date_format(event_properties.timestamp, 'HH:mm:ss.SSS'), "::", user.currency) end), "::")[1] as user_currency
        , split(min(case when nullif(header.ip_geo_location.city_name, "-") > "" then concat(date_format(event_properties.timestamp, 'HH:mm:ss.SSS'), "::", header.ip_geo_location.city_name) end), "::")[1] as ip_geo_city
        , split(min(case when nullif(header.ip_geo_location.region_name, "-") > "" then concat(date_format(event_properties.timestamp, 'HH:mm:ss.SSS'), "::", header.ip_geo_location.region_name) end), "::")[1] as ip_geo_region
        , split(min(case when user.country_id > 0 then concat(date_format(event_properties.timestamp, 'HH:mm:ss.SSS'), "::", user.country_id) end), "::")[1] as ip_geo_country_id
        , split(min(case when current_touchpoint.header.url > "" then concat(date_format(event_properties.timestamp, 'HH:mm:ss.SSS'), "::", current_touchpoint.header.url) end), "::")[1] as landing_page_url
        , CONCAT (
            parse_url(replace(split(min(case when current_touchpoint.header.url > "" then concat(date_format(event_properties.timestamp, 'HH:mm:ss.SSS'), "::", current_touchpoint.header.url) end), "::")[1], "|", "%7C"), "HOST")
            , parse_url(replace(split(min(case when current_touchpoint.header.url > "" then concat(date_format(event_properties.timestamp, 'HH:mm:ss.SSS'), "::", current_touchpoint.header.url) end), "::")[1], "|", "%7C"), "PATH")
          ) as landing_page_url_norm
        , COALESCE(NULLIF(get_url_parameters(split(min(case when current_touchpoint.header.url > "" then concat(date_format(event_properties.timestamp, 'HH:mm:ss.SSS'), "::", current_touchpoint.header.url) end), "::")[1], "").location_id, 0), concat("-", substr(attribution_session_id, 1, 6))) as landing_location_id
        , max(user.is_logged_in) AS has_logged_in
        , max(is_office_ip) AS is_office_ip
        , max(is_bounce) AS is_bounce
        , max(case when event_name in ("HomePageRequest", "HomeView", "SearchPageResultsRequest", "SearchResultsView", "DestinationPageRequest", "DestinationView", "DiscoveryView", "ActivityDetailPageRequest", "ActivityView") then 1 else 0 end) = 1 as is_discovery
        , max(case when event_name in ("AppInstall") then 1 else 0 end) = 1 as has_app_install
        , max(case when event_name in ("SignupAction") then 1 else 0 end) = 1 as has_account_created
        , count(distinct case when event_name rlike "^.*(Page.*Request|View)" THEN view_id END) as number_page_views
        , sum(case when event_name IN ("UIClick", "MobileAppUITap") AND ui.target = "create-list-confirm" then 1 end) as number_wishlists_created
        , sum(case when event_name IN ("UIClick", "MobileAppUITap") AND ui.target LIKE "%list%" then 1 end) as number_wishlist_interactions
      from session_events
      group by 1, 2
    )

    -- here we compute session properties that must be in a specific order
    -- hence windowing is required
    , session_ordered_properties as (
        select distinct
          date
          , attribution_session_id as session_id
          , collect_list(
              named_struct("event_name", event_name, "event_id", event_properties.uuid, "timestamp", event_properties.timestamp)
            ) OVER w_order AS events
        from session_events
        where
          (event_name rlike "^.*(Page.*Request|View|Action)"
            OR event_name IN ("AppInstall", "AppOpen", "AttributionTracking"))
        window
          w_order AS (
            PARTITION BY date, attribution_session_id ORDER BY event_properties.timestamp ASC
              , event_properties.uuid ASC rows BETWEEN unbounded preceding AND unbounded following
            )
    )

  -- computing unique sessions together with their properties
  -- partition key: date, session_id, visitor_id
  , sessions AS (
      select
        session_properties.*
        , session_ordered_properties.events
      from
        session_properties
        left join session_ordered_properties on
          session_properties.date = session_ordered_properties.date
          and session_properties.session_id = session_ordered_properties.session_id
    )

SELECT s.DATE
  , s.session_id
  , s.visitor_id
  , s.touchpoint_id
  , s.started_at
  , s.ended_at
  , pl.platform_id
  , dv.device_id
  , s.device_model
  , s.device_system_version
  , s.device_app_version
  , s.domain
  , s.user_agent
  , s.user_locale_code
  , REPLACE(s.accept_language, "_", "-") AS accept_language
  , s.user_currency
  , s.ip_geo_city
  , s.ip_geo_region
  , s.ip_geo_country_id
  , s.landing_page_url
  , s.landing_page_url_norm
  , get_url_parameters(s.landing_page_url, l.location_type).canonical_url as landing_page_url_canonical
  , pt.landing_page_type_id
  , s.has_logged_in
  , s.is_office_ip
  , s.is_bounce
  , s.is_discovery
  , s.has_app_install
  , s.has_account_created
  , s.number_page_views
  , s.number_wishlists_created
  , s.number_wishlist_interactions
  , s.events
FROM sessions AS s
LEFT JOIN {{ source('dwh', 'dim_platform') }} AS pl
  ON s.temp_platform_name = pl.platform_name
LEFT JOIN {{ source('dwh', 'dim_device') }} AS dv
  ON s.temp_device_name = dv.device_name
LEFT JOIN {{ source('dwh', 'dim_location') }} l
  ON s.landing_location_id = l.location_id
LEFT JOIN {{ source('dwh', 'dim_landing_page_type') }} pt
  ON get_url_parameters(s.landing_page_url, l.location_type).page_type = pt.landing_page_type