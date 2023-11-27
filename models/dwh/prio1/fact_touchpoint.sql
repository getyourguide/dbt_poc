{{ config(schema=var('dwh')) }}





-- Once we finish refatoring attribution pipeline, we can potentially move this model to depend on touchpoints_with_cost as then we will have correct channel there
WITH attribution_base
AS (
  SELECT *
    , COALESCE(attribution.utm_source, trim(split(attribution.adjust_attribution, '::') [0])) AS utm_source
    , COALESCE(attribution.utm_campaign, CASE
        WHEN size(split(attribution.adjust_attribution, '::')) > 1
          THEN trim(substring_index(split(attribution.adjust_attribution, '::') [1], " (", 1))
        ELSE attribution.adjust_attribution
        END) AS utm_campaign
    , attribution.utm_content AS utm_content
    , attribution.partner_hash_code AS partner_hash_code
    , attribution.partner_campaign AS partner_campaign
    , sem_parameters.campaign_id AS campaign_id
    , sem_parameters.adgroup_id AS adgroup_id
    , sem_parameters.ad_id AS ad_id
    , sem_parameters.keyword_id AS keyword_id
    , attribution.partner_sources AS partner_source
    , COALESCE(NULLIF(get_url_parameters(header.url, '').location_id, 0), - CAST(rand() * 100 AS INT)) AS landing_location_id --extract location_id from landing page and avoid data skew
    , COALESCE(NULLIF(user.visitor_id, ""), concat("-", md5(string(rand())))) as visitor_id_join
  FROM {{ source('default', 'fact_attribution') }}
  WHERE 1 = 1
    AND DATE BETWEEN "{{ var ('touchpoint-start') }}" AND "{{ var ('touchpoint-end') }}"
    AND COALESCE(event_properties.event_name, '') <> 'AppInstall' -- exclude appinstall virtual touchpoints
    AND virtual_touchpoint IS NULL -- exclude all types of virtual touchpoints, we only consider real touchpoints
    AND touchpoint_id IS NOT NULL
  )
  , visitor_history
AS (
  SELECT *
  FROM {{ ref('fact_attribution_visitor_history') }}
  WHERE DATE BETWEEN add_months("{{ var ('touchpoint-start') }}", -18) AND "{{ var ('touchpoint-end') }}"
    AND visitor_id IS NOT NULL
  )
  , new_visitors
AS (
  SELECT DISTINCT ab.DATE
    , ab.touchpoint_id
    , av.visitor_id IS NULL AS is_new_visitor
  FROM attribution_base ab
  LEFT JOIN visitor_history av
    ON av.TIMESTAMP >= ab.event_properties.TIMESTAMP - INTERVAL 18 MONTHS
      AND av.TIMESTAMP < ab.event_properties.TIMESTAMP
      AND ab.visitor_id_join = av.visitor_id
      -- check against all touchpoints in the last 360 days of each touchpoint
  )
SELECT a.touchpoint_id
  , a.DATE
  , a.user.visitor_id
  , a.event_properties.timestamp AS touchpoint_timestamp
  , nullif(a.header.ip_geo_location.city_name, "-") AS ip_geo_city
  , nullif(a.header.ip_geo_location.region_name, "-") AS ip_geo_region
  , a.user.country_id AS ip_geo_country_id
  , b.purchase_type_id = 1 AS is_acquisition
  , ev.is_new_visitor
  , a.header.url AS landing_page_url
  , CONCAT (
    parse_url(replace(a.header.url, "|", "%7C"), "HOST")
    , parse_url(replace(a.header.url, "|", "%7C"), "PATH")
    ) AS landing_page_url_norm --remove all params after '?' in the URL
  , get_url_parameters(a.header.url, l.location_type).canonical_url AS landing_page_url_canonical
  , a.header.referrer_url
  , pt.landing_page_type_id AS landing_page_type_id
  , de.device_id AS device_id
  , cm.channel_id
  , re.reseller_id AS partner_id
  , a.partner_campaign
  , a.partner_source
  , b.promo_id
  , a.campaign_id
  , a.ad_id
  , a.adgroup_id
  , a.keyword_id
  , a.utm_campaign
  , a.utm_content
  , a.utm_source
  , dt.display_type_id AS display_type_id
  , cp.cost_provider_id
  , COALESCE(SUM(a.clicks.ad_clicks / a.cost_weight), 0) AS clicks
  , COALESCE(SUM(a.impressions.ad_impressions / a.cost_weight), 0) AS impressions
  , COALESCE(SUM((a.cost.unit_cost + a.cost.remaining_cost) / a.cost_weight), 0) AS ad_spend
  , COALESCE(SUM(a.cost.unit_cost / a.cost_weight), 0) AS unit_cost
  , COALESCE(SUM(a.cost.remaining_cost / a.cost_weight), 0) AS remaining_cost
FROM attribution_base a
LEFT JOIN {{ source('marketing', 'dim_touchpoint') }} b
  ON a.DATE = b.DATE
    AND a.touchpoint_id = b.touchpoint_id
LEFT JOIN new_visitors ev
  ON a.touchpoint_id = ev.touchpoint_id
LEFT JOIN {{ source('dwh', 'dim_device') }} de
  ON a.header.device = de.device_name
LEFT JOIN {{ source('dwh', 'dim_attribution_channel_group') }} cm
  ON b.channel = cm.channel
LEFT JOIN {{ source('dwh', 'dim_reseller') }} re
  ON a.partner_hash_code = re.cookie_partner_id
LEFT JOIN {{ source('marketing', 'dim_promo_with_display_type') }} dy
  ON b.promo_id = dy.promo_id -- skewed join because of promos that mean empty values (e.g. `a87ff679a2f3e71d9181a67b7542122c` or `ac5d442acff4dd737b861b09300a2b3f`)
LEFT JOIN {{ source('dwh', 'dim_display_type') }} dt
  ON dy.display_type = dt.display_type
LEFT JOIN {{ source('dwh', 'dim_location') }} l
  ON a.landing_location_id = l.location_id
LEFT JOIN {{ source('dwh', 'dim_landing_page_type') }} pt
  ON get_url_parameters(a.header.url, l.location_type).page_type = pt.landing_page_type -- we need the location_type to determine page type
LEFT JOIN {{ source('dwh', 'dim_cost_provider') }} cp
  ON cost.cost_source = cp.cost_provider
GROUP BY 1
  , 2
  , 3
  , 4
  , 5
  , 6
  , 7
  , 8
  , 9
  , 10
  , 11
  , 12
  , 13
  , 14
  , 15
  , 16
  , 17
  , 18
  , 19
  , 20
  , 21
  , 22
  , 23
  , 24
  , 25
  , 26
  , 27
  , 28
  , 29
