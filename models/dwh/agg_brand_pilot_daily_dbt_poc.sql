
{%- set data_sources = [ source('brand_reports', 'weekly_pilot_campaign_report') ,
                         source('default', 'brand_de_ooh_berlinbikes_2023_manualbaptiste')
                        ]
%}

with pilot_ooh_23_campaigns as (
-- There are 4 ooh campaigns that their costs are not reported correctly by Pilot
-- The costs are taken directly from Brand manager (Baptiste)
 SELECT
  network
  , sum(impressions*1000) AS impressions
  , COUNT('*') campaigns_length_days
 FROM {{ source('brand_reports', 'weekly_pilot_campaign_report') }}
 WHERE LOWER(media_channel) IN ('ooh', 'dooh')
 GROUP BY 1
)
, pilot_ooh_23_campaigns_costs as
(
  SELECT
   pcam.network,
   pc.cost / pcam.campaigns_length_days as daily_cost
   from pilot_ooh_23_campaigns pcam
   left join {{ ref('pilot_ooh_network_costs')}} pc using (network)
 )

{%- for data_source in data_sources %}

SELECT
  to_date(from_unixtime(unix_timestamp(DATE (airing_date), 'yyyy-MM-dd'))) AS report_date
  , {{ generate_brand_provider('agency', 'media_channel') }} AS provider
  , CAST(NULL AS string) AS account
  , replace(media_channel, 'programatic', 'programmatic') AS channel
  , CAST(NULL AS string) AS campaign_id
  , utm_campaign AS campaign_name
  , {{ extract_brand_provider('utm_campaign') }} AS brand_campaign
  , 0 AS is_attraction
  , CAST(NULL AS string) AS attraction
  , CASE
    WHEN utm_campaign LIKE '%|CREATIVE_%'
      THEN extract_attribute(utm_campaign, 'CREATIVE')
    WHEN media_channel IN ('dooh', 'ooh')
      THEN NULL
    ELSE creative
    END AS brand_creative
  , NULLIF(extract_attribute(utm_campaign, 'MOTIF'), '') AS motif
  , cast( {{ get_country_id('Germany') }} AS INT) AS country_id
--  , 27 as  AS country_id
  , sum(impressions) AS impressions
  , sum(clicks) AS clicks
  {%- if data_source == source('brand_reports', 'weekly_pilot_campaign_report') %}
  , sum( COALESCE( ooh_23.daily_cost, media_costs / 100) )AS cost
  {%- else  %}
   sum(  media_costs / 100)AS cost
  {%- endif %}
  , CAST(NULL AS INT) AS reach
  , CAST(NULL AS INT) AS email_sent
  , CAST(NULL AS INT) AS clicks_GYG
  , sum(transactions) AS transactions
  , CAST(NULL AS INT) AS nr
  , CAST(NULL AS INT) AS gmv
  , CAST(NULL AS INT) AS clicks_qualified_GYG
  , "{{data_source}}" AS data_source
  , current_timestamp AS dwh_created_at
FROM {{data_source}}
{%- if data_source == source('brand_reports', 'weekly_pilot_campaign_report') %}
LEFT JOIN pilot_ooh_23_campaigns_costs ooh_23 USING (network)
-- File names reflect the lasted data dump from Pilot
WHERE file_name = (select max(file_name) from brand_reports.weekly_pilot_campaign_report)
{%- endif %}
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,12

{{ 'UNION ALL' if not loop.last else '' }}
{%- endfor %}