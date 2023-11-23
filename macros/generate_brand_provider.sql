{%- macro generate_brand_provider(agency, media_channel) %}
    CONCAT (
    agency
    , '/'
    , CASE
      WHEN media_channel = ('Programmatic Display')
        THEN 'Programmatic Display'
      WHEN media_channel = ('Programmatic Video')
        THEN 'Programmatic Video'
      WHEN media_channel = 'linear_tv_zdf_weather' OR utm_campaign RLIKE 'zdf_weather'
        THEN 'Linear TV - ZDF Wetter'
      WHEN media_channel = 'linear_tv'
        THEN 'Linear TV'
      WHEN media_channel = 'linear_tv_bachelorette'
        THEN 'Linear TV - Bachelorette'
      WHEN media_channel = 'ooh' OR media_channel = 'dooh'
        THEN 'OOH'
      WHEN media_channel RLIKE 'CTV'
        THEN 'Connected TV'
      WHEN media_channel RLIKE 'OTT'
        THEN 'OTT'
      WHEN media_channel = 'native_ads'
        THEN 'Native Ads'
      ELSE NULL
      END
    )
{%- endmacro %}