



{%- macro extract_brand_provider(utm_campaign) %}
    CASE
    WHEN utm_campaign RLIKE 'BIGBANG_2022_DE'
      THEN 'BIGBANG_2022_DE'
    WHEN utm_campaign RLIKE 'BRAND_launch'
      THEN 'BIGBANG_2022_DE'
    WHEN utm_campaign RLIKE 'SUMMER22-BRA-Vine-A-DE-TV'
      THEN 'BIGBANG_2022_DE'
    WHEN utm_campaign RLIKE 'BRAND_DE_Q1_2023'
      THEN 'DE_Q1_2023'
    WHEN utm_campaign RLIKE 'BRAND_DE_MANIFESTO_2023'
      THEN 'DE_MANIFESTO_2023'
    ELSE NULLIF(extract_attribute(utm_campaign, 'BRAND'),'')
    END
{%- endmacro %}