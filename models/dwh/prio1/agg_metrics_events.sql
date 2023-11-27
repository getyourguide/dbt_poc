{{ config(schema=var('reports')) }}




WITH date_config AS (
       SELECT
         DATE(DATE_TRUNC('QUARTER', a.date)) - INTERVAL 15 days AS start_date
         , a.date AS end_date
         , DATE(DATE_TRUNC('QUARTER', a.date)) AS quarter_start_date
       FROM (
              SELECT 
                {% if is_incremental() %}
                '{{ var ('start-date') }}'
                {% endif %}
                {% if target.name == 'dev' and not is_incremental() %}
                CURRENT_DATE()
                {% endif %}
                {% if target.name != 'dev' and not is_incremental() %}
                '2020-03-31'
                {% endif %}
                AS date
       ) a
     ),

dates AS (
    SELECT DISTINCT
        *
        , DATE(DATE_TRUNC('QUARTER', date_id)) AS quarter
        , date_id - INTERVAL 6  DAYS AS start_l7
        , date_id - INTERVAL 13 DAYS AS start_l14
        , date_id - INTERVAL 7  DAYS AS end_p7
    FROM {{ source('default', 'dim_date') }}
    CROSS JOIN date_config ON date_id BETWEEN start_date AND end_date
    WHERE 1=1
    {% if target.name == 'dev' and not is_incremental() %}
    LIMIT 1
    {% endif %}

),

quoters AS (
  SELECT
    date
    , user.visitor_id
  FROM {{ source('default', 'events') }}
  WHERE 1=1
    AND event_name IN ('ActivityDetailPageRequest', 'ActivityView')
    AND date BETWEEN (SELECT start_date FROM date_config) AND (SELECT end_date FROM date_config)

),

visitors AS (
  SELECT
    date
    , user.visitor_id
  FROM {{ source('default', 'events') }}
  WHERE 1=1
    AND event_name IN ('AttributionTracking')
    AND date BETWEEN (SELECT start_date FROM date_config) AND (SELECT end_date FROM date_config)

),

visitors_processed AS (
  SELECT
  date_id as date
    , COUNT(DISTINCT CASE WHEN date = date_id THEN visitor_id END) AS visitors
    , COUNT(DISTINCT CASE WHEN date BETWEEN start_l7  AND date_id THEN visitor_id END) AS visitors_last_7_days
    , COUNT(DISTINCT CASE WHEN date BETWEEN start_l14 AND date_id THEN visitor_id END) AS visitors_last_14_days
    , COUNT(DISTINCT CASE WHEN date BETWEEN start_l14 AND end_p7  THEN visitor_id END) AS visitors_prev_7_days
    , COUNT(DISTINCT CASE WHEN date BETWEEN DATE(DATE_TRUNC('QUARTER', date_id)) AND date_id THEN visitor_id ELSE NULL END) AS visitors_qtd

  FROM visitors
  CROSS JOIN dates on visitors.date between start_date and end_date
  group by 1
  order by 1
),

quoters_pr AS (
  SELECT
  date_id as date
    , COUNT(DISTINCT CASE WHEN date = date_id THEN visitor_id END) AS quoters
    , COUNT(DISTINCT CASE WHEN date BETWEEN start_l7  AND date_id THEN visitor_id END) AS quoters_last_7_days
    , COUNT(DISTINCT CASE WHEN date BETWEEN start_l14 AND date_id THEN visitor_id END) AS quoters_last_14_days
    , COUNT(DISTINCT CASE WHEN date BETWEEN start_l14 AND end_p7  THEN visitor_id END) AS quoters_prev_7_days
    , COUNT(DISTINCT CASE WHEN date BETWEEN DATE(DATE_TRUNC('QUARTER', date_id)) AND date_id THEN visitor_id ELSE NULL END) AS quoters_qtd

  FROM quoters
  CROSS JOIN dates on quoters.date between start_date and end_date
  group by 1
  order by 1
)

SELECT
  visitors_processed.*,
  quoters,
  quoters_last_7_days,
  quoters_last_14_days,
  quoters_prev_7_days,
  quoters_qtd

FROM visitors_processed
LEFT JOIN quoters_pr on quoters_pr.date = visitors_processed.date
WHERE visitors_processed.date >= (SELECT quarter_start_date FROM date_config)
