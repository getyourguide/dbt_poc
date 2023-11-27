{{ config(schema=var('marketplace_reports')) }}




with sessions as (
  select *
  from {{ ref('fact_session') }}
  where
    date between "{{ var ('based-on-events-start-date') }}" - interval 60 days and "{{ var ('end-date') }}"
    and is_discovery
    and not is_office_ip
)

, sessions_with_stats as (
  select
    date
    , session_id
    , nullif(platform_id, 0) as platform_id
    , nullif(landing_page_type_id, 0) as landing_page_type_id
    , count(session_id) over(
        partition by visitor_id
        order by unix_timestamp(started_at) range between 60 * 24 * 3600 preceding and current row
      ) - 1 as prev_sessions
  from sessions
)

select
  date
  , session_id
  , case
      when lpt.landing_page_type = 'Tour' then 'activity'
      when lpt.landing_page_type in ('Poi', 'Poi Category') and p.platform_name = 'mobile' then 'activity'
      when lpt.landing_page_type in ('Poi', 'Poi Category') and p.platform_name = 'desktop' and prev_sessions > 1 then 'activity'
      else 'destination'
    end as visitor_intent
from
  sessions_with_stats as s
  left join {{ source('dwh', 'dim_platform') }} as p on s.platform_id = p.platform_id
  left join {{ source('dwh', 'dim_landing_page_type') }} as lpt on s.landing_page_type_id = lpt.landing_page_type_id
where date between "{{ var ('based-on-events-start-date') }}" and "{{ var ('end-date') }}"