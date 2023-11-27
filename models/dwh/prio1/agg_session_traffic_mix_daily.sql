{{ config(schema=var('marketplace_reports')) }}




with touchpoints as (
  select *
  from {{ ref('fact_touchpoint') }}
  where date between "{{ var ('based-on-events-start-date') }}" - interval 5 day and "{{ var ('end-date') }}"
)

, sessions as (
  select *
  from {{ ref('fact_session') }}
  where
    date between "{{ var ('based-on-events-start-date') }}" and "{{ var ('end-date') }}"
    and is_discovery
    and not is_office_ip
)

select
  s.date
  , coalesce(nullif(t.landing_page_type_id, 0), nullif(s.landing_page_type_id, 0)) as landing_page_type_id
  , t.channel_id
  , count(distinct s.session_id) as sessions
  , count(distinct s.visitor_id) as visitors
from
  sessions as s
  left join touchpoints as t on t.date between s.date - interval 5 day and s.date and s.touchpoint_id = t.touchpoint_id
group by 1, 2, 3