{% set events = ['tourcreationstarted', 'toursubmitted', 'tourapproved', 'firstbooking']  %}

with funnel_events as (

    select * from {{ source('dbt_poc', 'agg_selection_tour_funnel_events')}}

)
select

    tour_id,
    {% for item in events %}

        min(case when lower(funnel_step) = '{{item}}' then event_timestamp else null end) as date_{{item}}{% if not loop.last %}, {% endif %}

    {% endfor  %}

   from funnel_events

    {{dbt_utils.group_by(1)}}
    