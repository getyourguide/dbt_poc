{% set events = [
    'TourCreationStarted',
    'TourContentAdded',
    'TourSubmitted',
    'TourApproved',
    'FirstBooking',
    'ThirdReview'
    ] %}
with funnel_events as (

    select * from {{ source('dbt_poc', 'agg_selection_tour_funnel_events')}}


)
select

    tour_id,
    {% for item in ['tourcreationstarted', 'toursubmitted', 'tourapproved', 'firstbooking'] %}

        min(case when funnel_step = lower({{item}}) then date_id else null end) as date_{{item}}
    {% endfor  %}

   from

