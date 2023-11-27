{{ config(schema=var('dwh')) }}




-- This transformation is used to compute is_new_visitor in dwh.fact_touchpoint, we need timestamp in order to
-- determine the visitor's status at a given timestamp.
-- Excluding AppInstall events as they still exist in the old period's dataset
SELECT DISTINCT DATE
  , event_properties.TIMESTAMP
  , USER.visitor_id
FROM {{ source('default', 'fact_attribution') }}
WHERE 1 = 1
  AND DATE BETWEEN "{{ var ('touchpoint-start') }}" AND "{{ var ('touchpoint-end') }}"
  AND COALESCE(event_properties.event_name, '') <> 'AppInstall' -- exclude appinstall events
  AND virtual_touchpoint IS NULL -- exclude all types of virtual touchpoints, we only consider real touchpoints
  AND touchpoint_id IS NOT NULL
