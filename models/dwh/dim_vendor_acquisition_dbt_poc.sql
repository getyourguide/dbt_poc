{{
    config(
        materialized='incremental',
        partition_by='registration_date',
        incremental_strategy='append'
    )
}}

{% set start_date = '2023-11-23' %}

with events as (

        select {{ source('dbt_poc', 'events') }} where date >= {{ start_date }}

        {% if is_incremental() %}
                -- this filter will only be applied on an incremental run
                 -- (uses > to include records whose timestamp occurred since the last run of this model)
           and date > (select max(date) from {{ this }})

        {% endif %}

),

supplier_registration as (

    select

        ui.metadata: supplier_id as supplier_id,
        date as registration_date,
        event_properties.timestamp as event_timestamp,
        case
            when current_touchpoint.header.domain = '{{ var("SUPPLIER_DOMAIN") }}' then current_touchpoint.event_properties.uuid
        else 'virtual_' || event_properties.uuid
        end as acquisition_touchpoint_id,
        event_properties.uuid as registration_event_id

    from events

    where 1=1

        and header.domain = '{{ var("SUPPLIER_DOMAIN") }}'

        and {{ is_supplier_register_action() }}

        and ui.metadata: supplier_id is not null

),


final as (

    select

        supplier_id as vendor_id,
        '{{var("SUPPLIER")}}' as vendor_type,
        max(registration_date) as registration_date,
        max(event_timestamp) as registration_timestamp,
        {{ get_latest_id('event_timestamp', 'acquisition_touchpoint_id') }},
        {{ get_latest_id('event_timestamp', 'registration_event_id') }}

    from supplier_registration

    {{ dbt_utils.group_by(2) }}

)

select * from final
