{{ config(schema=var('public')) }}




-- defining public.dim_catalog_tour_location = replicating db_mirror_dbz.catalog__tour_to_location
select
    id,
    entrance_type,
    access_mode,
    location_id,
    tag_type,
    update_timestamp,
    update_user_id,
    tour_id
from {{ source('db_mirror_dbz', 'catalog__tour_to_location') }}
