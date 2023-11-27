{{ config(schema=var('marketplace_reports')) }}



select
  booking.booking_id
  , booking.customer_id
  , date(booking.date_of_checkout) as booking_checkout_date
  , trip.starts_on as trip_starts_on
  , trip.ends_on as trip_ends_on
  , trip.trip_number
  , row_number() over(partition by booking.customer_id, trip.trip_number order by booking.date_of_checkout, booking.booking_id) as booking_in_trip_number
from
  {{ source('dwh', 'fact_booking') }} as booking
  left join {{ source('dwh', 'dim_reseller') }} as reseller on booking.reseller_id = reseller.reseller_id
  left join {{ source('dwh', 'fact_customer_trip') }} as trip on
    booking.customer_id = trip.customer_id
    and date(booking.date_of_checkout) between trip.starts_on and trip.ends_on
where
  booking.status_id in (1, 2)
  and booking.customer_id > 0
  and booking.date_of_checkout is not null
  and (reseller.channel is null or reseller.channel not in ("travel_agent", "tour_operator"))