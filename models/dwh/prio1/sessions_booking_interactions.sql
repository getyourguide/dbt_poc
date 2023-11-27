{{ config(schema=var('marketplace_reports')) }}




/*
    Model to see user interactions wih Book related events.
    PK — attribution_session_id.
*/

WITH events_bookings AS (
    SELECT    date
            , event_name
            , event_properties.timestamp AS event_time
            , kafka_timestamp
            , attribution_session_id
            , is_bounce
            , user.visitor_id AS visitor_id
            , header.platform AS platform

              --url
            , header.url AS url

            , booking
            , booking.cart.cart_id   AS shopping_cart_id
            , booking.cart.cart_hash AS booking_cart_hash
            , booking.cart.cart_items.activities.booking_hash_code AS booking_hash_code
            , booking.cart.cart_items.activities.tour_id
            , booking.nr
            , booking.gmv

    FROM   {{ source('default', 'events') }}
    WHERE      1=1
    {% if is_incremental() %}
    AND        date BETWEEN "{{ var ('based-on-events-start-date') }}" AND "{{ var ('end-date') }}"
    {% endif %}
    AND        event_name IN ('AddToCartAction',
                              'BookAction',
                              'CheckoutPageRequest',
                              'CheckoutView')

)
SELECT
       b.date
     , b.attribution_session_id
     , b.visitor_id
     , first(platform, TRUE) AS platform
     , max(is_bounce)        AS is_bounce
     , min(event_time)       AS first_event_time
     , max(event_time)       AS last_event_time
     , COLLECT_SET(b.shopping_cart_id)  AS shopping_cart_ids
     , COLLECT_SET(b.booking_cart_hash) AS booking_cart_hash
       --bookings related flags
     , max(if(b.event_name = 'AddToCartAction', 1, 0))                        AS has_addtocart
     , max(if(b.event_name IN ('CheckoutPageRequest', 'CheckoutView'), 1, 0)) AS has_checkout_request
     , max(if(b.event_name = 'BookAction', 1, 0))                             AS has_bookaction

     , sum(if(b.event_name = 'BookAction', nr, 0))  AS nr
     , sum(if(b.event_name = 'BookAction', gmv, 0)) AS gmv
FROM events_bookings b
GROUP BY 1,2,3