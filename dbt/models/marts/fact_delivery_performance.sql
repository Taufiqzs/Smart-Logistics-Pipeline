select
  date(event_ts) as event_date,
  city,
  count(distinct order_id) as orders,
  avg(actual_delivery_min) as avg_actual_delivery_min,
  avg(expected_delivery_min) as avg_expected_delivery_min,
  avg(delay_min) as avg_delay_min,
  safe_divide(countif(delay_min > 0), count(*)) as delay_rate,
  avg(route_rating) as avg_route_rating
from {{ ref('stg_driver_events') }}
group by 1, 2
