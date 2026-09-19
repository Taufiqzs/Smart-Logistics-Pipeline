-- Berkas dbt/models/staging/stg_driver_events.sql
-- Menyiapkan data event driver dari layer Bronze untuk digunakan oleh model dbt.

select
  cast(event_id as string) as event_id,
  timestamp(event_ts) as event_ts,
  cast(city as string) as city,
  cast(driver_id as string) as driver_id,
  cast(order_id as string) as order_id,
  cast(latitude as float64) as latitude,
  cast(longitude as float64) as longitude,
  cast(expected_delivery_min as int64) as expected_delivery_min,
  cast(actual_delivery_min as int64) as actual_delivery_min,
  cast(delay_min as int64) as delay_min,
  cast(route_rating as float64) as route_rating
from `smart_logistics_bronze.driver_events`
