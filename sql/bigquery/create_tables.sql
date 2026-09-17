CREATE TABLE IF NOT EXISTS `smart_logistics_silver.logistics_events` (
  event_id STRING NOT NULL, event_ts TIMESTAMP, city STRING, driver_id STRING, order_id STRING,
  latitude FLOAT64, longitude FLOAT64, expected_delivery_min INT64, actual_delivery_min INT64,
  delay_min INT64, route_rating FLOAT64, status STRING
) PARTITION BY DATE(event_ts) CLUSTER BY city, driver_id;

CREATE TABLE IF NOT EXISTS `smart_logistics_silver.environment_risk_daily` (
  event_date DATE, city STRING, weather_risk FLOAT64, air_quality_risk FLOAT64, active_weather_alerts INT64
) PARTITION BY event_date CLUSTER BY city;
