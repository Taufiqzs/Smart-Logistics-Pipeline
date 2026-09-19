-- Berkas sql/bigquery/create_environment_risk_table.sql
-- Membuat tabel Silver untuk menyimpan risiko cuaca harian hasil pemrosesan BMKG.

CREATE TABLE IF NOT EXISTS
`jcdeah-009.smart_logistics_silver.environment_risk_daily`
(
  event_date DATE NOT NULL,
  city STRING NOT NULL,

  weather_risk FLOAT64,
  air_quality_risk FLOAT64,

  active_weather_alerts INT64,

  bmkg_alert_id STRING,
  weather_event STRING,
  severity STRING,
  urgency STRING,
  certainty STRING,
  area_desc STRING,

  effective TIMESTAMP,
  expires TIMESTAMP,

  processed_at TIMESTAMP
)
PARTITION BY event_date
CLUSTER BY city;
