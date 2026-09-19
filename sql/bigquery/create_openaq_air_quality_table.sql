-- Berkas sql/bigquery/create_openaq_air_quality_table.sql
-- Membuat tabel Silver untuk menyimpan agregasi kualitas udara harian OpenAQ.

CREATE TABLE IF NOT EXISTS
`jcdeah-009.smart_logistics_silver.openaq_air_quality_daily`
(
    event_date DATE NOT NULL,
    city STRING NOT NULL,
    avg_pm25 FLOAT64,
    air_quality_risk FLOAT64,
    station_count INT64,
    observation_count INT64,
    processed_at TIMESTAMP
)
PARTITION BY event_date
CLUSTER BY city;
