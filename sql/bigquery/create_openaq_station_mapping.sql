-- Berkas sql/bigquery/create_openaq_station_mapping.sql
-- Membuat tabel pemetaan station OpenAQ ke kota operasional proyek.

CREATE TABLE IF NOT EXISTS
`jcdeah-009.smart_logistics_silver.openaq_station_mapping`
(
    location_id INT64 NOT NULL,
    city STRING NOT NULL,
    mapping_method STRING NOT NULL,
    notes STRING
);
