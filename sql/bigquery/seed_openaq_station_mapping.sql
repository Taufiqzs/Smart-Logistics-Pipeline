-- Berkas sql/bigquery/seed_openaq_station_mapping.sql
-- Mengisi pemetaan station OpenAQ berdasarkan kota operasional yang digunakan proyek.

INSERT INTO
`jcdeah-009.smart_logistics_silver.openaq_station_mapping`
(
    location_id,
    city,
    mapping_method,
    notes
)

VALUES
    -- Kota Jakarta
    (2537, 'Jakarta', 'explicit_location', 'US Diplomatic Post: Jakarta South'),
    (2538, 'Jakarta', 'explicit_location', 'US Diplomatic Post: Jakarta Central'),
    (8320, 'Jakarta', 'explicit_location', 'Jakarta South'),
    (8637, 'Jakarta', 'explicit_location', 'Jakarta Central'),
    (223262, 'Jakarta', 'explicit_location', 'Cilandek'),
    (223824, 'Jakarta', 'explicit_location', 'Cilandek'),
    (1776543, 'Jakarta', 'explicit_location', 'Jakarta location'),
    (1894639, 'Jakarta', 'explicit_location', 'Jakarta location'),
    (2071976, 'Jakarta', 'explicit_location', 'Krukut'),
    (3276989, 'Jakarta', 'explicit_location', 'Cipete Jakarta'),
    (3280117, 'Jakarta', 'explicit_location', 'Cipete Jakarta'),
    (3294802, 'Jakarta', 'explicit_location', 'Cipete Dec'),
    (6051931, 'Jakarta', 'explicit_location', 'West Jakarta Mayor Office'),
    (6051932, 'Jakarta', 'explicit_location', 'East Jakarta Mayor Office'),
    (6051933, 'Jakarta', 'explicit_location', 'Rusunawa Marunda'),
    (6409860, 'Jakarta', 'explicit_location', 'Jakarta home open storage room'),
    (6455006, 'Jakarta', 'explicit_location', 'BMKG 1'),

    -- Kota Bandung
    (1285347, 'Bandung', 'explicit_location', 'SPARTAN - ITB Bandung'),
    (3056210, 'Bandung', 'explicit_location', 'Pasteur Gateway Apartment'),

    -- Kota Yogyakarta
    (3037147, 'Yogyakarta', 'explicit_location', 'Wedomartani'),
    (4047633, 'Yogyakarta', 'explicit_location', 'Yogyakarta'),

    -- Kota Medan
    (5586536, 'Medan', 'explicit_location', 'USU');
