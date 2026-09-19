-- Berkas sql/bigquery/create_datasets.sql
-- Membuat dataset BigQuery untuk layer Bronze, Silver, Gold, dan audit.

CREATE SCHEMA IF NOT EXISTS `smart_logistics_bronze`;
CREATE SCHEMA IF NOT EXISTS `smart_logistics_silver`;
CREATE SCHEMA IF NOT EXISTS `smart_logistics_gold`;
CREATE SCHEMA IF NOT EXISTS `smart_logistics_audit`;
