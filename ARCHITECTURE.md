# Architecture — Smart Logistics Weather-Air Quality Risk Pipeline

## Final flow

**BMKG / OpenAQ / Logistics → Airflow + Pub/Sub → GCS Bronze → Dataflow → GCS Silver → BigQuery → dbt Gold → Dashboard / Alerts / Incentives**

### Layer responsibility

| Layer | Technology | Responsibility |
|---|---|---|
| Source | BMKG, OpenAQ, Kaggle, Synthetic | Generate operational/environmental data |
| Orchestration | Airflow / Cloud Composer | Schedule BMKG, trigger batch processing, run dbt, monitor failures |
| Streaming ingestion | Pub/Sub | Buffer OpenAQ + GPS/order events |
| Bronze | GCS | Raw, source-faithful data; daily partitions |
| Processing | Dataflow / Apache Beam | Parse, validate, enrich, join, calculate risk |
| Silver | GCS | Cleaned/enriched records ready for warehouse |
| Warehouse | BigQuery | Partitioned analytical storage |
| Gold | dbt + BigQuery | Business-ready marts for dashboard |
| Serving | Looker Studio / Metabase | Risk trends and operational views |

## Why only one Bronze layer?

GCS is the data lake's raw landing zone. BigQuery is the analytical warehouse, so there is no second BigQuery Bronze layer in this design.

## Batch path

BMKG → Airflow → GCS Bronze → Dataflow batch → GCS Silver → BigQuery → dbt → Gold.

## Streaming path

OpenAQ/GPS → Pub/Sub → Dataflow streaming → GCS Silver → BigQuery → dbt/serving.

A production implementation should keep the streaming Dataflow job long-running and have Airflow monitor/restart it when necessary rather than starting a duplicate stream every few minutes.

## Data quality and failure handling

- Beam parse errors → dead-letter output.
- dbt tests → not-null / accepted values / key checks.
- Airflow retries → transient failure recovery.
- Airflow failure callback → notification integration.
- BigQuery partitioning/clustering → analytical efficiency.
