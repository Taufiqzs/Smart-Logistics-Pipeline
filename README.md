# Smart Logistics Weather-Air Quality Risk Pipeline

Final Project Data Engineering - Purwadhika

## 1. Business problem

Perusahaan logistik/ride-hailing membutuhkan visibilitas risiko operasional per area dan waktu dengan menggabungkan:
- peringatan dini cuaca BMKG,
- kualitas udara OpenAQ,
- aktivitas order/driver logistik.

Output utama adalah `risk_score` 0-100 per kota/area/time window yang dapat digunakan untuk dashboard, early warning, rerouting, dan insentif kondisi ekstrem.

## 2. Architecture

```text
BMKG CAP -------- Airflow batch --------\
                                          \
OpenAQ -------- Pub/Sub streaming --------> GCS Bronze
                                           |
Synthetic GPS/Orders -- Pub/Sub ----------/
                                           |
                                      Dataflow / Beam
                                  clean + standardize +
                                  enrich + risk score
                                           |
                                           v
                                     GCS Silver
                                           |
                                           v
                                      BigQuery
                                           |
                                           v
                                         dbt
                                   Gold dashboard marts
                                           |
                              +------------+-------------+
                              |                          |
                         Looker Studio              Alert/ops
```

The same Apache Beam code is designed to support batch and streaming modes. Pub/Sub is used for event ingestion, while GCS is the durable raw/staging layer.

## 3. Data sources

### BMKG
BMKG publishes weather early-warning data using Common Alerting Protocol (CAP), including affected subdistricts. The public endpoint is:
`https://www.bmkg.go.id/alerts/nowcast/id`

BMKG states that the nowcast data are updated continuously and access is limited to 60 requests/minute/IP. Attribution to BMKG is required.

### OpenAQ
OpenAQ API v3 requires an API key. This project uses location/latest data for near-real-time ingestion. Do not assume that a "latest" value is complete historical coverage; OpenAQ explicitly notes that latest values may not guarantee complete time-series coverage.

### Logistics
Use the specified public Kaggle Transportation and Logistics Tracking Dataset as historical/batch input. Because the exact Kaggle file name can vary, place the downloaded CSV under `data/raw/logistics/`.

Synthetic GPS/order events are generated with Python to simulate real-time operations.

## 4. Risk model

The project intentionally uses an explainable weighted score instead of ML:

```text
risk_score =
    0.45 * weather_risk
  + 0.35 * air_quality_risk
  + 0.20 * logistics_risk
```

Each component is normalized to 0-100.

Weather:
- severity/severity keywords from BMKG CAP
- alert active/expired
- affected area

Air quality:
- PM2.5 / PM10 when available
- normalized using configurable thresholds

Logistics:
- delay rate
- average actual delivery time vs expected delivery time

Risk bands:
- 0-24: LOW
- 25-49: MODERATE
- 50-74: HIGH
- 75-100: CRITICAL

These thresholds are project assumptions and should be documented to the mentor rather than presented as medical or regulatory thresholds.

## 5. Repository structure

```text
smart-logistics-weather-air-quality/
├── dags/
│   └── smart_logistics_pipeline.py
├── data/
│   └── raw/logistics/.gitkeep
├── docker/
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml.example
│   └── models/
│       ├── staging/
│       └── marts/
├── schemas/
│   └── bigquery/
├── src/
│   ├── ingestion/
│   │   ├── bmkg_ingest.py
│   │   ├── openaq_publisher.py
│   │   └── synthetic_events.py
│   ├── beam/
│   │   └── risk_pipeline.py
│   └── common/
│       └── config.py
├── tests/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

## 6. Local quick start

### Prerequisites
- Docker Desktop
- Python 3.11+
- GCP project for cloud execution
- OpenAQ API key
- `gcloud` CLI for cloud deployment

### Environment

```bash
cp .env.example .env
# edit .env
```

### Generate synthetic logistics events

```bash
python -m src.ingestion.synthetic_events --rows 5000 --output data/raw/logistics/synthetic_orders.csv
```

### Test BMKG ingestion

```bash
python -m src.ingestion.bmkg_ingest --output data/raw/bmkg
```

### Test OpenAQ publisher

```bash
python -m src.ingestion.openaq_publisher --once
```

### Start Airflow

```bash
docker compose up airflow-init
docker compose up -d
```

Airflow UI: http://localhost:8080

Default development login:
- user: `airflow`
- password: `airflow`

## 7. GCP setup

Create APIs:

```bash
gcloud services enable \
  storage.googleapis.com \
  pubsub.googleapis.com \
  dataflow.googleapis.com \
  bigquery.googleapis.com \
  composer.googleapis.com
```

Create buckets:

```bash
gsutil mb -l asia-southeast2 gs://$GCP_PROJECT_ID-smart-logistics-raw
gsutil mb -l asia-southeast2 gs://$GCP_PROJECT_ID-smart-logistics-silver
gsutil mb -l asia-southeast2 gs://$GCP_PROJECT_ID-smart-logistics-temp
```

Create Pub/Sub topics:

```bash
gcloud pubsub topics create openaq-events
gcloud pubsub topics create driver-events
```

The production deployment should use Secret Manager for `OPENAQ_API_KEY`; `.env` is only for local development.

## 8. Bronze naming convention

Monthly source files must be normalized into daily partitions:

```text
gs://PROJECT-smart-logistics-raw/
  source=bmkg/
    ingestion_date=2026-09-16/
      bmkg_20260916T110000Z.xml

  source=openaq/
    ingestion_date=2026-09-16/
      openaq_20260916T110000Z.jsonl

  source=logistics/
    event_date=2026-09-16/
      logistics_20260916.jsonl

  source=driver/
    event_date=2026-09-16/
      driver_20260916.jsonl
```

This satisfies the requirement to turn monthly/bulk source files into daily storage partitions without altering the original raw records.

## 9. BigQuery layers

Recommended datasets:

```text
smart_logistics_bronze
smart_logistics_silver
smart_logistics_gold
smart_logistics_audit
```

Gold tables:

```text
fact_area_risk_daily
fact_delivery_performance
fact_driver_risk_event
dim_area
dim_date
```

Dashboard queries should read only from Gold.

## 10. Data quality

Minimum checks:
1. required columns not null,
2. event timestamps valid,
3. latitude/longitude within Indonesia bounds,
4. risk score between 0 and 100,
5. no duplicate event_id,
6. weather alerts have valid severity,
7. delivery time >= 0,
8. dashboard table is not stale.

dbt tests are included for key fields.

## 11. Dashboard

Minimum charts:
1. Line: daily risk score by city for last 30 days.
2. Bar/heatmap: current risk by city/area.

Optional:
3. Scatter: risk score vs actual delivery time.
4. KPI cards: current critical areas, average risk, active weather alerts, average delay.

Suggested dashboard title:

**Smart Logistics Environmental Risk Monitor**

Problem statement should be visible on the dashboard:
> Mengidentifikasi area dan waktu berisiko tinggi dengan menggabungkan cuaca, kualitas udara, dan aktivitas logistik agar keputusan operasional dapat dilakukan lebih cepat dan berbasis data.

## 12. Failure alert

Airflow DAG has failure callbacks. In production, replace the placeholder callback with:
- email,
- Slack webhook,
- Google Chat webhook,
- PagerDuty,
or another approved notification channel.

## 13. Mentor presentation storyline

1. Business problem
2. Why three data sources are needed
3. Why batch + streaming
4. Why GCS is the raw source of truth
5. Why Dataflow/Beam is used for common processing logic
6. Why BigQuery is the analytical warehouse
7. Why dbt is used for final business modeling
8. Explain the risk formula
9. Demonstrate one BMKG alert and one OpenAQ event
10. Show how a synthetic driver/order event changes operational risk
11. Show dashboard
12. Explain data quality and failure alerting
13. Explain scalability and cost considerations
14. Explain limitations and future improvements
