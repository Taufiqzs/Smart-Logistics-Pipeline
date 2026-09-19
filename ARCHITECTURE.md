# Arsitektur — Smart Logistics Weather-Air Quality Risk Pipeline

## 1. Gambaran Umum

Project menggunakan arsitektur hybrid batch dan streaming pada Google Cloud Platform.

```text
                           BATCH

 BMKG CAP
    |
    v
 Airflow
    |
    v
 GCS BRONZE
    |
    v
 Pemrosesan BMKG
    |
    v
 BigQuery SILVER


                         STREAMING

 OpenAQ API
    |
    v
 Python Publisher
    |
    v
 Pub/Sub
    |
    v
 Dataflow / Apache Beam
    |
    v
 GCS SILVER
    |
    v
 BigQuery / External Table


                     DATA OPERASIONAL

 Dataset Logistik ------> GCS BRONZE / BigQuery
 Data Sintetis ----------> GCS BRONZE / BigQuery

 Semua sumber analitik
          |
          v
      dbt Transformation
          |
          v
   BigQuery GOLD MART
          |
          v
      Looker Studio

 Airflow Task Failure
          |
          v
   on_failure_callback
          |
          v
      Gmail SMTP
```

## 2. Tanggung Jawab Setiap Lapisan

| Lapisan | Teknologi | Tanggung Jawab |
|---|---|---|
| Sumber | BMKG, OpenAQ, Kaggle, data sintetis | Menyediakan data lingkungan dan operasional |
| Orkestrasi | Apache Airflow | Menjalankan proses batch, menjalankan dbt, dan memantau kegagalan task |
| Message broker | Pub/Sub | Menampung event streaming OpenAQ dan event operasional |
| Bronze / raw lake | GCS | Menyimpan data sumber dengan bentuk sedekat mungkin dengan sumber asli |
| Pemrosesan streaming | Dataflow / Apache Beam | Parsing, validasi, enrichment, freshness check, windowing, dan penulisan Silver |
| Silver | GCS + BigQuery | Menyimpan data yang sudah dibersihkan dan diperkaya untuk kebutuhan analitik |
| Warehouse | BigQuery | Menyediakan penyimpanan analitik yang dapat dipartisi dan di-cluster |
| Gold | dbt + BigQuery | Membentuk model bisnis dan metrik yang siap digunakan dashboard |
| Serving | Looker Studio | Menampilkan tren risiko dan informasi operasional |
| Alert | Airflow callback + Gmail SMTP | Mengirim notifikasi email ketika task Airflow gagal |

## 3. Mengapa Bronze Berada di GCS?

GCS berfungsi sebagai data lake dan raw landing zone. Data sumber dapat dipertahankan untuk kebutuhan audit dan pemrosesan ulang.

BigQuery berfungsi sebagai analytical warehouse. Karena itu project tidak membuat lapisan Bronze fisik kedua di BigQuery hanya untuk mengikuti pola medallion.

## 4. Jalur Batch BMKG

```text
BMKG CAP
   |
   v
bmkg_ingest.py
   |
   v
GCS Bronze
   |
   v
bmkg_weather_risk.py
   |
   v
load_bmkg_weather.py
   |
   v
BigQuery Silver
   |
   v
DBT Gold
```

Airflow mengorkestrasi tiga tahap BMKG tersebut.

## 5. Jalur Streaming OpenAQ

```text
OpenAQ API
   |
   v
openaq_publisher.py
   |
   v
Pub/Sub: openaq-events
   |
   v
Dataflow / Apache Beam
   |
   +---- valid ----> GCS Silver
   |
   +---- invalid --> audit/dead-letter output
```

Dataflow melakukan:

1. parsing pesan Pub/Sub;
2. pemeriksaan field wajib;
3. validasi parameter PM2.5;
4. validasi nilai pengukuran;
5. validasi koordinat Indonesia;
6. parsing timestamp;
7. perhitungan `measurement_age_hours`;
8. penentuan `freshness_status`;
9. penulisan data valid ke GCS Silver;
10. penulisan data invalid ke jalur audit.

## 6. Jalur Data Logistik

Dataset historis dan data sintetis digunakan sebagai input operasional/batch.

Data tersebut digunakan untuk membentuk `fact_delivery_performance`, termasuk metrik:

- jumlah order;
- rata-rata actual delivery time;
- rata-rata expected delivery time;
- rata-rata delay;
- delay rate;
- rata-rata route rating.

## 7. Business Modeling

dbt menggabungkan tiga sinyal utama:

```text
Weather Risk
     |
     +----------------+
                      |
Air Quality Risk -----+----> fact_area_risk_daily
                      |
Logistics Risk -------+
```

Formula:

```text
risk_score =
    0.45 * weather_risk
  + 0.35 * air_quality_risk
  + 0.20 * logistics_risk
```

Jika seluruh komponen tidak tersedia, hasil diberi status `INSUFFICIENT_DATA`.

## 8. Pemetaan OpenAQ

Tidak semua lokasi OpenAQ langsung dipetakan ke kota operasional. Project menggunakan tabel:

```text
smart_logistics_silver.openaq_station_mapping
```

Pendekatan ini mencegah nama lokasi yang ambigu atau koordinat yang tidak sesuai wilayah Indonesia digunakan secara langsung dalam perhitungan risiko kota.

## 9. Orkestrasi Airflow

DAG utama:

```text
smart_logistics_pipeline
```

Urutan dependency:

```text
ingest_bmkg
    |
    v
process_bmkg_weather
    |
    v
load_bmkg_weather
    |
    +-------------------+
                        |
ingest_logistics --------+----> run_dbt ---> test_dbt
```

Airflow tidak digunakan untuk menjalankan streaming Dataflow secara berulang. Streaming Dataflow dirancang berjalan sebagai job streaming yang berkelanjutan.

## 10. Failure Alert

```text
Airflow Task
     |
     | gagal
     v
on_failure_callback
     |
     v
notify_pipeline_failure()
     |
     v
Gmail SMTP
     |
     v
Email Penerima
```

Email memuat informasi DAG, task, Run ID, waktu eksekusi, detail exception, dan tautan ke log Airflow.

## 11. Kualitas Data dan Penanganan Kegagalan

- Parsing atau validasi Beam yang gagal diarahkan ke output audit.
- Data OpenAQ yang terlalu lama diberi status `STALE`.
- Data OpenAQ dengan parameter selain PM2.5 tidak digunakan sebagai input risiko PM2.5.
- dbt memeriksa `not_null`, `accepted_values`, rentang skor, dan konsistensi risk band.
- Airflow failure callback mengirim email ketika task gagal.
- BigQuery menggunakan partitioning dan clustering pada tabel yang sesuai untuk membantu efisiensi query.

## 12. Autentikasi dan Secret

Untuk lingkungan lokal, credential Google dapat menggunakan Application Default Credentials.

Pada container Airflow, path credential menggunakan:

```text
/home/airflow/.config/gcloud/application_default_credentials.json
```

Path tersebut dipetakan dari credential Google pada host melalui volume Docker.

Secret seperti API key OpenAQ, Gmail App Password, dan JWT secret tidak boleh dimasukkan ke Git.

## 13. Batasan Desain

Risk score, bobot, dan normalisasi PM2.5 merupakan asumsi desain project yang dibuat agar mudah dijelaskan dan diuji.

Data logistik sintetis digunakan untuk mensimulasikan kondisi operasional karena data operasional internal tidak tersedia sebagai dataset publik.

Ketersediaan stasiun OpenAQ juga berbeda antarwilayah sehingga `INSUFFICIENT_DATA` tetap dipertahankan sebagai status yang valid pada layer Gold.
