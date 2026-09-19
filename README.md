# Smart Logistics Weather-Air Quality Risk Pipeline

**Final Project Data Engineering — Purwadhika**

## 1. Ringkasan Project

**Smart Logistics Weather-Air Quality Risk Pipeline** adalah pipeline data engineering end-to-end yang menggabungkan data lingkungan dan data operasional logistik untuk membentuk gambaran risiko operasional berdasarkan kota/area dan waktu.

Pipeline menggabungkan empat sumber utama:

- peringatan dini cuaca dari **BMKG**;
- kualitas udara **PM2.5 dari OpenAQ**;
- dataset logistik historis dari **Transportation and Logistics Tracking Dataset**;
- data order/driver sintetis untuk mensimulasikan aktivitas operasional.

Output utama adalah `risk_score` dengan rentang 0–100 beserta `risk_band`. Hasil akhir disimpan pada BigQuery Gold dan dapat digunakan oleh Looker Studio untuk pemantauan risiko operasional.

> **Catatan:** skor risiko, bobot, dan normalisasi PM2.5 pada project ini merupakan asumsi desain project. Nilai tersebut bukan standar medis atau regulasi.

---

## 2. Permasalahan Bisnis

Perusahaan logistik/ride-hailing membutuhkan visibilitas risiko operasional berdasarkan area dan waktu. Risiko tidak hanya dipengaruhi oleh kondisi internal seperti keterlambatan pengiriman, tetapi juga oleh kondisi eksternal seperti cuaca dan kualitas udara.

Pipeline ini dirancang untuk menggabungkan sinyal tersebut sehingga tersedia satu sumber data analitik yang dapat digunakan untuk:

- memantau kondisi risiko per kota;
- melihat perubahan risiko dari waktu ke waktu;
- mendukung early warning operasional;
- menjadi dasar analisis keterlambatan pengiriman;
- menyediakan data terstruktur untuk dashboard.

---

## 3. Tujuan Project

Tujuan utama pipeline:

1. menyediakan visibilitas risiko operasional berdasarkan area;
2. menggabungkan data lingkungan dan data operasional dalam satu model analitik;
3. memproses data batch dan streaming dalam satu arsitektur;
4. menerapkan validasi kualitas data sebelum data digunakan pada layer analitik;
5. menghasilkan tabel Gold yang siap digunakan dashboard;
6. memberikan notifikasi email ketika task pada pipeline Airflow gagal.

---

## 4. Arsitektur Sistem

Arsitektur aktual project terdiri dari jalur batch dan jalur streaming.

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
 BMKG Processing
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
          |
          v
     Email Penerima
```

### Prinsip arsitektur

- **GCS** digunakan sebagai data lake untuk data Bronze/raw dan Silver hasil pemrosesan streaming.
- **BigQuery** digunakan sebagai analytical warehouse.
- **dbt** digunakan untuk membentuk model bisnis pada layer Gold.
- **Airflow** digunakan untuk orkestrasi proses batch dan pemantauan kegagalan task.
- **Pub/Sub** digunakan sebagai message broker untuk event streaming.
- **Dataflow/Apache Beam** digunakan untuk parsing, validasi, enrichment, windowing, dan penulisan data streaming.
- **Looker Studio** digunakan sebagai layer visualisasi.
- **Cloud Composer tidak diperlukan** dalam desain project ini karena project menggunakan Apache Airflow yang dijalankan melalui Docker.

### Mengapa tidak membuat Bronze fisik kedua di BigQuery?

GCS berfungsi sebagai data lake dan raw landing zone. BigQuery berfungsi sebagai analytical warehouse. Karena itu, project tidak membuat salinan fisik layer Bronze di BigQuery hanya untuk mengikuti istilah medallion.

---

## 5. Sumber Data

### 5.1 BMKG

BMKG menyediakan data peringatan dini cuaca menggunakan **Common Alerting Protocol (CAP)**.

Endpoint publik yang digunakan:

```text
https://www.bmkg.go.id/alerts/nowcast/id
```

Proses ingestion:

```text
BMKG CAP/RSS
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
BigQuery Silver
```

Data CAP diproses untuk mengambil informasi seperti:

- identifier alert;
- event;
- severity;
- urgency;
- certainty;
- waktu efektif;
- waktu kedaluwarsa;
- area terdampak.

### 5.2 OpenAQ

OpenAQ digunakan sebagai sumber kualitas udara mendekati real-time.

Pipeline hanya menggunakan observasi **PM2.5** dengan `parameter_id = 2`.

Alur aktual:

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
    +--> valid
    |       |
    |       v
    |    GCS Silver
    |
    +--> invalid
            |
            v
          Audit
```

Validasi yang diterapkan antara lain:

- parameter harus PM2.5;
- nilai pengukuran tidak boleh negatif atau NaN;
- koordinat harus berada dalam batas geografis Indonesia;
- timestamp pengukuran harus dapat diproses;
- status freshness dihitung berdasarkan usia data.

Project membedakan status freshness menjadi `CURRENT`, `STALE`, dan `FUTURE` sesuai hasil validasi pipeline.

**Data stale tidak dianggap sebagai bukti bahwa kualitas udara sedang baik.** Jika data terkini tidak tersedia, kondisi tersebut harus diperlakukan sebagai keterbatasan ketersediaan data.

### 5.3 Dataset Logistik Historis

Dataset **Transportation and Logistics Tracking Dataset** digunakan sebagai baseline historis/batch untuk analisis performa pengiriman.

Dataset ini bukan sumber real-time.

### 5.4 Data Order/Driver Sintetis

Data sintetis dibuat dengan Python untuk mensimulasikan aktivitas operasional seperti:

- order;
- lokasi/kota;
- waktu pengiriman;
- expected delivery time;
- actual delivery time;
- delay;
- rating rute.

Data sintetis digunakan untuk kebutuhan pengembangan dan demonstrasi pipeline karena data operasional internal tidak tersedia sebagai dataset publik.

---

## 6. Model Data dan Layer

### 6.1 GCS Bronze

Bronze digunakan untuk mempertahankan data sumber dalam bentuk sedekat mungkin dengan bentuk aslinya.

Contoh struktur aktual:

```text
gs://jcdeah-009-smart-logistics-raw/
├── bmkg/
│   └── weather_warning/
│       ├── feed/
│       └── cap/
├── logistics/
│   └── historical/
└── logistics/
    └── synthetic/
```

### 6.2 GCS Silver

Silver digunakan untuk data streaming yang telah melalui parsing, validasi, dan enrichment.

Contoh:

```text
gs://jcdeah-009-smart-logistics-silver/
└── openaq/
    └── events-*.jsonl
```

### 6.3 BigQuery Silver

Dataset BigQuery Silver digunakan untuk data yang telah dipersiapkan untuk analitik, termasuk:

```text
smart_logistics_silver.environment_risk_daily
smart_logistics_silver.openaq_air_quality_daily
smart_logistics_silver.openaq_station_mapping
smart_logistics_silver.openaq_events_external
```

### 6.4 BigQuery Gold

Model Gold dibuat menggunakan dbt.

Model utama:

```text
smart_logistics_gold.fact_delivery_performance
smart_logistics_gold.fact_area_risk_daily
```

---

## 7. Model Risiko

Project menggunakan weighted score yang sederhana dan mudah dijelaskan, bukan machine learning.

```text
risk_score =
    0.45 * weather_risk
  + 0.35 * air_quality_risk
  + 0.20 * logistics_risk
```

Setiap komponen memiliki rentang 0–100.

### 7.1 Risiko Cuaca

Pemetaan severity BMKG:

```text
minor     = 25
moderate  = 50
severe    = 80
extreme   = 100
```

### 7.2 Risiko Kualitas Udara

Risiko kualitas udara dihitung dari nilai PM2.5 dan dinormalisasi ke 0–100.

Normalisasi ini merupakan **asumsi desain project**, bukan batas medis atau batas regulasi kualitas udara.

### 7.3 Risiko Logistik

```text
delay_rate = delayed_orders / total_orders
logistics_risk = min(delay_rate * 100, 100)
```

### 7.4 Kategori Risiko

```text
0–24    LOW
25–49   MODERATE
50–74   HIGH
75–100  CRITICAL
```

Jika seluruh komponen risiko tidak tersedia, model menghasilkan:

```text
INSUFFICIENT_DATA
```

Model juga mempertahankan flag ketersediaan data seperti:

```text
has_weather_data
has_air_quality_data
```

Hal ini mencegah kondisi data yang hilang langsung dianggap sebagai kondisi operasional yang aman.

---

## 8. Kualitas Data

Validasi utama pada pipeline meliputi:

1. kolom wajib tidak boleh null;
2. timestamp harus dapat diproses;
3. latitude dan longitude harus berada pada batas geografis Indonesia;
4. nilai PM2.5 tidak boleh negatif atau NaN;
5. hanya parameter PM2.5 yang digunakan pada jalur OpenAQ;
6. `validation_status` menunjukkan hasil validasi data;
7. `freshness_status` menunjukkan kesegaran observasi OpenAQ;
8. `risk_score` harus berada pada rentang 0–100;
9. `risk_band` harus konsisten dengan rentang `risk_score`;
10. pengujian dbt memeriksa constraint dan business rule utama.

### OpenAQ freshness

OpenAQ dapat menyediakan data dengan usia yang berbeda-beda. Karena itu pipeline menyimpan:

```text
measurement_age_hours
freshness_status
```

Data yang terlalu lama diberi status `STALE` dan tidak digunakan sebagai representasi kondisi udara terkini.

---

## 9. Pemetaan Stasiun OpenAQ

Tidak semua lokasi OpenAQ otomatis dianggap sebagai kota operasional project.

Project menggunakan tabel pemetaan eksplisit:

```text
smart_logistics_silver.openaq_station_mapping
```

Pemetaan saat ini mencakup kota operasional yang digunakan model analitik, seperti:

- Jakarta;
- Bandung;
- Yogyakarta;
- Medan.

Pendekatan ini digunakan untuk menghindari asumsi bahwa nama lokasi OpenAQ selalu cukup untuk menentukan kota operasional.

---

## 10. Struktur Repositori

Struktur utama project:

```text
smart-logistics-weather-air-quality/
├── dags/
│   ├── smart_logistics_pipeline.py
│   └── test_email_alert.py
├── data/
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml.example
│   ├── models/
│   └── tests/
├── schemas/
├── sql/
│   └── bigquery/
├── src/
│   ├── ingestion/
│   ├── processing/
│   ├── beam/
│   └── common/
├── tests/
├── Dockerfile
├── docker-compose.yml
├── docker-compose.airflow.yml
├── requirements.txt
└── .env.example
```

### Modul penting

```text
src/ingestion/bmkg_ingest.py
```
Mengambil dan menyimpan data peringatan BMKG ke GCS Bronze.

```text
src/ingestion/openaq_publisher.py
```
Mengambil observasi OpenAQ yang relevan dan menerbitkannya ke Pub/Sub.

```text
src/beam/openaq_streaming_dataflow.py
```
Pipeline Apache Beam untuk membaca Pub/Sub, memvalidasi data OpenAQ, memberi status freshness, dan menulis hasil ke GCS Silver.

```text
src/processing/bmkg_weather_risk.py
```
Mengubah data CAP BMKG menjadi data risiko cuaca berdasarkan severity.

```text
src/processing/load_bmkg_weather.py
```
Memuat hasil risiko cuaca ke BigQuery Silver.

```text
src/common/airflow_alerts.py
```
Mengirim email ketika task Airflow gagal menggunakan Gmail SMTP.

```text
dbt/models/marts/fact_area_risk_daily.sql
```
Membentuk model Gold untuk menggabungkan weather risk, air quality risk, dan logistics risk.

> Nama file dan modul yang ditampilkan di atas adalah identifier teknis dan dipertahankan sesuai repository.

---

## 11. Menjalankan Secara Lokal

### 11.1 Prasyarat

- Docker Desktop;
- Python 3.11+;
- virtual environment Python;
- project GCP;
- Google Cloud CLI (`gcloud`);
- kredensial Application Default Credentials (ADC);
- API key OpenAQ untuk akses API sesuai konfigurasi project.

### 11.2 Membuat Environment

```cmd
copy .env.example .env
```

Kemudian isi nilai rahasia pada `.env`.

**Jangan commit `.env` yang berisi password, App Password Gmail, API key, JWT secret, atau credential lainnya.**

### 11.3 Membuat Data Logistik Sintetis

```cmd
python -m src.ingestion.synthetic_events --rows 5000 --output data/raw/logistics/synthetic_orders.csv
```

### 11.4 Menjalankan Ingestion BMKG

```cmd
python -m src.ingestion.bmkg_ingest
```

Script akan menggunakan konfigurasi GCS dari environment dan mengunggah data BMKG ke bucket Bronze.

### 11.5 Menjalankan Publisher OpenAQ

```cmd
python -m src.ingestion.openaq_publisher
```

Publisher akan mencari lokasi Indonesia yang memiliki parameter PM2.5, memeriksa observasi terkini, memvalidasi lokasi/sensor, lalu menerbitkan event yang memenuhi syarat ke Pub/Sub.

### 11.6 Menjalankan Dataflow Streaming

Pipeline production dijalankan dengan Dataflow Runner pada GCP.

Untuk menjalankan dari Windows host, pastikan Application Default Credentials menggunakan path Windows yang valid, misalnya:

```cmd
set GOOGLE_APPLICATION_CREDENTIALS=C:\Users\TAUFIQ\AppData\Roaming\gcloud\application_default_credentials.json
set GOOGLE_CLOUD_PROJECT=jcdeah-009
python -m src.beam.openaq_streaming_dataflow
```

**Catatan:** path `/home/airflow/.config/gcloud/application_default_credentials.json` digunakan di dalam container Airflow, bukan sebagai path Windows host.

### 11.7 Menjalankan dbt

```cmd
cd dbt
dbt debug --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
```

---

## 12. Menjalankan Airflow

Airflow dijalankan menggunakan Docker Compose.

```cmd
docker compose -f docker-compose.airflow.yml up -d
```

Komponen utama:

```text
PostgreSQL
Airflow Webserver / API Server
Airflow Scheduler
Airflow DAG Processor
```

Airflow UI:

```text
http://localhost:8080
```

DAG utama:

```text
smart_logistics_pipeline
```

Urutan task:

```text
ingest_bmkg
    ↓
process_bmkg_weather
    ↓
load_bmkg_weather
    ↓
run_dbt
    ↓
test_dbt
```

`ingest_logistics` berjalan paralel setelah task yang diperlukan untuk dbt tersedia, kemudian menjadi dependency untuk `run_dbt`.

### Mendapatkan password Airflow

Jika menggunakan SimpleAuthManager dan password dibuat oleh container, password dapat dilihat melalui log webserver:

```cmd
docker compose -f docker-compose.airflow.yml logs airflow-webserver | findstr /i "Password for user"
```

---

## 13. Email Alert Kegagalan Pipeline

Pipeline memiliki mekanisme failure alert berbasis email.

Alur:

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
Gmail SMTP + STARTTLS
     |
     v
Email Penerima
```

Ketika task gagal, callback mengumpulkan informasi:

- DAG ID;
- task yang gagal;
- Run ID;
- waktu eksekusi;
- detail exception;
- tautan ke log task Airflow.

Email menggunakan Gmail SMTP pada port `587` dengan STARTTLS.

### Konfigurasi Gmail

Gunakan Gmail App Password, bukan password Gmail biasa.

Contoh konfigurasi:

```env
AIRFLOW__SMTP__SMTP_HOST=smtp.gmail.com
AIRFLOW__SMTP__SMTP_PORT=587
AIRFLOW__SMTP__SMTP_STARTTLS=true
AIRFLOW__SMTP__SMTP_SSL=false
AIRFLOW__SMTP__SMTP_USER=yourgmail@gmail.com
AIRFLOW__SMTP__SMTP_PASSWORD=YOUR_GMAIL_APP_PASSWORD
AIRFLOW__SMTP__SMTP_MAIL_FROM=yourgmail@gmail.com
AIRFLOW_ALERT_EMAIL=mentor@example.com
```

Panduan pengujian lengkap tersedia pada `ALERT_SETUP.md`.

### Bukti pengujian

Project menyediakan DAG pengujian:

```text
test_email_alert
```

DAG tersebut sengaja membuat task gagal untuk memastikan callback email dapat berjalan. Pengujian berhasil menghasilkan email alert Gmail dengan informasi DAG, task, Run ID, waktu eksekusi, detail error, dan tautan log Airflow.

---

## 14. Konfigurasi GCP

Aktifkan API yang diperlukan:

```cmd
gcloud services enable ^
  storage.googleapis.com ^
  pubsub.googleapis.com ^
  dataflow.googleapis.com ^
  bigquery.googleapis.com ^
  secretmanager.googleapis.com
```

Bucket utama project:

```text
jcdeah-009-smart-logistics-raw
jcdeah-009-smart-logistics-silver
jcdeah-009-smart-logistics-temp
```

Topik Pub/Sub:

```text
openaq-events
driver-events
```

Untuk deployment produksi, API key OpenAQ sebaiknya disimpan pada Secret Manager. File `.env` digunakan untuk pengembangan lokal.

---

## 15. Dataset BigQuery

Dataset yang digunakan:

```text
smart_logistics_bronze
smart_logistics_silver
smart_logistics_gold
smart_logistics_audit
```

Contoh tabel/model penting:

```text
smart_logistics_bronze.driver_events
smart_logistics_silver.environment_risk_daily
smart_logistics_silver.openaq_events_external
smart_logistics_silver.openaq_air_quality_daily
smart_logistics_silver.openaq_station_mapping
smart_logistics_gold.fact_delivery_performance
smart_logistics_gold.fact_area_risk_daily
```

`fact_area_risk_daily` merupakan sumber utama dashboard risiko.

---

## 16. Dashboard Looker Studio

Dashboard menggunakan Looker Studio dengan sumber utama:

```text
smart_logistics_gold.fact_area_risk_daily
```

Visualisasi yang disarankan:

1. KPI rata-rata risk score;
2. jumlah area dengan risk band HIGH/CRITICAL;
3. rata-rata delay rate;
4. coverage data kualitas udara;
5. risk score berdasarkan kota;
6. tren risk score dari waktu ke waktu;
7. weather risk;
8. air quality risk;
9. tabel detail operasional.

`INSUFFICIENT_DATA` sebaiknya tetap ditampilkan agar dashboard tidak menyamarkan keterbatasan data.

Judul dashboard:

**Smart Logistics Environmental Risk Monitor**

Pertanyaan bisnis:

> Area/kota mana yang memiliki risiko operasional berdasarkan kombinasi cuaca, kualitas udara, dan aktivitas logistik, dan bagaimana perubahannya dari waktu ke waktu?

---

## 17. Pengujian

### Pengujian Python

Test dapat dijalankan dengan:

```cmd
pytest
```

### Pengujian dbt

```cmd
cd dbt
dbt test --profiles-dir .
```

Pengujian dbt mencakup:

- `not_null`;
- `accepted_values` untuk risk band;
- rentang risk score;
- konsistensi risk band terhadap risk score.

### Pengujian Email Alert

```text
test_email_alert
```

DAG ini digunakan khusus untuk menguji failure callback tanpa mengubah pipeline utama.

---

## 18. Keterbatasan Project

1. Data logistik sintetis tidak mewakili seluruh kondisi operasional dunia nyata.
2. Ketersediaan stasiun OpenAQ berbeda antarwilayah.
3. Data OpenAQ yang stale tidak dapat digunakan untuk menggambarkan kondisi udara terkini.
4. Pemetaan stasiun OpenAQ ke kota operasional menggunakan mapping eksplisit project.
5. Risk score dan bobotnya merupakan asumsi desain project.
6. Threshold PM2.5 yang digunakan untuk normalisasi bukan standar medis atau regulasi.
7. Streaming Dataflow membutuhkan resource GCP dan dapat menimbulkan biaya cloud.

---

## 19. Pengembangan Selanjutnya

Pengembangan yang dapat dilakukan:

- memperluas mapping stasiun OpenAQ;
- menambahkan lebih banyak sumber kualitas udara;
- mengintegrasikan GPS driver real-time secara penuh;
- menambahkan alert berbasis risk band;
- menggunakan Secret Manager untuk seluruh secret production;
- menambahkan monitoring Dataflow dan pipeline-level metrics;
- menambahkan historisasi perubahan risk score;
- mengembangkan model prediktif setelah tersedia data historis yang memadai.

---

## 20. Alur Presentasi Mentor

Urutan penjelasan yang disarankan:

1. jelaskan permasalahan bisnis;
2. jelaskan empat sumber data;
3. jelaskan perbedaan batch dan streaming;
4. jelaskan GCS sebagai data lake;
5. jelaskan Pub/Sub dan Dataflow pada jalur OpenAQ;
6. jelaskan BigQuery sebagai analytical warehouse;
7. jelaskan dbt sebagai business transformation layer;
8. jelaskan formula risk score;
9. demonstrasikan data BMKG;
10. demonstrasikan event OpenAQ;
11. demonstrasikan hasil BigQuery Gold;
12. tampilkan dashboard Looker Studio;
13. demonstrasikan failure alert Gmail;
14. tampilkan hasil `dbt test`;
15. jelaskan keterbatasan dan pengembangan berikutnya.
