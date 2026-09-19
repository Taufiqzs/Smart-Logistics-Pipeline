"""
DAG utama Smart Logistics Weather-Air Quality Risk Pipeline.

Pipeline ini melakukan:
1. Ingest data peringatan cuaca BMKG.
2. Memproses data cuaca menjadi weather risk.
3. Memuat weather risk ke BigQuery.
4. Ingest data logistics historis/sintetis.
5. Menjalankan dbt transformation.
6. Menjalankan dbt data quality tests.

Jika salah satu task gagal, Airflow akan menjalankan
notify_pipeline_failure() dan mengirimkan email alert.
"""

import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

from src.common.airflow_alerts import notify_pipeline_failure


# ============================================================
# KONFIGURASI PROJECT
# ============================================================

# Direktori utama project di dalam container Airflow.
PROJECT_ROOT = "/opt/airflow"


# ============================================================
# DEFINISI DAG
# ============================================================

with DAG(
    # ID unik DAG di Airflow.
    dag_id="smart_logistics_pipeline",

    # Deskripsi DAG.
    description=(
        "Smart Logistics Weather-Air Quality "
        "Risk Pipeline"
    ),

    # Waktu mulai DAG.
    start_date=pendulum.datetime(
        2026,
        9,
        19,
        tz="Asia/Jakarta",
    ),

    # DAG dijalankan secara manual.
    schedule=None,

    # Tidak menjalankan backfill untuk tanggal sebelumnya.
    catchup=False,

    # Tag untuk mempermudah filtering di Airflow UI.
    tags=[
        "smart-logistics",
        "gcp",
        "bmkg",
        "openaq",
        "dbt",
        "risk-pipeline",
    ],

    # ========================================================
    # NOTIFIKASI EMAIL KETIKA PIPELINE GAGAL
    # ========================================================
    #
    # Callback ini akan dipanggil ketika DAG mengalami
    # kegagalan.
    #
    # Fungsi notify_pipeline_failure() akan:
    # - mengambil informasi DAG
    # - mengambil task yang gagal
    # - mengambil Run ID
    # - mengambil waktu eksekusi
    # - mengambil exception
    # - mengambil URL log Airflow
    # - mengirim email melalui Gmail SMTP
    #
    on_failure_callback=notify_pipeline_failure,

) as dag:

    # ========================================================
    # 1. INGEST BMKG
    # ========================================================

    ingest_bmkg = BashOperator(
        task_id="ingest_bmkg",

        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python -m src.ingestion.bmkg_ingest"
        ),
    )


    # ========================================================
    # 2. MEMPROSES RISIKO CUACA BMKG
    # ========================================================

    process_bmkg_weather = BashOperator(
        task_id="process_bmkg_weather",

        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python -m src.processing.bmkg_weather_risk"
        ),
    )


    # ========================================================
    # 3. MEMUAT RISIKO CUACA BMKG
    # ========================================================

    load_bmkg_weather = BashOperator(
        task_id="load_bmkg_weather",

        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python -m src.processing.load_bmkg_weather"
        ),
    )


    # ========================================================
    # 4. MENGAMBIL DATA LOGISTIK
    # ========================================================

    ingest_logistics = BashOperator(
        task_id="ingest_logistics",

        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python -m src.ingestion.logistics_ingest"
        ),
    )


    # ========================================================
    # 5. MENJALANKAN DBT
    # ========================================================

    run_dbt = BashOperator(
        task_id="run_dbt",

        bash_command=(
            f"cd {PROJECT_ROOT}/dbt && "
            "rm -rf target && dbt run --no-partial-parse --profiles-dir ."
        ),
    )

    # ========================================================
    # 6. MENJALANKAN PENGUJIAN DBT
    # ========================================================

    test_dbt = BashOperator(
        task_id="test_dbt",

        bash_command=(
            f"cd {PROJECT_ROOT}/dbt && "
            "rm -rf target && dbt test --no-partial-parse --profiles-dir ."
        ),
    )


    # ========================================================
    # DEPENDENSI / ALUR KERJA
    # ========================================================

    # BMKG:
    #
    # ingest BMKG
    #      ↓
    # memproses risiko cuaca
    #      ↓
    # memuat risiko cuaca
    #
    ingest_bmkg >> process_bmkg_weather
    process_bmkg_weather >> load_bmkg_weather


    # Ingestion data logistik berjalan sebagai dependensi
    # terpisah sebelum dbt.
    #
    # Setelah data cuaca BMKG dan data logistik selesai,
    # dbt dapat dijalankan.
    [
        load_bmkg_weather,
        ingest_logistics,
    ] >> run_dbt


    # Setelah transformasi dbt selesai,
    # jalankan pengujian kualitas data.
    run_dbt >> test_dbt