from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def failure_alert(context):
    print(f"[ALERT] Pipeline failed: {context['dag'].dag_id}.{context['task_instance'].task_id}")

with DAG(
    dag_id='smart_logistics_weather_air_quality',
    start_date=datetime(2026,1,1),
    schedule='0 */3 * * *',
    catchup=False,
    default_args={'owner':'data-engineering','retries':2,'retry_delay':timedelta(minutes=5),'on_failure_callback':failure_alert},
    tags=['purwadhika','gcp','logistics'],
) as dag:
    ingest_bmkg=BashOperator(
        task_id='bmkg_to_gcs_bronze',
        bash_command='python -m src.ingestion.bmkg_ingest --output /opt/airflow/data/raw',
    )
    trigger_dataflow=BashOperator(
        task_id='trigger_dataflow_batch',
        bash_command='echo "Production: submit src.beam.risk_pipeline to DataflowRunner using GCS Bronze as input and GCS Silver as output"',
    )
    load_bigquery=BashOperator(
        task_id='load_silver_to_bigquery',
        bash_command='echo "Production: load GCS Silver partitions into smart_logistics_silver"',
    )
    dbt_run=BashOperator(task_id='dbt_run',bash_command='dbt run --project-dir /opt/airflow/dbt')
    dbt_test=BashOperator(task_id='dbt_test',bash_command='dbt test --project-dir /opt/airflow/dbt')
    ingest_bmkg >> trigger_dataflow >> load_bigquery >> dbt_run >> dbt_test
