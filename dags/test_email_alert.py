import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

from src.common.airflow_alerts import notify_pipeline_failure


def intentionally_fail():
    """
    Task yang sengaja dibuat gagal, untuk menguji
    mekanisme email alert Airflow.
    """

    # Sengaja menghasilkan error.
    raise RuntimeError(
        "TEST EMAIL ALERT - Smart Logistics Pipeline"
    )

with DAG(
    dag_id="test_email_alert",
    description="DAG untuk menguji Airflow email failure alert",
    start_date=pendulum.datetime(
        2026,
        9,
        19,
        tz="Asia/Jakarta",
    ),
    schedule=None,
    catchup=False,
    tags=[
        "test",
        "email-alert",
        "smart-logistics",
    ],
) as dag:

    test_failure = PythonOperator(
        task_id="test_failure",
        python_callable=intentionally_fail,
        on_failure_callback=notify_pipeline_failure,
    )