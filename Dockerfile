FROM apache/airflow:2.10.5-python3.11

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY src /opt/airflow/src
COPY dags /opt/airflow/dags
COPY dbt /opt/airflow/dbt
