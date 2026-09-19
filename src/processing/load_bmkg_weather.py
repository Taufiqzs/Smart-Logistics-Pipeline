import json
import os
import uuid
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import bigquery


load_dotenv()

# ID project Google Cloud yang digunakan oleh pipeline.
PROJECT_ID = os.getenv("PROJECT_ID", "jcdeah-009")
# Nama dataset BigQuery tujuan.
DATASET = "smart_logistics_silver"
# Nama tabel BigQuery tujuan.
TARGET_TABLE = "environment_risk_daily"

# Path file input yang akan dimuat ke BigQuery.
INPUT_FILE = Path(
    os.getenv(
        "BMKG_WEATHER_RISK_OUTPUT",
        "data/processed/bmkg_weather_risk_daily.jsonl",
    )
)


# Membaca record hasil pemrosesan dari file input.
def load_records():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(
            f"BMKG weather-risk output not found: {INPUT_FILE}"
        )

# Menyimpan records yang digunakan pada proses ini.
    records = []

    with INPUT_FILE.open("r", encoding="utf-8") as file:
        for line in file:
# Menyimpan line yang digunakan pada proses ini.
            line = line.strip()

            if line:
                records.append(json.loads(line))

    if not records:
        raise ValueError(
            f"No records found in {INPUT_FILE}"
        )

    return records


# Menyusun schema BigQuery untuk tabel target.
def get_schema():
    return [
        bigquery.SchemaField("event_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("city", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("weather_risk", "FLOAT"),
        bigquery.SchemaField("air_quality_risk", "FLOAT"),
        bigquery.SchemaField("active_weather_alerts", "INTEGER"),
        bigquery.SchemaField("bmkg_alert_id", "STRING"),
        bigquery.SchemaField("weather_event", "STRING"),
        bigquery.SchemaField("severity", "STRING"),
        bigquery.SchemaField("urgency", "STRING"),
        bigquery.SchemaField("certainty", "STRING"),
        bigquery.SchemaField("area_desc", "STRING"),
        bigquery.SchemaField("effective", "TIMESTAMP"),
        bigquery.SchemaField("expires", "TIMESTAMP"),
        bigquery.SchemaField("processed_at", "TIMESTAMP"),
    ]


# Memastikan tabel BigQuery target sudah tersedia.
def ensure_target_table(client):
# Menyimpan table id yang digunakan pada proses ini.
    table_id = f"{PROJECT_ID}.{DATASET}.{TARGET_TABLE}"

    try:
        client.get_table(table_id)
        print(f"Target table exists: {table_id}")
        return table_id

    except Exception:
        print(f"Creating target table: {table_id}")

# Menyimpan table yang digunakan pada proses ini.
        table = bigquery.Table(
            table_id,
            schema=get_schema(),
        )

# Menyimpan time partitioning yang digunakan pada proses ini.
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="event_date",
        )

# Menyimpan clustering fields yang digunakan pada proses ini.
        table.clustering_fields = ["city"]

        client.create_table(table)

        print(f"Created: {table_id}")

        return table_id


# Memuat record hasil pemrosesan ke tabel BigQuery.
def load_to_bigquery(records):
# Menyimpan client yang digunakan pada proses ini.
    client = bigquery.Client(project=PROJECT_ID)

# Menyimpan target table yang digunakan pada proses ini.
    target_table = ensure_target_table(client)

# Menyimpan temp table yang digunakan pada proses ini.
    temp_table = (
        f"{PROJECT_ID}.{DATASET}."
        f"_bmkg_weather_stage_{uuid.uuid4().hex[:12]}"
    )

# Menyimpan rows yang digunakan pada proses ini.
    rows = []

    for record in records:
        rows.append(
            {
                "event_date": record.get("event_date"),
                "city": record.get("city"),
                "weather_risk": record.get("weather_risk"),
                "air_quality_risk": None,
                "active_weather_alerts": record.get(
                    "active_weather_alerts"
                ),
                "bmkg_alert_id": record.get("bmkg_alert_id"),
                "weather_event": record.get("weather_event"),
                "severity": record.get("severity"),
                "urgency": record.get("urgency"),
                "certainty": record.get("certainty"),
                "area_desc": record.get("area_desc"),
                "effective": record.get("effective"),
                "expires": record.get("expires"),
                "processed_at": record.get("processed_at"),
            }
        )

# Menyimpan job config yang digunakan pada proses ini.
    job_config = bigquery.LoadJobConfig(
        schema=get_schema(),
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    print(f"Loading {len(rows)} records into staging table...")

# Menyimpan load job yang digunakan pada proses ini.
    load_job = client.load_table_from_json(
        rows,
        temp_table,
        job_config=job_config,
    )

    load_job.result()

# Menyimpan merge sql yang digunakan pada proses ini.
    merge_sql = f"""
    MERGE `{target_table}` AS target
    USING `{temp_table}` AS source
    ON target.event_date = source.event_date
       AND target.city = source.city

    WHEN MATCHED THEN
      UPDATE SET
        weather_risk = source.weather_risk,
        active_weather_alerts = source.active_weather_alerts,
        bmkg_alert_id = source.bmkg_alert_id,
        weather_event = source.weather_event,
        severity = source.severity,
        urgency = source.urgency,
        certainty = source.certainty,
        area_desc = source.area_desc,
        effective = source.effective,
        expires = source.expires,
        processed_at = source.processed_at

    WHEN NOT MATCHED THEN
      INSERT (
        event_date,
        city,
        weather_risk,
        air_quality_risk,
        active_weather_alerts,
        bmkg_alert_id,
        weather_event,
        severity,
        urgency,
        certainty,
        area_desc,
        effective,
        expires,
        processed_at
      )
      VALUES (
        source.event_date,
        source.city,
        source.weather_risk,
        NULL,
        source.active_weather_alerts,
        source.bmkg_alert_id,
        source.weather_event,
        source.severity,
        source.urgency,
        source.certainty,
        source.area_desc,
        source.effective,
        source.expires,
        source.processed_at
      )
    """

    print("Merging BMKG records into BigQuery...")

# Menyimpan query job yang digunakan pada proses ini.
    query_job = client.query(merge_sql)
    query_job.result()

    client.delete_table(temp_table, not_found_ok=True)

    print(
        f"BMKG weather risk loaded successfully: "
        f"{target_table}"
    )


# Menjalankan alur utama script dari awal sampai selesai.
def main():
    print("Starting BMKG BigQuery load...")

# Menyimpan records yang digunakan pada proses ini.
    records = load_records()

    print(
        f"Loaded {len(records)} records "
        f"from {INPUT_FILE}"
    )

    load_to_bigquery(records)

    print("BMKG BigQuery load completed successfully.")


if __name__ == "__main__":
    main()