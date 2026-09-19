import os
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import storage

from dotenv import load_dotenv

load_dotenv()
# ID project Google Cloud yang digunakan oleh pipeline.
PROJECT_ID = os.environ["PROJECT_ID"]
# Nama bucket GCS untuk menyimpan data mentah atau Bronze.
RAW_BUCKET = os.environ["RAW_BUCKET"]

# Menyimpan local file yang digunakan pada proses ini.
LOCAL_FILE = os.environ.get(
    "LOCAL_LOGISTICS_FILE",
    "data/raw/logistics/Transportation_and_Logistics_Tracking_Dataset.xlsx",
)


# Mengunggah data atau file ke Google Cloud Storage.
def upload_to_gcs():
    """Mengunggah dataset logistik historis ke GCS Bronze."""

    # ---------------------------------------------------------
    # 1. Tentukan lokasi file dataset lokal
    # ---------------------------------------------------------
    file_path = Path(LOCAL_FILE)

    if not file_path.exists():
        raise FileNotFoundError(
            f"Logistics dataset not found: {file_path}"
        )

    print(f"Source: {file_path}")
    print(f"Raw bucket: {RAW_BUCKET}")
    print(f"Project: {PROJECT_ID}")

    # ---------------------------------------------------------
    # 2. Buat klien GCS secara eksplisit menggunakan ID proyek
    # ---------------------------------------------------------
    client = storage.Client(
        project=PROJECT_ID
    )

# Menyimpan bucket yang digunakan pada proses ini.
    bucket = client.bucket(
        RAW_BUCKET
    )

    # ---------------------------------------------------------
    # 3. Buat partisi berdasarkan tanggal ingestion
    # ---------------------------------------------------------
    now = datetime.now(timezone.utc)

# Menyimpan date partition yang digunakan pada proses ini.
    date_partition = now.strftime(
        "%Y-%m-%d"
    )

    # ---------------------------------------------------------
    # 4. Bentuk path objek Bronze
    # ---------------------------------------------------------
    blob_name = (
        "logistics/historical/"
        f"ingestion_date={date_partition}/"
        f"{file_path.name}"
    )

    print(
        f"Destination: gs://{RAW_BUCKET}/{blob_name}"
    )

    # ---------------------------------------------------------
    # 5. Unggah file
    # ---------------------------------------------------------
    blob = bucket.blob(
        blob_name
    )

    blob.upload_from_filename(
        str(file_path)
    )

    # ---------------------------------------------------------
    # 6. Verifikasi hasil unggah
    # ---------------------------------------------------------
    print(
        f"Uploaded: gs://{RAW_BUCKET}/{blob_name}"
    )


# Menjalankan alur utama script dari awal sampai selesai.
def main():
    """Menjalankan ingestion dataset logistik."""
    upload_to_gcs()


if __name__ == "__main__":
    main()