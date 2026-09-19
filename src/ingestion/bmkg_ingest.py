import os
import re
from datetime import datetime, timezone
from urllib.parse import urljoin
from xml.etree import ElementTree as ET

import requests
from google.cloud import storage

from dotenv import load_dotenv

load_dotenv()
# ID project Google Cloud yang digunakan oleh pipeline.
PROJECT_ID = os.environ["PROJECT_ID"]
# Nama bucket GCS untuk menyimpan data mentah atau Bronze.
RAW_BUCKET = os.environ["RAW_BUCKET"]
# Menyimpan bmkg url id yang digunakan pada proses ini.
BMKG_URL_ID = os.environ["BMKG_URL_ID"]


# Mengunduh data dari URL menggunakan HTTP GET.
def download_url(url: str) -> bytes:
    """Mengunduh konten dari sebuah URL."""
# Menyimpan response yang digunakan pada proses ini.
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.content


# Mengekstrak URL detail CAP BMKG dari feed RSS/XML.
def get_detail_urls(feed_data: bytes, feed_url: str) -> list[str]:
    """Mengekstrak URL detail CAP BMKG dari feed RSS/XML."""
# Menyimpan root yang digunakan pada proses ini.
    root = ET.fromstring(feed_data)

# Menyimpan detail urls yang digunakan pada proses ini.
    detail_urls = []

    for item in root.iter():
        if item.tag.endswith("item"):
# Menyimpan link yang digunakan pada proses ini.
            link = None

            for child in item:
                if child.tag.endswith("link"):
# Menyimpan link yang digunakan pada proses ini.
                    link = child
                    break

            if link is not None and link.text:
                detail_urls.append(
                    urljoin(feed_url, link.text.strip())
                )

    return detail_urls


# Mengunggah data atau file ke Google Cloud Storage.
def upload_to_gcs(
    data: bytes,
    blob_name: str,
    content_type: str,
):
    """Mengunggah data mentah ke GCS."""
# Menyimpan client yang digunakan pada proses ini.
    client = storage.Client(project=PROJECT_ID)
# Menyimpan bucket yang digunakan pada proses ini.
    bucket = client.bucket(RAW_BUCKET)
# Menyimpan blob yang digunakan pada proses ini.
    blob = bucket.blob(blob_name)

    blob.upload_from_string(
        data,
        content_type=content_type,
    )

    print(
        f"Uploaded: gs://{RAW_BUCKET}/{blob_name}"
    )


# Mengambil identifier CAP BMKG dari URL detail peringatan.
def extract_cap_identifier(url: str) -> str:
    """Mengekstrak identifier CAP BMKG dari URL."""
# Menyimpan filename yang digunakan pada proses ini.
    filename = url.rstrip("/").split("/")[-1]

    if filename.endswith("_alert.xml"):
# Menyimpan filename yang digunakan pada proses ini.
        filename = filename[:-len("_alert.xml")]

    return filename


# Menjalankan alur utama script dari awal sampai selesai.
def main():
    """Menjalankan ingestion peringatan cuaca BMKG."""
# Menyimpan ingestion date yang digunakan pada proses ini.
    ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
# Menyimpan ingestion timestamp yang digunakan pada proses ini.
    ingestion_timestamp = datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%SZ"
    )

    print(f"BMKG feed URL: {BMKG_URL_ID}")
    print(f"Raw bucket: {RAW_BUCKET}")
    print(f"Ingestion date: {ingestion_date}")

    # ---------------------------------------------------------
    # 1. Unduh feed RSS/XML BMKG
    # ---------------------------------------------------------
    print("Downloading BMKG feed...")

# Menyimpan feed data yang digunakan pada proses ini.
    feed_data = download_url(BMKG_URL_ID)

    print(
        f"Downloaded BMKG feed: {len(feed_data):,} bytes"
    )

    # ---------------------------------------------------------
    # 2. Simpan feed asli ke GCS Bronze
    # ---------------------------------------------------------
    feed_blob_name = (
        "bmkg/weather_warning/feed/"
        f"ingestion_date={ingestion_date}/"
        f"feed_{ingestion_timestamp}.xml"
    )

    upload_to_gcs(
        data=feed_data,
        blob_name=feed_blob_name,
        content_type="application/xml",
    )

    # ---------------------------------------------------------
    # 3. Ekstrak URL detail CAP
    # ---------------------------------------------------------
    detail_urls = get_detail_urls(
        feed_data,
        BMKG_URL_ID,
    )

    print(
        f"Found {len(detail_urls)} BMKG CAP detail URLs."
    )

    # ---------------------------------------------------------
    # 4. Unduh dan simpan setiap XML CAP
    # ---------------------------------------------------------
    successful = 0
# Menyimpan failed yang digunakan pada proses ini.
    failed = 0

    for url in detail_urls:
        try:
# Menyimpan cap data yang digunakan pada proses ini.
            cap_data = download_url(url)

# Menyimpan identifier yang digunakan pada proses ini.
            identifier = extract_cap_identifier(url)

# Menyimpan cap blob name yang digunakan pada proses ini.
            cap_blob_name = (
                "bmkg/weather_warning/cap/"
                f"ingestion_date={ingestion_date}/"
                f"{identifier}_alert.xml"
            )

            upload_to_gcs(
                data=cap_data,
                blob_name=cap_blob_name,
                content_type="application/xml",
            )

# Menyimpan successful yang digunakan pada proses ini.
            successful += 1

        except Exception as exc:
# Menyimpan failed yang digunakan pada proses ini.
            failed += 1

            print(
                f"Failed to ingest BMKG CAP URL: {url}"
            )
            print(
                f"Error: {type(exc).__name__}: {exc}"
            )

    # ---------------------------------------------------------
    # 5. Ringkasan
    # ---------------------------------------------------------
    print()
    print("=" * 60)
    print("BMKG INGESTION SUMMARY")
    print("=" * 60)
    print(f"Feed bytes       : {len(feed_data):,}")
    print(f"CAP URLs found   : {len(detail_urls)}")
    print(f"CAP successful   : {successful}")
    print(f"CAP failed       : {failed}")
    print("=" * 60)


if __name__ == "__main__":
    main()