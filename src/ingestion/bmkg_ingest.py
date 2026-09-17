import os
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv
from google.cloud import storage


load_dotenv()


BMKG_URL_ID = os.environ["BMKG_URL_ID"]
RAW_BUCKET = os.environ["RAW_BUCKET"]

HEADERS = {
    "User-Agent": "smart-logistics-risk-pipeline/1.0"
}


def fetch_url(url: str) -> bytes:
    """Fetch raw content from BMKG."""
    response = requests.get(
        url,
        timeout=30,
        headers=HEADERS,
    )

    response.raise_for_status()

    return response.content


def parse_feed(feed_data: bytes) -> list[str]:
    """
    Parse the BMKG RSS feed and extract detail CAP URLs.
    """
    root = ET.fromstring(feed_data)

    detail_urls = []

    for item in root.findall(".//item"):
        link = item.find("link")

        if link is not None and link.text:
            detail_urls.append(link.text.strip())

    return detail_urls


def upload_to_gcs(
    client: storage.Client,
    data: bytes,
    blob_name: str,
    content_type: str,
):
    """Upload raw data to GCS."""
    bucket = client.bucket(RAW_BUCKET)
    blob = bucket.blob(blob_name)

    blob.upload_from_string(
        data,
        content_type=content_type,
    )

    print(
        f"Uploaded: gs://{RAW_BUCKET}/{blob_name}"
    )


def main():
    print("Starting BMKG ingestion...")

    now = datetime.now(timezone.utc)

    date_partition = now.strftime("%Y-%m-%d")
    timestamp = now.strftime("%Y%m%dT%H%M%SZ")

    client = storage.Client()

    # ---------------------------------------------------------
    # 1. Download BMKG RSS feed
    # ---------------------------------------------------------

    print(f"Fetching BMKG feed: {BMKG_URL_ID}")

    feed_data = fetch_url(BMKG_URL_ID)

    print(
        f"Received BMKG feed: {len(feed_data)} bytes"
    )

    # ---------------------------------------------------------
    # 2. Store the original RSS feed in Bronze
    # ---------------------------------------------------------

    feed_blob = (
        f"bmkg/weather_warning/feed/"
        f"ingestion_date={date_partition}/"
        f"feed_{timestamp}.xml"
    )

    upload_to_gcs(
        client=client,
        data=feed_data,
        blob_name=feed_blob,
        content_type="application/xml",
    )

    # ---------------------------------------------------------
    # 3. Extract detailed CAP URLs
    # ---------------------------------------------------------

    detail_urls = parse_feed(feed_data)

    print(
        f"Found {len(detail_urls)} BMKG CAP detail URLs"
    )

    # ---------------------------------------------------------
    # 4. Download each detailed CAP file
    # ---------------------------------------------------------

    successful = 0
    failed = 0

    for index, detail_url in enumerate(detail_urls, start=1):

        try:
            print(
                f"[{index}/{len(detail_urls)}] "
                f"Fetching CAP: {detail_url}"
            )

            cap_data = fetch_url(detail_url)

            # Extract filename from URL
            filename = os.path.basename(
                urlparse(detail_url).path
            )

            if not filename:
                filename = f"cap_{index}.xml"

            cap_blob = (
                f"bmkg/weather_warning/cap/"
                f"ingestion_date={date_partition}/"
                f"{filename}"
            )

            upload_to_gcs(
                client=client,
                data=cap_data,
                blob_name=cap_blob,
                content_type="application/xml",
            )

            successful += 1

        except Exception as exc:
            failed += 1

            print(
                f"Failed to process {detail_url}: {exc}"
            )

    # ---------------------------------------------------------
    # 5. Summary
    # ---------------------------------------------------------

    print("\nBMKG ingestion completed.")
    print(f"CAP files successful: {successful}")
    print(f"CAP files failed: {failed}")


if __name__ == "__main__":
    main()