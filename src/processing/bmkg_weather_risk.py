import argparse
import json
import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import storage


# ID project Google Cloud yang digunakan oleh pipeline.
PROJECT_ID = "jcdeah-009"

# Nama bucket GCS untuk menyimpan data mentah atau Bronze.
RAW_BUCKET = "jcdeah-009-smart-logistics-raw"

# Path file lokal tempat hasil pemrosesan disimpan.
OUTPUT_FILE = "data/processed/bmkg_weather_risk_daily.jsonl"


# Pemetaan eksplisit yang ditetapkan oleh proyek:
# Provinsi BMKG -> kota operasional logistik
PROVINCE_TO_CITY = {
    "DKI Jakarta": "Jakarta",
    "Jakarta": "Jakarta",
    "Jawa Barat": "Bandung",
    "Jawa Timur": "Surabaya",
    "Jawa Tengah": "Semarang",
    "DI Yogyakarta": "Yogyakarta",
    "Daerah Istimewa Yogyakarta": "Yogyakarta",
    "Sumatera Utara": "Medan",
}


# Asumsi penilaian risiko yang ditetapkan oleh proyek.
SEVERITY_TO_RISK = {
    "minor": 25.0,
    "moderate": 50.0,
    "severe": 80.0,
    "extreme": 100.0,
}


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

# Menyimpan logger yang digunakan pada proses ini.
logger = logging.getLogger(__name__)


# Mengubah teks waktu menjadi objek datetime yang dapat diproses Python.
def parse_datetime(value):
    """Mem-parsing datetime ISO-8601 dan mengembalikan datetime dengan timezone UTC."""
    if not value:
        return None

# Menyimpan value yang digunakan pada proses ini.
    value = value.strip()

    try:
# Menyimpan dt yang digunakan pada proses ini.
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))

        if dt.tzinfo is None:
# Menyimpan dt yang digunakan pada proses ini.
            dt = dt.replace(tzinfo=timezone.utc)

        return dt.astimezone(timezone.utc)

    except ValueError:
        logger.warning("Unable to parse datetime: %s", value)
        return None


# Mengubah tingkat keparahan cuaca BMKG menjadi skor risiko proyek.
def severity_to_risk(severity):
    """Mengubah tingkat keparahan BMKG menjadi skor risiko cuaca proyek."""
    if not severity:
        return 0.0

    return SEVERITY_TO_RISK.get(
        severity.strip().lower(),
        0.0,
    )


# Membersihkan dan menormalkan nama area dari data BMKG.
def normalize_area(area_desc):
    """Menormalkan nama provinsi BMKG agar dapat digunakan dalam pemetaan kota."""
    if not area_desc:
        return ""

# Menyimpan area yang digunakan pada proses ini.
    area = area_desc.strip()

    # Menormalkan variasi umum penamaan wilayah BMKG.
    replacements = {
        "Kep. Riau": "Kepulauan Riau",
        "Kep. Bangka Belitung": "Kepulauan Bangka Belitung",
    }

    return replacements.get(area, area)


# Membaca dan mengekstrak informasi penting dari dokumen CAP BMKG.
def extract_cap(xml_content):
    """Mengekstrak field yang diperlukan dari dokumen XML CAP BMKG."""

# Menyimpan root yang digunakan pada proses ini.
    root = ET.fromstring(xml_content)

    # Format CAP menggunakan namespace:
    # urn:oasis:names:tc:emergency:cap:1.2
    ns = {
        "cap": "urn:oasis:names:tc:emergency:cap:1.2"
    }

# Menyimpan info yang digunakan pada proses ini.
    info = root.find("cap:info", ns)

    if info is None:
        raise ValueError("CAP document has no info element")

# Fungsi `text()` digunakan untuk menjalankan proses text.
    def text(name):
# Menyimpan element yang digunakan pada proses ini.
        element = info.find(f"cap:{name}", ns)
        return element.text.strip() if element is not None and element.text else None

# Menyimpan identifier element yang digunakan pada proses ini.
    identifier_element = root.find("cap:identifier", ns)

# Menyimpan identifier yang digunakan pada proses ini.
    identifier = (
        identifier_element.text.strip()
        if identifier_element is not None and identifier_element.text
        else None
    )

# Menyimpan area yang digunakan pada proses ini.
    area = info.find("cap:area", ns)

# Menyimpan area desc yang digunakan pada proses ini.
    area_desc = None
# Menyimpan polygon yang digunakan pada proses ini.
    polygon = None

    if area is not None:
# Menyimpan area element yang digunakan pada proses ini.
        area_element = area.find("cap:areaDesc", ns)
# Menyimpan polygon element yang digunakan pada proses ini.
        polygon_element = area.find("cap:polygon", ns)

        if area_element is not None and area_element.text:
# Menyimpan area desc yang digunakan pada proses ini.
            area_desc = area_element.text.strip()

        if polygon_element is not None and polygon_element.text:
# Menyimpan polygon yang digunakan pada proses ini.
            polygon = polygon_element.text.strip()

    return {
        "identifier": identifier,
        "event": text("event"),
        "urgency": text("urgency"),
        "severity": text("severity"),
        "certainty": text("certainty"),
        "effective": text("effective"),
        "expires": text("expires"),
        "headline": text("headline"),
        "description": text("description"),
        "area_desc": normalize_area(area_desc),
        "polygon": polygon,
    }


# Menentukan kota operasional berdasarkan nama provinsi atau area BMKG.
def extract_city_from_province(area_desc):
    """Menerapkan pemetaan provinsi ke kota operasional secara eksplisit."""

    return PROVINCE_TO_CITY.get(area_desc)


# Memproses satu objek CAP BMKG menjadi record risiko cuaca.
def process_cap_object(blob_name, ingestion_date):
    """Mengunduh dan memproses satu objek CAP."""

# Menyimpan client yang digunakan pada proses ini.
    client = storage.Client(project=PROJECT_ID)

# Menyimpan bucket yang digunakan pada proses ini.
    bucket = client.bucket(RAW_BUCKET)
# Menyimpan blob yang digunakan pada proses ini.
    blob = bucket.blob(blob_name)

# Menyimpan xml content yang digunakan pada proses ini.
    xml_content = blob.download_as_bytes()

# Menyimpan cap yang digunakan pada proses ini.
    cap = extract_cap(xml_content)

# Menyimpan city yang digunakan pada proses ini.
    city = extract_city_from_province(cap["area_desc"])

    if city is None:
        logger.info(
            "BMKG alert %s ignored: province '%s' "
            "does not map to project logistics cities",
            cap["identifier"],
            cap["area_desc"],
        )
        return None

# Menyimpan weather risk yang digunakan pada proses ini.
    weather_risk = severity_to_risk(cap["severity"])

# Menyimpan effective yang digunakan pada proses ini.
    effective = parse_datetime(cap["effective"])
# Menyimpan expires yang digunakan pada proses ini.
    expires = parse_datetime(cap["expires"])

    # Tanggal ingestion pada Bronze digunakan sebagai tanggal analitik.
    event_date = ingestion_date

# Menyimpan record yang digunakan pada proses ini.
    record = {
        "event_date": event_date,
        "city": city,
        "weather_risk": weather_risk,
        "active_weather_alerts": 1,
        "bmkg_alert_id": cap["identifier"],
        "weather_event": cap["event"],
        "severity": cap["severity"],
        "urgency": cap["urgency"],
        "certainty": cap["certainty"],
        "area_desc": cap["area_desc"],
        "effective": cap["effective"],
        "expires": cap["expires"],
        "headline": cap["headline"],
        "processed_at": datetime.now(timezone.utc).isoformat(),
    }

    logger.info(
        "Mapped BMKG alert: %s -> %s | severity=%s | risk=%s",
        cap["area_desc"],
        city,
        cap["severity"],
        weather_risk,
    )

    return record


# Mencari objek CAP BMKG yang tersedia pada penyimpanan GCS.
def discover_cap_objects():
    """Mencari seluruh objek CAP BMKG yang tersimpan pada layer Bronze."""

# Menyimpan client yang digunakan pada proses ini.
    client = storage.Client(project=PROJECT_ID)

# Menyimpan bucket yang digunakan pada proses ini.
    bucket = client.bucket(RAW_BUCKET)

# Menyimpan prefix yang digunakan pada proses ini.
    prefix = "bmkg/weather_warning/cap/"

# Menyimpan blobs yang digunakan pada proses ini.
    blobs = bucket.list_blobs(prefix=prefix)

# Menyimpan objects yang digunakan pada proses ini.
    objects = []

    for blob in blobs:
        if blob.name.endswith("_alert.xml"):
            objects.append(blob.name)

    return objects


# Menggabungkan record risiko cuaca berdasarkan tanggal dan kota.
def aggregate_records(records):
    """
    Menggabungkan beberapa peringatan BMKG berdasarkan kota dan tanggal.

    Risiko cuaca dengan nilai tertinggi dipertahankan.
    Jumlah peringatan dijumlahkan.
    """

# Menyimpan aggregated yang digunakan pada proses ini.
    aggregated = {}

    for record in records:

# Menyimpan key yang digunakan pada proses ini.
        key = (
            record["event_date"],
            record["city"],
        )

        if key not in aggregated:
            aggregated[key] = record.copy()
            continue

# Menyimpan current yang digunakan pada proses ini.
        current = aggregated[key]

        current["weather_risk"] = max(
            current["weather_risk"],
            record["weather_risk"],
        )

        current["active_weather_alerts"] += (
            record["active_weather_alerts"]
        )

        # Simpan daftar kejadian dalam bentuk teks yang dipisahkan koma.
        existing_events = current.get("weather_event", "")
# Menyimpan new event yang digunakan pada proses ini.
        new_event = record.get("weather_event", "")

        if new_event and new_event not in existing_events:
            current["weather_event"] = (
                f"{existing_events}; {new_event}"
                if existing_events
                else new_event
            )

    return list(aggregated.values())


# Menjalankan alur utama script dari awal sampai selesai.
def main():

# Menyimpan parser yang digunakan pada proses ini.
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--output",
        default=OUTPUT_FILE,
        help="Local JSONL output path",
    )

# Menyimpan args yang digunakan pada proses ini.
    args = parser.parse_args()

# Menyimpan cap objects yang digunakan pada proses ini.
    cap_objects = discover_cap_objects()

    logger.info(
        "Found %d BMKG CAP objects",
        len(cap_objects),
    )

# Menyimpan records yang digunakan pada proses ini.
    records = []

    for blob_name in cap_objects:

        # Ekstrak ingestion_date=YYYY-MM-DD
        match = re.search(
            r"ingestion_date=(\d{4}-\d{2}-\d{2})",
            blob_name,
        )

        if not match:
            logger.warning(
                "Skipping object without ingestion date: %s",
                blob_name,
            )
            continue

# Menyimpan ingestion date yang digunakan pada proses ini.
        ingestion_date = match.group(1)

        try:
# Menyimpan record yang digunakan pada proses ini.
            record = process_cap_object(
                blob_name,
                ingestion_date,
            )

            if record:
                records.append(record)

        except Exception as exc:
            logger.exception(
                "Failed processing %s: %s",
                blob_name,
                exc,
            )

# Menyimpan records yang digunakan pada proses ini.
    records = aggregate_records(records)

# Menyimpan output path yang digunakan pada proses ini.
    output_path = Path(args.output)

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with output_path.open(
        "w",
        encoding="utf-8",
    ) as file:

        for record in sorted(
            records,
            key=lambda x: (
                x["event_date"],
                x["city"],
            ),
        ):

            file.write(
                json.dumps(
                    record,
                    ensure_ascii=False,
                )
                + "\n"
            )

    logger.info(
        "Generated %d city/date weather-risk records",
        len(records),
    )

    logger.info(
        "Output: %s",
        output_path,
    )


if __name__ == "__main__":
    main()