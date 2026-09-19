import json
import os
import time
from datetime import datetime, timedelta, timezone

import requests
from dotenv import load_dotenv
from google.cloud import pubsub_v1


# ============================================================
# Konfigurasi
# ============================================================

load_dotenv()

# ID project Google Cloud yang digunakan oleh pipeline.
PROJECT_ID = os.environ["PROJECT_ID"]
# API key OpenAQ yang digunakan untuk autentikasi request.
OPENAQ_API_KEY = os.environ["OPENAQ_API_KEY"]
# Nama topic Pub/Sub untuk mengirim event dari OpenAQ.
OPENAQ_TOPIC = os.environ["OPENAQ_TOPIC"]

# Endpoint API OpenAQ untuk mengambil daftar lokasi.
LOCATIONS_URL = "https://api.openaq.org/v3/locations"
# Endpoint API OpenAQ untuk mengambil metadata sensor.
SENSORS_URL = "https://api.openaq.org/v3/sensors"

# Client Google Pub/Sub yang digunakan untuk menerbitkan event.
PUBLISHER = pubsub_v1.PublisherClient()

# Path lengkap topic Pub/Sub tujuan.
TOPIC_PATH = PUBLISHER.topic_path(
    PROJECT_ID,
    OPENAQ_TOPIC,
)

# Batas waktu tunggu setiap request HTTP dalam detik.
REQUEST_TIMEOUT = 30

# Hanya terima observasi dari 24 jam terakhir.
FRESHNESS_HOURS = 24

# ID parameter OpenAQ:
# 2 = PM2.5
PM25_PARAMETER_ID = 2


# ============================================================
# Pencarian lokasi aktif
# ============================================================

# Jumlah lokasi yang INGIN ditemukan dan benar-benar memiliki
# pengukuran PM2.5 terbaru.
TARGET_ACTIVE_LOCATIONS = 10

# Jumlah maksimum lokasi yang bersedia diperiksa.
MAX_LOCATIONS_TO_CHECK = 40

# Jumlah lokasi yang diminta dari OpenAQ untuk setiap halaman.
LOCATION_PAGE_SIZE = 100


# ============================================================
# Pembatasan laju permintaan
# ============================================================

# Jeda antar-request untuk mengurangi risiko terkena rate limit API.
REQUEST_DELAY_SECONDS = 1

# Jumlah maksimum percobaan ulang request ketika terjadi rate limit.
MAX_RETRIES = 5


# ============================================================
# Cache metadata sensor
# ============================================================

# Cache metadata sensor agar informasi sensor tidak perlu diminta berulang kali.
SENSOR_CACHE = {}


# ============================================================
# Fungsi bantuan HTTP
# ============================================================

# Mengirim request HTTP GET dengan mekanisme retry dan exponential backoff ketika API mengembalikan HTTP 429.
def request_with_retry(
    url,
    *,
    headers=None,
    params=None,
    max_retries=MAX_RETRIES,
):
    """
    Melakukan GET request dengan penanganan retry untuk HTTP 429.

    OpenAQ dapat mengembalikan HTTP 429 ketika batas permintaan API
    terlampaui.

    Gunakan Retry-After jika tersedia; jika tidak, gunakan
    exponential backoff.
    """

    for attempt in range(max_retries):

# Menyimpan response yang digunakan pada proses ini.
        response = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=REQUEST_TIMEOUT,
        )

        # Respons berhasil atau bukan respons akibat pembatasan laju.
        if response.status_code != 429:
            response.raise_for_status()
            return response

        # ----------------------------------------------------
        # Terkena pembatasan laju
        # ----------------------------------------------------

# Menyimpan retry after yang digunakan pada proses ini.
        retry_after = response.headers.get(
            "Retry-After"
        )

        if retry_after:

            try:
# Menyimpan wait seconds yang digunakan pada proses ini.
                wait_seconds = int(
                    retry_after
                )

            except ValueError:

# Menyimpan wait seconds yang digunakan pada proses ini.
                wait_seconds = min(
                    2 ** attempt,
                    30,
                )

        else:

# Menyimpan wait seconds yang digunakan pada proses ini.
            wait_seconds = min(
                2 ** attempt,
                30,
            )

        print(
            f"OpenAQ rate limit reached (429). "
            f"Waiting {wait_seconds}s "
            f"before retry "
            f"{attempt + 1}/{max_retries}..."
        )

        time.sleep(
            wait_seconds
        )

    raise requests.HTTPError(
        "OpenAQ request failed after "
        f"{max_retries} retries: {url}"
    )


# ============================================================
# Pencarian lokasi
# ============================================================

# Mengambil daftar lokasi OpenAQ di Indonesia yang memiliki parameter PM2.5.
def get_indonesia_locations():
    """
    Mencari lokasi Indonesia yang terkait dengan PM2.5.

    OpenAQ ditanyakan menggunakan:
        iso=ID
        parameters_id=2

    Validasi negara tetap dilakukan secara eksplisit setelahnya
    karena respons API tidak boleh menjadi
    satu-satunya lapisan validasi negara.
    """

# Menyimpan response yang digunakan pada proses ini.
    response = request_with_retry(
        LOCATIONS_URL,
        headers={
            "X-API-Key": OPENAQ_API_KEY,
        },
        params={
            "iso": "ID",
            "parameters_id": PM25_PARAMETER_ID,
            "limit": LOCATION_PAGE_SIZE,
        },
    )

    return response.json().get(
        "results",
        [],
    )


# ============================================================
# Validasi negara
# ============================================================

# Memvalidasi apakah lokasi OpenAQ benar-benar berada di wilayah Indonesia berdasarkan metadata negara dan koordinat.
def is_indonesia_location(location):
    """
    Memvalidasi bahwa lokasi OpenAQ benar-benar berada di Indonesia.

    Dua pemeriksaan digunakan:

    1. Metadata negara OpenAQ harus menunjukkan ID.
    2. Koordinat harus berada dalam
       batas geografis perkiraan Indonesia.

    Pemeriksaan ini mencegah lokasi OpenAQ yang salah penandaan
    masuk ke Indonesian logistics pipeline.
    """

    # --------------------------------------------------------
    # Pemeriksaan 1: metadata negara dari OpenAQ
    # --------------------------------------------------------

# Menyimpan metadata negara dari lokasi OpenAQ.
    country = location.get(
        "country"
    ) or {}

# Menyimpan kode negara lokasi untuk validasi Indonesia.
    country_code = str(
        country.get("code") or ""
    ).upper()

    if country_code != "ID":
        return False

    # --------------------------------------------------------
    # Pemeriksaan 2: koordinat geografis
    # --------------------------------------------------------

# Menyimpan coordinates yang digunakan pada proses ini.
    coordinates = (
        location.get(
            "coordinates"
        ) or {}
    )

# Menyimpan latitude yang digunakan pada proses ini.
    latitude = coordinates.get(
        "latitude"
    )

# Menyimpan longitude yang digunakan pada proses ini.
    longitude = coordinates.get(
        "longitude"
    )

    if latitude is None or longitude is None:
        return False

    try:

# Menyimpan latitude yang digunakan pada proses ini.
        latitude = float(latitude)
# Menyimpan longitude yang digunakan pada proses ini.
        longitude = float(longitude)

    except (TypeError, ValueError):

        return False

    # Batas geografis perkiraan wilayah Indonesia.
    INDONESIA_MIN_LAT = -11.0
# Menyimpan indonesia max lat yang digunakan pada proses ini.
    INDONESIA_MAX_LAT = 6.0

# Menyimpan indonesia min lon yang digunakan pada proses ini.
    INDONESIA_MIN_LON = 95.0
# Menyimpan indonesia max lon yang digunakan pada proses ini.
    INDONESIA_MAX_LON = 141.0

    return (
        INDONESIA_MIN_LAT
        <= latitude
        <= INDONESIA_MAX_LAT
        and
        INDONESIA_MIN_LON
        <= longitude
        <= INDONESIA_MAX_LON
    )

# ============================================================
# Pengukuran terbaru
# ============================================================

# Mengambil pengukuran terbaru dari lokasi OpenAQ tertentu.
def get_latest_measurements(location_id):
    """
    Mengambil pengukuran dari 24 jam terakhir.

    Penting:
    Endpoint /latest dapat mengembalikan beberapa jenis sensor.

    Karena itu metadata sensor harus diperiksa sebelum event dipublikasikan.
    """

# Menyimpan url yang digunakan pada proses ini.
    url = (
        f"https://api.openaq.org/v3/"
        f"locations/{location_id}/latest"
    )

# Menyimpan datetime min yang digunakan pada proses ini.
    datetime_min = (
        datetime.now(timezone.utc)
        - timedelta(hours=FRESHNESS_HOURS)
    ).isoformat()

# Menyimpan response yang digunakan pada proses ini.
    response = request_with_retry(
        url,
        headers={
            "X-API-Key": OPENAQ_API_KEY,
        },
        params={
            "datetime_min": datetime_min,
            "limit": 100,
        },
    )

    return response.json().get(
        "results",
        [],
    )


# ============================================================
# Metadata sensor
# ============================================================

# Mengambil metadata sensor OpenAQ berdasarkan ID sensor.
def get_sensor_metadata(sensor_id):
    """
    Mengambil metadata sensor.

    Hasilnya disimpan dalam cache agar sensor yang sama tidak
    diminta berulang kali.
    """

    if sensor_id in SENSOR_CACHE:
        return SENSOR_CACHE[sensor_id]

# Menyimpan url yang digunakan pada proses ini.
    url = f"{SENSORS_URL}/{sensor_id}"

# Menyimpan response yang digunakan pada proses ini.
    response = request_with_retry(
        url,
        headers={
            "X-API-Key": OPENAQ_API_KEY,
        },
    )

# Menyimpan results yang digunakan pada proses ini.
    results = response.json().get(
        "results",
        [],
    )

    if not results:

        SENSOR_CACHE[sensor_id] = None

        return None

# Menyimpan metadata sensor yang diperoleh dari OpenAQ.
    sensor = results[0]

    SENSOR_CACHE[sensor_id] = sensor

    return sensor


# ============================================================
# Validasi PM2.5
# ============================================================

# Memastikan sensor yang digunakan benar-benar mengukur parameter PM2.5.
def is_pm25_sensor(sensor):
    """
    Memastikan bahwa sensor mengukur PM2.5.

    OpenAQ:
        parameter.id = 2
        parameter.name = pm25
    """

    if not sensor:
        return False

# Menyimpan parameter yang digunakan pada proses ini.
    parameter = sensor.get(
        "parameter"
    ) or {}

# Menyimpan parameter id yang digunakan pada proses ini.
    parameter_id = parameter.get(
        "id"
    )

# Menyimpan nama parameter yang digunakan untuk validasi PM2.5.
    parameter_name = str(
        parameter.get("name") or ""
    ).lower()

    return (
        parameter_id == PM25_PARAMETER_ID
        or parameter_name == "pm25"
    )


# ============================================================
# Publikasikan event
# ============================================================

# Menerbitkan satu event hasil pengukuran ke topic Google Pub/Sub.
def publish_event(
    location,
    measurement,
    sensor,
):
    """
    Mempublikasikan satu observasi PM2.5 yang sudah lolos validasi.
    """

# Menyimpan parameter yang digunakan pada proses ini.
    parameter = sensor.get(
        "parameter"
    ) or {}

# Menyimpan event yang digunakan pada proses ini.
    event = {
        "source": "openaq",

        "ingestion_timestamp": (
            datetime.now(
                timezone.utc
            ).isoformat()
        ),

        # ----------------------------------------------------
        # Lokasi
        # ----------------------------------------------------

        "location_id": location.get(
            "id"
        ),

        "location_name": location.get(
            "name"
        ),

        "locality": location.get(
            "locality"
        ),

        "country_code": (
            location.get(
                "country"
            ) or {}
        ).get("code"),

        "latitude": (
            location.get(
                "coordinates"
            ) or {}
        ).get("latitude"),

        "longitude": (
            location.get(
                "coordinates"
            ) or {}
        ).get("longitude"),

        # ----------------------------------------------------
        # Parameter pengukuran
        # ----------------------------------------------------

        "parameter": parameter.get(
            "name",
            "pm25",
        ),

        "parameter_id": parameter.get(
            "id"
        ),

        "parameter_display_name": (
            parameter.get(
                "displayName"
            )
        ),

        "unit": parameter.get(
            "units"
        ),

        # ----------------------------------------------------
        # Pengukuran
        # ----------------------------------------------------

        "value": measurement.get(
            "value"
        ),

        "measurement_datetime": (
            measurement.get(
                "datetime"
            )
        ),

        # ----------------------------------------------------
        # Informasi sensor
        # ----------------------------------------------------

        "sensor_id": measurement.get(
            "sensorsId"
        ),

        "source_location_id": (
            measurement.get(
                "locationsId"
            )
        ),
    }

# Menyimpan pesan Pub/Sub yang akan dipublikasikan.
    message = json.dumps(
        event,
        ensure_ascii=False,
    ).encode("utf-8")

# Menyimpan objek future untuk memantau hasil publikasi asinkron.
    future = PUBLISHER.publish(
        TOPIC_PATH,
        message,
    )

# Menyimpan ID pesan yang dikembalikan Pub/Sub setelah publikasi.
    message_id = future.result()

    print(
        "  PUBLISHED PM2.5 | "
        f"location={location.get('id')} | "
        f"name={location.get('name')} | "
        f"value={measurement.get('value')} | "
        f"sensor={measurement.get('sensorsId')} | "
        f"message_id={message_id}"
    )


# ============================================================
# Fungsi utama
# ============================================================

# Menjalankan alur utama script dari awal sampai selesai.
def main():

    print("=" * 60)
    print("Starting OpenAQ publisher...")
    print("=" * 60)

    print(
        f"Project: {PROJECT_ID}"
    )

    print(
        f"Topic: {OPENAQ_TOPIC}"
    )

    print(
        f"Freshness window: "
        f"{FRESHNESS_HOURS} hours"
    )

    print(
        f"PM2.5 parameter ID: "
        f"{PM25_PARAMETER_ID}"
    )

    print(
        f"Target active locations: "
        f"{TARGET_ACTIVE_LOCATIONS}"
    )

    print(
        f"Maximum locations to check: "
        f"{MAX_LOCATIONS_TO_CHECK}"
    )

    print(
        f"Request delay: "
        f"{REQUEST_DELAY_SECONDS}s"
    )

    # --------------------------------------------------------
    # Mencari lokasi OpenAQ
    # --------------------------------------------------------

    try:

# Menyimpan daftar lokasi yang ditemukan dari OpenAQ.
        locations = get_indonesia_locations()

    except requests.RequestException as error:

        print(
            "Failed to discover Indonesian "
            f"locations: {error}"
        )

        return

    print()

    print(
        f"Discovered {len(locations)} "
        "Indonesian PM2.5 locations."
    )

    # --------------------------------------------------------
    # Membatasi jumlah lokasi yang diperiksa
    # --------------------------------------------------------

# Menyimpan daftar lokasi yang akan diperiksa lebih lanjut.
    locations_to_check = locations[
        :MAX_LOCATIONS_TO_CHECK
    ]

    print(
        f"Will inspect "
        f"{len(locations_to_check)} "
        "locations."
    )

    # --------------------------------------------------------
    # Penghitung statistik proses
    # --------------------------------------------------------

# Menyimpan jumlah lokasi yang sudah diperiksa.
    locations_checked = 0
# Menyimpan lokasi yang memiliki pengukuran terkini.
    active_locations = 0

# Menyimpan jumlah pengukuran yang sudah diperiksa.
    measurements_checked = 0
# Menyimpan jumlah observasi PM2.5 yang ditemukan.
    pm25_found = 0
# Menyimpan jumlah event yang berhasil dipublikasikan.
    published = 0

# Menyimpan jumlah kesalahan saat mengambil data lokasi.
    location_errors = 0
# Menyimpan jumlah kesalahan saat mengambil metadata sensor.
    sensor_errors = 0
# Menyimpan jumlah pengukuran non-PM2.5 yang dilewati.
    non_pm25_skipped = 0
# Menyimpan jumlah lokasi yang dilewati karena tidak memenuhi validasi Indonesia.
    non_indonesia_skipped = 0

    # --------------------------------------------------------
    # Memproses lokasi
    # --------------------------------------------------------

    try:

        for location in locations_to_check:

            # ------------------------------------------------
            # Mengambil ID lokasi
            # ------------------------------------------------

# Menyimpan ID lokasi OpenAQ yang sedang diproses.
            location_id = location.get(
                "id"
            )

            if not location_id:
                continue

            # ------------------------------------------------
            # EXPLICIT COUNTRY VALIDATION
            # ------------------------------------------------
            #
            # Jangan hanya mengandalkan:
            #
            #     iso=ID
            #
            # OpenAQ sebelumnya pernah mengembalikan data berikut:
            #
            #     6315887
            #     Свищов - СПГ Алеко Константинов
            #
            # Karena itu, validasi berikut harus dipenuhi:
            #
            #     country.code == "ID"
            #
            # ------------------------------------------------

            if not is_indonesia_location(location):

# Menyimpan metadata negara dari lokasi OpenAQ.
                country = location.get(
                    "country"
                ) or {}

# Menyimpan kode negara lokasi untuk validasi Indonesia.
                country_code = (
                    country.get(
                        "code"
                    )
                    or "unknown"
                )

# Menyimpan jumlah lokasi yang dilewati karena tidak memenuhi validasi Indonesia.
                non_indonesia_skipped += 1

                print(
                    f"[SKIP] Non-Indonesia "
                    f"location {location_id}: "
                    f"{location.get('name')} "
                    f"(country={country_code})"
                )

                continue
            # ------------------------------------------------
            # Menghentikan pencarian setelah jumlah lokasi aktif yang dibutuhkan tercapai.
            # ------------------------------------------------

            if (
                active_locations
                >= TARGET_ACTIVE_LOCATIONS
            ):

                print()

                print(
                    "Target number of active "
                    "locations reached."
                )

                break

# Menyimpan jumlah lokasi yang sudah diperiksa.
            locations_checked += 1

            print()

            print(
                f"[{locations_checked}/"
                f"{len(locations_to_check)}] "
                f"Checking "
                f"{location_id} "
                f"({location.get('name')})"
            )

            # ------------------------------------------------
            # Ambil pengukuran terbaru
            # ------------------------------------------------

            try:

# Menyimpan pengukuran terbaru dari lokasi tersebut.
                measurements = (
                    get_latest_measurements(
                        location_id
                    )
                )

            except requests.RequestException as error:

# Menyimpan jumlah kesalahan saat mengambil data lokasi.
                location_errors += 1

                print(
                    f"  Location request failed: "
                    f"{error}"
                )

                time.sleep(
                    REQUEST_DELAY_SECONDS
                )

                continue

            print(
                f"  Recent measurements: "
                f"{len(measurements)}"
            )

# Menyimpan jumlah pengukuran yang sudah diperiksa.
            measurements_checked += len(
                measurements
            )

            # ------------------------------------------------
            # Tidak ada data terkini
            # ------------------------------------------------

            if not measurements:

                print(
                    "  No recent measurements."
                )

                time.sleep(
                    REQUEST_DELAY_SECONDS
                )

                continue

            # ------------------------------------------------
            # Menentukan sensor PM2.5
            # ------------------------------------------------

# Menyimpan observasi yang berpotensi merupakan pengukuran PM2.5.
            pm25_measurements = []

            for measurement in measurements:

# Menyimpan ID sensor yang terkait dengan pengukuran.
                sensor_id = measurement.get(
                    "sensorsId"
                )

                if not sensor_id:

                    print(
                        "  Skipping measurement "
                        "without sensor ID."
                    )

                    continue

                try:

# Menyimpan metadata sensor yang diperoleh dari OpenAQ.
                    sensor = (
                        get_sensor_metadata(
                            sensor_id
                        )
                    )

                except requests.RequestException as error:

# Menyimpan jumlah kesalahan saat mengambil metadata sensor.
                    sensor_errors += 1

                    print(
                        f"  Sensor {sensor_id} "
                        f"metadata failed: "
                        f"{error}"
                    )

                    continue

                # ------------------------------------------------
                # Hanya terima PM2.5
                # ------------------------------------------------

                if not is_pm25_sensor(
                    sensor
                ):

# Menyimpan jumlah pengukuran non-PM2.5 yang dilewati.
                    non_pm25_skipped += 1

# Menyimpan parameter yang digunakan pada proses ini.
                    parameter = (
                        sensor or {}
                    ).get(
                        "parameter"
                    ) or {}

                    print(
                        f"  Skip sensor "
                        f"{sensor_id}: "
                        f"{parameter.get('name', 'unknown')}"
                    )

                    continue

                pm25_measurements.append(
                    (
                        measurement,
                        sensor,
                    )
                )

            # ------------------------------------------------
            # PM2.5 found
            # ------------------------------------------------

            if pm25_measurements:

# Menyimpan lokasi yang memiliki pengukuran terkini.
                active_locations += 1

                print(
                    f"  PM2.5 observations found: "
                    f"{len(pm25_measurements)}"
                )

                for (
                    measurement,
                    sensor,
                ) in pm25_measurements:

# Menyimpan jumlah observasi PM2.5 yang ditemukan.
                    pm25_found += 1

                    publish_event(
                        location,
                        measurement,
                        sensor,
                    )

# Menyimpan jumlah event yang berhasil dipublikasikan.
                    published += 1

            else:

                print(
                    "  No PM2.5 observation "
                    "available."
                )

            # ------------------------------------------------
            # Perlindungan terhadap batas permintaan API
            # ------------------------------------------------

            time.sleep(
                REQUEST_DELAY_SECONDS
            )

    except KeyboardInterrupt:

        print()

        print(
            "Publisher interrupted by user."
        )

    # --------------------------------------------------------
    # Ringkasan akhir
    # --------------------------------------------------------

    print()

    print("=" * 60)
    print("OpenAQ publishing completed.")
    print("=" * 60)

    print(
        f"Locations discovered: "
        f"{len(locations)}"
    )

    print(
        f"Locations checked: "
        f"{locations_checked}"
    )

    print(
        f"Non-Indonesia locations skipped: "
        f"{non_indonesia_skipped}"
    )

    print(
        f"Active locations found: "
        f"{active_locations}"
    )

    print(
        f"Measurements checked: "
        f"{measurements_checked}"
    )

    print(
        f"PM2.5 observations found: "
        f"{pm25_found}"
    )

    print(
        f"Non-PM2.5 measurements skipped: "
        f"{non_pm25_skipped}"
    )

    print(
        f"Sensor metadata errors: "
        f"{sensor_errors}"
    )

    print(
        f"Location errors: "
        f"{location_errors}"
    )

    print(
        f"PM2.5 messages published: "
        f"{published}"
    )

    print(
        f"Sensor metadata cached: "
        f"{len(SENSOR_CACHE)}"
    )

    print("=" * 60)


if __name__ == "__main__":
    main()