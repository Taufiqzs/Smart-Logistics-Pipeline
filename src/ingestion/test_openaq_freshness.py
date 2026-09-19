import os
from datetime import datetime, timedelta, timezone

import requests
from dotenv import load_dotenv

load_dotenv()

# API key yang digunakan oleh script pengujian OpenAQ.
API_KEY = os.environ["OPENAQ_API_KEY"]

# Endpoint API OpenAQ untuk mengambil daftar lokasi.
LOCATIONS_URL = "https://api.openaq.org/v3/locations"

# Header HTTP yang dikirim pada request API.
headers = {
    "X-API-Key": API_KEY,
}


# Fungsi `get_locations()` digunakan untuk menjalankan proses get locations.
def get_locations(limit=100):
# Menyimpan response yang digunakan pada proses ini.
    response = requests.get(
        LOCATIONS_URL,
        headers=headers,
        params={
            "iso": "ID",
            "parameters_id": 2,
            "limit": limit,
        },
        timeout=30,
    )

    response.raise_for_status()

    return response.json().get("results", [])


# Fungsi `get_recent_measurements()` digunakan untuk menjalankan proses get recent measurements.
def get_recent_measurements(location_id):
# Menyimpan url yang digunakan pada proses ini.
    url = (
        f"https://api.openaq.org/v3/"
        f"locations/{location_id}/latest"
    )

# Menyimpan datetime min yang digunakan pada proses ini.
    datetime_min = (
        datetime.now(timezone.utc)
        - timedelta(hours=24)
    ).isoformat()

# Menyimpan response yang digunakan pada proses ini.
    response = requests.get(
        url,
        headers=headers,
        params={
            "datetime_min": datetime_min,
            "limit": 100,
        },
        timeout=30,
    )

    response.raise_for_status()

    return response.json().get("results", [])


# Menjalankan alur utama script dari awal sampai selesai.
def main():
# Menyimpan locations yang digunakan pada proses ini.
    locations = get_locations(limit=100)

    print(f"Locations discovered: {len(locations)}")

# Menyimpan current locations yang digunakan pada proses ini.
    current_locations = 0
# Menyimpan current measurements yang digunakan pada proses ini.
    current_measurements = 0

    for location in locations:
# Menyimpan location id yang digunakan pada proses ini.
        location_id = location.get("id")

        try:
# Menyimpan measurements yang digunakan pada proses ini.
            measurements = get_recent_measurements(
                location_id
            )

            if measurements:
# Menyimpan current locations yang digunakan pada proses ini.
                current_locations += 1
# Menyimpan current measurements yang digunakan pada proses ini.
                current_measurements += len(measurements)

                print(
                    f"CURRENT location="
                    f"{location_id} "
                    f"name={location.get('name')} "
                    f"measurements={len(measurements)}"
                )

                for measurement in measurements:
                    print(
                        f"  value={measurement.get('value')} "
                        f"datetime={measurement.get('datetime')}"
                    )

        except requests.RequestException as error:
            print(
                f"ERROR location={location_id}: {error}"
            )

    print()
    print("Discovery completed.")
    print(f"Locations checked: {len(locations)}")
    print(f"Locations with recent data: {current_locations}")
    print(f"Recent measurements: {current_measurements}")


if __name__ == "__main__":
    main()