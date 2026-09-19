import os

import requests
from dotenv import load_dotenv


load_dotenv()

# API key yang digunakan oleh script pengujian OpenAQ.
API_KEY = os.environ["OPENAQ_API_KEY"]

# Endpoint API yang digunakan oleh script pengujian.
URL = "https://api.openaq.org/v3/locations"


# Menjalankan alur utama script dari awal sampai selesai.
def main():
# Menyimpan response yang digunakan pada proses ini.
    response = requests.get(
        URL,
        headers={
            "X-API-Key": API_KEY
        },
        params={
            "iso": "ID",
            "parameters_id": 2,
            "limit": 10,
        },
        timeout=30,
    )

    print("HTTP status:", response.status_code)

    response.raise_for_status()

# Menyimpan data yang digunakan pada proses ini.
    data = response.json()

    print("Indonesia PM2.5 locations:")
    print("Found:", data.get("meta", {}).get("found"))

    for location in data.get("results", []):
        print(
            {
                "location_id": location.get("id"),
                "name": location.get("name"),
                "locality": location.get("locality"),
                "country": location.get("country"),
                "coordinates": location.get("coordinates"),
            }
        )


if __name__ == "__main__":
    main()