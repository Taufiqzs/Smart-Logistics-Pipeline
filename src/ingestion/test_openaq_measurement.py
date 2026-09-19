import os
import requests
from dotenv import load_dotenv

load_dotenv()

# API key yang digunakan oleh script pengujian OpenAQ.
API_KEY = os.environ["OPENAQ_API_KEY"]

# ID lokasi OpenAQ yang sedang diuji.
LOCATION_ID = 2537

# Endpoint API yang digunakan oleh script pengujian.
URL = f"https://api.openaq.org/v3/locations/{LOCATION_ID}/latest"


# Menjalankan alur utama script dari awal sampai selesai.
def main():
# Menyimpan response yang digunakan pada proses ini.
    response = requests.get(
        URL,
        headers={"X-API-Key": API_KEY},
        timeout=30,
    )

    print("HTTP status:", response.status_code)

    response.raise_for_status()

# Menyimpan data yang digunakan pada proses ini.
    data = response.json()

    print("API request successful")
    print("Location ID:", LOCATION_ID)
    print("Response:")

    for item in data.get("results", []):
        print(item)


if __name__ == "__main__":
    main()