import os

from dotenv import load_dotenv


load_dotenv()


# ============================================================
# KONFIGURASI GCP
# ============================================================

# ID project Google Cloud yang digunakan oleh pipeline.
PROJECT_ID = os.getenv("PROJECT_ID", "jcdeah-009")
# Region Google Cloud tempat resource dan pipeline dijalankan.
REGION = os.getenv("REGION", "asia-southeast2")

# Nama bucket GCS untuk menyimpan data mentah atau Bronze.
RAW_BUCKET = os.getenv(
    "RAW_BUCKET",
    f"{PROJECT_ID}-smart-logistics-raw",
)

# Nama bucket GCS untuk menyimpan data hasil pembersihan atau Silver.
SILVER_BUCKET = os.getenv(
    "SILVER_BUCKET",
    f"{PROJECT_ID}-smart-logistics-silver",
)

# Nama bucket GCS yang digunakan sebagai lokasi sementara proses cloud.
TEMP_BUCKET = os.getenv(
    "TEMP_BUCKET",
    f"{PROJECT_ID}-smart-logistics-temp",
)


# ============================================================
# KONFIGURASI PUB/SUB
# ============================================================

# Nama topic Pub/Sub untuk mengirim event dari OpenAQ.
OPENAQ_TOPIC = os.getenv(
    "OPENAQ_TOPIC",
    "openaq-events",
)

# Nama topic Pub/Sub untuk event data driver/logistik.
DRIVER_TOPIC = os.getenv(
    "DRIVER_TOPIC",
    "driver-events",
)


# ============================================================
# KONFIGURASI API
# ============================================================

# API key OpenAQ yang digunakan untuk autentikasi request.
OPENAQ_API_KEY = os.getenv(
    "OPENAQ_API_KEY",
    "",
)


# ============================================================
# KOTA YANG DIGUNAKAN DALAM PROYEK
# ============================================================

# Daftar kota operasional beserta koordinat latitude dan longitude.
CITIES = {
    "Jakarta": (-6.2088, 106.8456),
    "Bandung": (-6.9175, 107.6191),
    "Surabaya": (-7.2575, 112.7521),
    "Semarang": (-6.9667, 110.4167),
    "Yogyakarta": (-7.7956, 110.3695),
    "Medan": (3.5952, 98.6722),
}