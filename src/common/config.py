import os
from dotenv import load_dotenv

load_dotenv()

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
GCP_REGION = os.getenv("GCP_REGION", "asia-southeast2")
RAW_BUCKET = os.getenv("GCS_RAW_BUCKET", "")
SILVER_BUCKET = os.getenv("GCS_SILVER_BUCKET", "")
TEMP_BUCKET = os.getenv("GCS_TEMP_BUCKET", "")
OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY", "")
OPENAQ_TOPIC = os.getenv("PUBSUB_OPENAQ_TOPIC", "openaq-events")
DRIVER_TOPIC = os.getenv("PUBSUB_DRIVER_TOPIC", "driver-events")

CITIES = {
    "Jakarta": (-6.2088, 106.8456),
    "Bandung": (-6.9175, 107.6191),
    "Surabaya": (-7.2575, 112.7521),
    "Semarang": (-6.9667, 110.4167),
    "Yogyakarta": (-7.7956, 110.3695),
    "Medan": (3.5952, 98.6722),
}
