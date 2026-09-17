import argparse, json, os
from datetime import datetime, timezone
import requests
from google.cloud import pubsub_v1
from src.common.config import GCP_PROJECT_ID, OPENAQ_API_KEY, OPENAQ_TOPIC

BASE = "https://api.openaq.org/v3/locations"

def get_locations():
    headers = {"X-API-Key": OPENAQ_API_KEY}
    params = {"iso": "ID", "limit": 100, "page": 1}
    r = requests.get(BASE, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    return r.json().get("results", [])

def publish_once():
    if not GCP_PROJECT_ID or not OPENAQ_API_KEY:
        raise RuntimeError("Set GCP_PROJECT_ID and OPENAQ_API_KEY")
    publisher = pubsub_v1.PublisherClient()
    topic = publisher.topic_path(GCP_PROJECT_ID, OPENAQ_TOPIC)

    count = 0
    for loc in get_locations():
        payload = {
            "event_id": f"openaq-{loc.get('id')}-{datetime.now(timezone.utc).isoformat()}",
            "source": "openaq",
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "location_id": loc.get("id"),
            "location_name": loc.get("name"),
            "country": "ID",
            "coordinates": loc.get("coordinates"),
        }
        publisher.publish(topic, json.dumps(payload).encode("utf-8"))
        count += 1
    print(f"published={count}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    parser.parse_args()
    publish_once()
