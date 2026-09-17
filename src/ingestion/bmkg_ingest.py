import argparse
from datetime import datetime, timezone
from pathlib import Path
import requests

URL = "https://www.bmkg.go.id/alerts/nowcast/id"

def fetch_bmkg(output_dir: str):
    r = requests.get(URL, timeout=30)
    r.raise_for_status()
    ts = datetime.now(timezone.utc)
    day = ts.strftime("%Y-%m-%d")
    stamp = ts.strftime("%Y%m%dT%H%M%SZ")
    out = Path(output_dir) / f"ingestion_date={day}"
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"bmkg_{stamp}.xml"
    path.write_bytes(r.content)
    return str(path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="data/raw/bmkg")
    args = parser.parse_args()
    print(fetch_bmkg(args.output))
