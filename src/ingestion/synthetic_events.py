import argparse, csv, random, uuid
from datetime import datetime, timedelta, timezone
from faker import Faker
from src.common.config import CITIES

# Menyimpan fake yang digunakan pada proses ini.
fake = Faker("id_ID")

# Menghasilkan data event logistik sintetis untuk kebutuhan pengujian dan simulasi pipeline.
def generate(rows):
# Menyimpan start yang digunakan pada proses ini.
    start = datetime.now(timezone.utc) - timedelta(days=30)
# Menyimpan cities yang digunakan pada proses ini.
    cities = list(CITIES)
    for _ in range(rows):
# Menyimpan city yang digunakan pada proses ini.
        city = random.choice(cities)
# Menyimpan `lat`, `lon` yang digunakan pada proses ini.
        lat, lon = CITIES[city]
# Menyimpan expected yang digunakan pada proses ini.
        expected = random.randint(20, 60)
# Menyimpan delay yang digunakan pada proses ini.
        delay = max(0, int(random.gauss(8, 10)))
# Menyimpan actual yang digunakan pada proses ini.
        actual = expected + delay
        yield {
            "event_id": str(uuid.uuid4()),
            "event_ts": (start + timedelta(minutes=random.randint(0, 30*24*60))).isoformat(),
            "city": city,
            "district": fake.city_suffix(),
            "driver_id": f"DRV-{random.randint(1,250):04d}",
            "order_id": f"ORD-{random.randint(1,20000):06d}",
            "latitude": lat + random.uniform(-0.05, 0.05),
            "longitude": lon + random.uniform(-0.05, 0.05),
            "expected_delivery_min": expected,
            "actual_delivery_min": actual,
            "delay_min": delay,
            "route_rating": round(random.uniform(2.5, 5.0), 2),
            "status": random.choice(["completed", "completed", "completed", "delayed"]),
        }

if __name__ == "__main__":
# Menyimpan p yang digunakan pada proses ini.
    p = argparse.ArgumentParser()
    p.add_argument("--rows", type=int, default=5000)
    p.add_argument("--output", default="data/raw/logistics/synthetic_orders.csv")
# Menyimpan args yang digunakan pada proses ini.
    args = p.parse_args()
# Menyimpan rows yang digunakan pada proses ini.
    rows = list(generate(args.rows))
    import pathlib
    pathlib.Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w", newline="", encoding="utf-8") as f:
# Menyimpan writer yang digunakan pada proses ini.
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"generated {len(rows)} rows -> {args.output}")
