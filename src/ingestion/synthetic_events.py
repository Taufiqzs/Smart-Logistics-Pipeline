import argparse, csv, random, uuid
from datetime import datetime, timedelta, timezone
from faker import Faker
from src.common.config import CITIES

fake = Faker("id_ID")

def generate(rows):
    start = datetime.now(timezone.utc) - timedelta(days=30)
    cities = list(CITIES)
    for _ in range(rows):
        city = random.choice(cities)
        lat, lon = CITIES[city]
        expected = random.randint(20, 60)
        delay = max(0, int(random.gauss(8, 10)))
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
    p = argparse.ArgumentParser()
    p.add_argument("--rows", type=int, default=5000)
    p.add_argument("--output", default="data/raw/logistics/synthetic_orders.csv")
    args = p.parse_args()
    rows = list(generate(args.rows))
    import pathlib
    pathlib.Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"generated {len(rows)} rows -> {args.output}")
