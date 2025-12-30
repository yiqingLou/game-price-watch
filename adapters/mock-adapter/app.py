import os, time, random, ujson as json
from datetime import datetime, timezone
from confluent_kafka import Producer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = os.getenv("TOPIC", "raw.prices")

p = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "enable.idempotence": True,
    "acks": "all",
    "linger.ms": 5,
    "batch.num.messages": 1000,
    "compression.type": "zstd",
})

GAMES = [
    ("g001", "Elden Ring"),
    ("g002", "Baldur's Gate 3"),
    ("g003", "Cyberpunk 2077"),
    ("g004", "Hades II"),
    ("g005", "Palworld"),
]
PLATFORMS = ["STEAM", "PS", "XBOX", "SWITCH"]
REGIONS = ["US", "EU", "JP", "CN"]
CURRENCIES = {"US": "USD", "EU": "EUR", "JP": "JPY", "CN": "CNY"}

base_price = {gid: random.uniform(9, 69) for gid, _ in GAMES}

def delivery(err, msg):
    if err:
        print(f" delivery failed: {err}")

while True:
    gid, title = random.choice(GAMES)
    platform = random.choice(PLATFORMS)
    region = random.choice(REGIONS)
    curr = CURRENCIES[region]

    noise = random.uniform(-5, 5)
    price = max(1.0, base_price[gid] + noise)
    original = price * random.choice([1.0, 1.0, 1.2, 1.5])
    discount = 0.0 if original == 0 else max(0.0, 1 - price / original) * 100

    event_ts = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

    payload = {
        "game_id": gid,
        "title": title,
        "platform": platform,
        "region": region,
        "currency": curr,
        "price": round(price, 2),
        "original_price": round(original, 2),
        "discount_pct": round(discount, 2),
        "ts_event": event_ts,
    }

    p.produce(TOPIC, json.dumps(payload).encode("utf-8"), callback=delivery)
    p.poll(0)
    time.sleep(0.05)  # ~20 msg/s
