
import json, os, sys, signal
from kafka import KafkaConsumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = os.getenv("TOPIC", "raw.prices")
OUT = os.getenv("OUT_FILE", "/data/prices.jsonl")  

stop = False
def handle_sig(sig, frame):
    global stop
    stop = True

signal.signal(signal.SIGINT, handle_sig)
signal.signal(signal.SIGTERM, handle_sig)

print(f"[price-writer] consume {TOPIC} -> {OUT} (bootstrap={BOOTSTRAP})", flush=True)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

os.makedirs(os.path.dirname(OUT), exist_ok=True)

with open(OUT, "a", encoding="utf-8") as f:
    for msg in consumer:
        if stop:
            break
        json.dump(msg.value, f, ensure_ascii=False)
        f.write("\n")
        f.flush()
        print(f"[price-writer] wrote 1 record", flush=True)

consumer.close()
print("[price-writer] bye", flush=True)
