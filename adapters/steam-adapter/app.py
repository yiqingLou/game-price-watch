import os, time, random
import ujson as json
import requests
from datetime import datetime, timezone
from confluent_kafka import Producer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = os.getenv("TOPIC", "raw.prices")

APPIDS = [int(x) for x in os.getenv("STEAM_APPIDS", "1245620,1086940,1091500,1145360,1623730").split(",")]
REGIONS = [x.strip().upper() for x in os.getenv("STEAM_REGIONS", "US,EU,JP,CN").split(",")]
LANG = os.getenv("STEAM_LANG", "english")

REQUEST_INTERVAL = float(os.getenv("STEAM_REQUEST_INTERVAL", "10"))
BATCH_INTERVAL   = float(os.getenv("STEAM_BATCH_INTERVAL", "1800"))
MAX_RETRIES      = int(os.getenv("STEAM_MAX_RETRIES", "3"))
UA               = os.getenv("STEAM_UA", "Mozilla/5.0 (compatible; GamePriceWatch/1.0)")

HTTP_PROXY = os.getenv("HTTP_PROXY")
HTTPS_PROXY = os.getenv("HTTPS_PROXY")
PROXIES = {"http": HTTP_PROXY, "https": HTTPS_PROXY} if (HTTP_PROXY or HTTPS_PROXY) else None

p = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "enable.idempotence": True,
    "acks": "all",
    "compression.type": "zstd",
    "linger.ms": 5,
})

HEADERS = {
    "User-Agent": UA,
    "Accept": "application/json,text/javascript,*/*;q=0.1",
}

def fetch_price(appid: int, cc: str, lang: str):
    url = f"https://store.steampowered.com/api/appdetails?appids={appid}&cc={cc}&l={lang}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, timeout=20, headers=HEADERS, proxies=PROXIES)
            if r.status_code == 429:
                backoff = min(600, 30 * attempt)
                print(f"fetch 429 appid={appid} cc={cc}, backoff {backoff}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(backoff)
                continue
            r.raise_for_status()
            payload = r.json().get(str(appid), {})
            if not payload.get("success"):
                return None
            data = payload.get("data", {}) or {}
            name = data.get("name") or f"app_{appid}"
            pov = data.get("price_overview") or {}
            final = pov.get("final")
            initial = pov.get("initial")
            currency = pov.get("currency") or None
            if final is None and initial is None:
                return None
            price = (final or initial) / 100.0
            original = (initial or final) / 100.0
            discount_pct = float(pov.get("discount_percent") or 0.0)
            ts = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
            return {
                "game_id": str(appid),
                "title": name,
                "platform": "STEAM",
                "region": cc,
                "currency": currency or ("USD" if cc == "US" else "UNKNOWN"),
                "price": round(price, 2),
                "original_price": round(original, 2),
                "discount_pct": round(discount_pct, 2),
                "ts_event": ts,
            }
        except Exception as e:
            if attempt >= MAX_RETRIES:
                print(f"fetch error appid={appid} cc={cc} err={e}")
                break
            backoff = min(120, 5 * attempt)
            print(f"fetch error appid={appid} cc={cc} err={e}, retry in {backoff}s (attempt {attempt}/{MAX_RETRIES})")
            time.sleep(backoff)
    return None

def run_once():
    print(f"[steam-adapter] start batch. appids={APPIDS} regions={REGIONS} lang={LANG}")
    for appid in APPIDS:
        for cc in random.sample(REGIONS, k=len(REGIONS)):
            rec = fetch_price(appid, cc, LANG)
            if rec:
                try:
                    p.produce(TOPIC, json.dumps(rec).encode("utf-8"))
                    p.poll(0)
                except Exception as e:
                    print("produce error:", e)
            time.sleep(REQUEST_INTERVAL)
    try:
        p.flush()
    except Exception:
        pass

print(f"[steam-adapter] start. appids={APPIDS} regions={REGIONS} lang={LANG}")
while True:
    batch_begin = time.time()
    run_once()
    elapsed = time.time() - batch_begin
    sleep_left = BATCH_INTERVAL - elapsed
    if sleep_left > 0:
        print(f"[steam-adapter] batch done in {elapsed:.1f}s, sleep {sleep_left:.1f}s to next batch")
        time.sleep(sleep_left)
