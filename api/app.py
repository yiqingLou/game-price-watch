from fastapi import FastAPI, Query
from pydantic import BaseModel
from typing import Optional
import requests, json, time, os

DATA_FILE = "/data/prices.jsonl"
app = FastAPI()

class FetchReq(BaseModel):
    appid: int

def fetch_by_appid(appid: int):
    url = f"https://store.steampowered.com/api/appdetails?appids={appid}&cc=us&l=en"
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    raw = r.json()

    data = raw.get(str(appid), {}).get("data") or {}
    price = data.get("price_overview") or {}

    item = {
        "appid": appid,
        "title": data.get("name"),
        "price": price.get("final", 0) / 100 if price else None,
        "currency": price.get("currency"),
        "ts": int(time.time())
    }

    os.makedirs("/data", exist_ok=True)
    with open(DATA_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(item, ensure_ascii=False) + "\n")

    return item

def find_appid_by_name(name: str):
    search_url = "https://store.steampowered.com/api/storesearch"
    r = requests.get(search_url, params={"term": name, "cc": "US", "l": "english"}, timeout=15)
    r.raise_for_status()
    items = r.json().get("items", [])
    if not items:
        return None
    return items[0].get("id")

def load_history(appid: Optional[int] = None, limit: int = 500):
    if not os.path.exists(DATA_FILE):
        return []

    rows = []
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except:
                continue

            if appid is not None and int(item.get("appid", -1)) != int(appid):
                continue

            rows.append(item)

    rows.sort(key=lambda x: int(x.get("ts", 0)), reverse=True)
    return rows[:limit]

@app.post("/fetch")
def fetch(req: FetchReq):
    return fetch_by_appid(req.appid)

@app.get("/fetch_by_name")
def fetch_by_name(name: str = Query(...)):
    appid = find_appid_by_name(name)
    if not appid:
        return {"error": "Game not found"}
    return fetch_by_appid(int(appid))

@app.get("/history")
def history(
    appid: Optional[int] = Query(default=None),
    limit: int = Query(default=500, ge=1, le=5000)
):
    return load_history(appid=appid, limit=limit)
