import os
import requests
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

API_BASE = "http://127.0.0.1:8000"

st.set_page_config(page_title="Game Price Watcher", layout="centered")
st.title("🎮 Game Price Watcher")
st.caption("Search one game → see its history & chart")

st.divider()

game_name = st.text_input("Enter game name", value="overwatch 2")

if st.button("Search"):
    try:
        r = requests.get(
            f"{API_BASE}/fetch_by_name",
            params={"name": game_name},
            timeout=20
        )
        r.raise_for_status()
        item = r.json()
    except Exception as e:
        st.error(f"Fetch failed: {e}")
        st.stop()

    if "appid" not in item:
        st.error("Game not found")
        st.stop()

    appid = item["appid"]
    st.success(f"Fetched: {item['title']} (appid={appid})")

    try:
        r = requests.get(
            f"{API_BASE}/history",
            params={"appid": appid, "limit": 500},
            timeout=20
        )
        r.raise_for_status()
        rows = r.json()
    except Exception as e:
        st.error(f"History load failed: {e}")
        st.stop()

    if not rows:
        st.info("No history yet")
        st.stop()

    df = pd.DataFrame(rows)
    df["time"] = df["ts"].apply(lambda x: datetime.fromtimestamp(int(x)))
    df = df.sort_values("time")

    st.divider()
    st.subheader("📜 History")
    st.dataframe(
        df[["time", "title", "price", "currency"]],
        use_container_width=True
    )

    st.subheader("💰 Price Trend")

    df_price = df.dropna(subset=["price"]).copy()
    if df_price.empty:
        st.warning("This game has no price data (maybe free-to-play).")
    else:
        fig = plt.figure()
        plt.plot(df_price["time"], df_price["price"])
        plt.xlabel("Time")
        plt.ylabel("Price")
        plt.xticks(rotation=30)
        st.pyplot(fig)

  