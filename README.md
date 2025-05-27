import os
import time
import requests
import pandas as pd
import schedule
from datetime import datetime

# Binance API endpoint
BASE_URL = "https://api.binance.com/api/v3/klines"
SYMBOL = "ETHUSDT"
LIMIT = 500

# Function to fetch and save kline data for a given interval
def fetch_and_save(interval):
    try:
        print(f"[{datetime.now()}] Fetching {interval} data...")
        params = {
            "symbol": SYMBOL,
            "interval": interval,
            "limit": LIMIT
        }
        response = requests.get(BASE_URL, params=params)
        data = response.json()

        if isinstance(data, dict) and data.get("code"):
            print("Error from API:", data)
            return

        df = pd.DataFrame(data, columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "number_of_trades",
            "taker_buy_base_vol", "taker_buy_quote_vol", "ignore"
        ])

        # Convert and clean data
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
        df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")
        df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].astype(float)

        # Create folder for this timeframe
        folder = f"data/{interval}"
        os.makedirs(folder, exist_ok=True)

        # Save to CSV
        filename = f"{folder}/ethusdt_{interval}_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.csv"
        df[["open_time", "open", "high", "low", "close", "volume"]].to_csv(filename, index=False)
        print(f"✓ Saved: {filename}")

    except Exception as e:
        print(f"Error during fetch ({interval}):", e)

# Schedule jobs for each timeframe
schedule.every(1).minutes.do(lambda: fetch_and_save("1m"))
schedule.every(15).minutes.do(lambda: fetch_and_save("15m"))
schedule.every().hour.at(":00").do(lambda: fetch_and_save("1h"))
schedule.every(4).hours.at(":00").do(lambda: fetch_and_save("4h"))
schedule.every().day.at("23:59").do(lambda: fetch_and_save("1d"))

print("⏳ ETL scheduler started...")

# Continuous loop to run the scheduler
while True:
    schedule.run_pending()
    time.sleep(1)
