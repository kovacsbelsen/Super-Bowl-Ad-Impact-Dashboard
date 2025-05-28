from polygon import RESTClient
import pandas as pd
from datetime import datetime
import time
import os
client = RESTClient("Yv_mB9YaD_ezGA5ibmudIOTXEzdrvXeG")


def get_all_stock_tickers():
    tickers = []
    print("📥 Fetching all active US tickers...")
    try:
        for result in client.list_tickers(market="stocks", active=True, limit=1000):
            if result.type == "CS":  # Only common stocks
                tickers.append(result.ticker)
        print(f"✅ Found {len(tickers)} tickers.")
    except Exception as e:
        print(f"❌ Error fetching tickers: {e}")
    return tickers

def fetch_and_save_ticker_data(ticker, from_date="2015-01-01", to_date=None, output_dir="stock_data"):
    to_date = to_date or datetime.today().strftime("%Y-%m-%d")
    try:
        print(f"📈 Fetching {ticker}...")
        aggs = client.get_aggs(ticker=ticker, multiplier=1, timespan="day", from_=from_date, to=to_date, limit=50000)
        if not aggs:
            print(f"⚠️ No data for {ticker}")
            return

        data = [{
            "date": pd.to_datetime(a.timestamp, unit='ms'),
            "open": a.open,
            "high": a.high,
            "low": a.low,
            "close": a.close,
            "volume": a.volume
        } for a in aggs]

        df = pd.DataFrame(data)
        os.makedirs(output_dir, exist_ok=True)
        df.to_csv(f"{output_dir}/{ticker}.csv", index=False)
        print(f"✅ Saved {ticker}.csv")
    except Exception as e:
        print(f"❌ Failed for {ticker}: {e}")
    time.sleep(0.25)  # Respectful delay

# ---------------------------
# Run the batch download
# ---------------------------
if __name__ == "__main__":
    tickers = get_all_stock_tickers()
    for ticker in tickers:
        fetch_and_save_ticker_data(ticker)