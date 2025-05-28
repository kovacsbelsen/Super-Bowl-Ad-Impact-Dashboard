import pandas as pd
from polygon import RESTClient
from datetime import datetime
import time
import os

# --- CONFIG ---
API_KEY = "Yv_mB9YaD_ezGA5ibmudIOTXEzdrvXeG"
MISSING_TICKERS_FILE = r"C:\Work_Git\DataEngineer\CAPSTONE\missing_stock_tickers.csv"
OUTPUT_DIR = r"C:\Work_Git\DataEngineer\CAPSTONE\stock_data"
FROM_DATE = "2015-01-01"
TO_DATE = datetime.today().strftime("%Y-%m-%d")

# --- INIT ---
client = RESTClient(API_KEY)

def fetch_and_save_ticker_data(ticker, from_date=FROM_DATE, to_date=TO_DATE, output_dir=OUTPUT_DIR):
    try:
        print(f"📈 Fetching {ticker}...")
        aggs = client.get_aggs(ticker=ticker, multiplier=1, timespan="day", from_=from_date, to=to_date, limit=50000)

        if not aggs:
            print(f"⚠️ No data for {ticker}")
            return False

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
        file_path = os.path.join(output_dir, f"{ticker}.csv")
        df.to_csv(file_path, index=False)
        print(f"✅ Saved {file_path}")
        return True

    except Exception as e:
        print(f"❌ Failed for {ticker}: {e}")
        return False
    finally:
        time.sleep(0.25)  # Respect rate limits

# --- MAIN ---
if __name__ == "__main__":
    try:
        missing_df = pd.read_csv(MISSING_TICKERS_FILE)
        tickers = missing_df["missing_ticker"].dropna().unique()

        print(f"📋 Processing {len(tickers)} missing tickers...\n")

        for ticker in tickers:
            success = fetch_and_save_ticker_data(ticker)
            status = "✅ Success" if success else "❌ Failed"
            print(f"{status} - {ticker}\n")

    except Exception as e:
        print(f"❌ Error loading missing tickers file: {e}")
