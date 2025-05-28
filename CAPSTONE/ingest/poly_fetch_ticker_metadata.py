import os
import time
import pandas as pd
from pathlib import Path
from polygon import RESTClient

API_KEY = "Yv_mB9YaD_ezGA5ibmudIOTXEzdrvXeG"  # Replace with your real API key
client = RESTClient(API_KEY)

def get_all_common_stock_tickers():
    tickers = []
    print("📥 Fetching all active US common stock tickers...")
    try:
        for result in client.list_tickers(market="stocks", active=True, limit=1000):
            if result.type == "CS":
                tickers.append(result.ticker)
        print(f"✅ Retrieved {len(tickers)} tickers.")
    except Exception as e:
        print(f"❌ Error fetching tickers: {e}")
    return tickers

def fetch_ticker_metadata(ticker):
    try:
        details = client.get_ticker_details(ticker)
        return {
            "ticker": details.ticker,
            "name": details.name,
            "type": details.type,
            "primary_exchange": details.primary_exchange,
            "active": details.active,
            "market": details.market,
            "locale": details.locale,
            "list_date": details.list_date,
            "cik": details.cik,
            "composite_figi": details.composite_figi,
            "share_class_figi": details.share_class_figi,
            "homepage_url": details.homepage_url,
            "logo_url": details.branding.logo_url if details.branding else None,
            "icon_url": details.branding.icon_url if details.branding else None,
        }
    except Exception as e:
        print(f"⚠️ Failed for {ticker}: {e}")
        return None

def save_metadata_to_csv(tickers, output_path):
    records = []
    for i, ticker in enumerate(tickers):
        print(f"🔄 ({i+1}/{len(tickers)}) Processing: {ticker}")
        data = fetch_ticker_metadata(ticker)
        if data:
            records.append(data)
        time.sleep(0.3)
    df = pd.DataFrame(records)
    df.to_csv(output_path, index=False)
    print(f"✅ Metadata saved to {output_path}")

if __name__ == "__main__":
    output_dir = Path("polygon_ticker_metadata")
    output_dir.mkdir(exist_ok=True)
    all_tickers = get_all_common_stock_tickers()
    save_metadata_to_csv(all_tickers, output_dir / "ticker_metadata.csv")
