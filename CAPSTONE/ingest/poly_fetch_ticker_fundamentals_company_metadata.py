import os
import pandas as pd
from polygon import RESTClient
import time
from pathlib import Path


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

def fetch_ticker_overview(ticker):
    try:
        detail = client.get_ticker_details(ticker)
        return {
            "ticker": detail.ticker,
            "name": detail.name,
            "market_cap": detail.market_cap,
            "industry": getattr(detail, "sic_description", None),
            "description": getattr(detail, "description", None),
            "employees": getattr(detail, "total_employees", None),
            "list_date": getattr(detail, "list_date", None),
            "exchange": getattr(detail, "primary_exchange", None),
            "website": getattr(detail, "homepage_url", None),
            "address": getattr(detail.address, "address1", None) if detail.address else None,
            "city": getattr(detail.address, "city", None) if detail.address else None,
            "state": getattr(detail.address, "state", None) if detail.address else None,
            "logo_url": getattr(detail.branding, "logo_url", None) if detail.branding else None,
        }
    except Exception as e:
        print(f"⚠️ Error for {ticker}: {e}")
        return None

def save_fundamentals_to_csv(tickers, output_file="company_metadata.csv"):
    results = []
    for i, ticker in enumerate(tickers):
        print(f"🔄 ({i+1}/{len(tickers)}) Fetching: {ticker}")
        info = fetch_ticker_overview(ticker)
        if info:
            print(info)
            results.append(info)
        time.sleep(0.3)  # Be respectful to rate limits
    df = pd.DataFrame(results)
    df.to_csv(output_file, index=False)
    print(f"📁 Saved company metadata to {output_file}")

if __name__ == "__main__":
    output_folder = Path("polygon_company_data")
    output_folder.mkdir(exist_ok=True)
    all_tickers = get_all_common_stock_tickers()
    save_fundamentals_to_csv(all_tickers, output_file=output_folder / "company_metadata.csv")