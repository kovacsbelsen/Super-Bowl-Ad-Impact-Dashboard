import os
import time
import pandas as pd
from pathlib import Path
from polygon import RESTClient

API_KEY = "Yv_mB9YaD_ezGA5ibmudIOTXEzdrvXeG"
client = RESTClient(API_KEY)

# Replace with your list of target tickers (or load from file)
TICKERS = [
    "HYMTF", "BAMXF", "UL", "HYMLF", "KIMTF", "OTLY", "RKUNF", "TCEHY", "VLKAF", "PSNYW",
    "NSRGF", "ULVR", "MCARY", "LDSVF", "DANOY", "TM", "KWHIY", "DEO", "BUD", "LPL",
    "WW", "HENOF", "STBFY", "GRUB", "MHJ", "VWAGY", "BMWYY", "AZN", "NWFAX", "NSANF",
    "REMYF", "HLTHQ", "SONY", "HMC", "PDD", "NVS", "HINKF", "LRLCF", "NTDOY", "ZAGG", "MBGAF"
]

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

def update_fundamentals_csv(tickers, csv_path):
    existing_df = pd.read_csv(csv_path) if os.path.exists(csv_path) else pd.DataFrame()
    existing_tickers = set(existing_df["ticker"].dropna().unique())

    missing_tickers = [t for t in tickers if t not in existing_tickers]
    print(f"🕵️‍♂️ {len(missing_tickers)} tickers missing from metadata...")

    new_rows = []
    for i, ticker in enumerate(missing_tickers):
        print(f"🔄 ({i+1}/{len(missing_tickers)}) Fetching: {ticker}")
        info = fetch_ticker_overview(ticker)
        if info:
            print(f"✅ Retrieved: {info['ticker']}")
            new_rows.append(info)
        time.sleep(0.3)

    if new_rows:
        updated_df = pd.concat([existing_df, pd.DataFrame(new_rows)], ignore_index=True)
        updated_df.to_csv(csv_path, index=False)
        print(f"📁 Updated metadata saved to {csv_path}")
    else:
        print("✅ No new tickers to update.")

# -----------------------
# 🟢 Run the updater
# -----------------------
if __name__ == "__main__":
    metadata_file = Path("polygon_company_data") / "company_metadata.csv"
    update_fundamentals_csv(TICKERS, metadata_file)
