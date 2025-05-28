import os
import time
import pandas as pd
from pathlib import Path
from polygon import RESTClient

API_KEY = "Yv_mB9YaD_ezGA5ibmudIOTXEzdrvXeG"
client = RESTClient(API_KEY)

# List of tickers to check for and add if missing
TICKERS = [
    "HYMTF", "BAMXF", "UL", "HYMLF", "KIMTF", "OTLY", "RKUNF", "TCEHY", "VLKAF", "PSNYW",
    "NSRGF", "ULVR", "MCARY", "LDSVF", "DANOY", "TM", "KWHIY", "DEO", "BUD", "LPL",
    "WW", "HENOF", "STBFY", "GRUB", "MHJ", "VWAGY", "BMWYY", "AZN", "NWFAX", "NSANF",
    "REMYF", "HLTHQ", "SONY", "HMC", "PDD", "NVS", "HINKF", "LRLCF", "NTDOY", "ZAGG", "MBGAF"
]

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

def update_metadata_csv(tickers, output_path):
    # Load existing metadata
    if os.path.exists(output_path):
        existing_df = pd.read_csv(output_path)
        existing_tickers = set(existing_df['ticker'].dropna().unique())
    else:
        existing_df = pd.DataFrame()
        existing_tickers = set()

    # Filter missing tickers
    missing_tickers = [t for t in tickers if t not in existing_tickers]
    print(f"🕵️‍♂️ {len(missing_tickers)} tickers missing. Fetching...")

    new_rows = []
    for i, ticker in enumerate(missing_tickers):
        print(f"🔄 ({i+1}/{len(missing_tickers)}) Fetching: {ticker}")
        data = fetch_ticker_metadata(ticker)
        if data:
            print(f"✅ Added: {data['ticker']}")
            new_rows.append(data)
        time.sleep(0.3)

    if new_rows:
        updated_df = pd.concat([existing_df, pd.DataFrame(new_rows)], ignore_index=True)
        updated_df.to_csv(output_path, index=False)
        print(f"📁 Updated ticker metadata saved to: {output_path}")
    else:
        print("✅ No new tickers needed.")

# ----------------------
# 🟢 Run the script
# ----------------------
if __name__ == "__main__":
    csv_path = r"C:\Work_Git\DataEngineer\CAPSTONE\polygon_ticker_metadata\ticker_metadata.csv"
    update_metadata_csv(TICKERS, csv_path)
