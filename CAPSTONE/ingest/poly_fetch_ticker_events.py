import os
import pandas as pd
from polygon import RESTClient
from pathlib import Path
import time

API_KEY = "Yv_mB9YaD_ezGA5ibmudIOTXEzdrvXeG"
client = RESTClient(API_KEY)

stock_data_dir = Path("C:/Work_Git/DataEngineer/CAPSTONE/stock_data")
tickers = [f.stem for f in stock_data_dir.glob("*.csv")]

def fetch_ticker_events(ticker):
    try:
        response = client.get_ticker_events(ticker)

        name = getattr(response, "name", "")
        composite_figi = getattr(response, "composite_figi", "")
        cik = getattr(response, "cik", "")
        events = getattr(response, "events", [])

        event_rows = []
        for event in events:
            event_type = event.get("type")
            event_date = event.get("date")
            ticker_change = event.get("ticker_change", {}).get("ticker", "")
            event_rows.append({
                "ticker": ticker,
                "event_type": event_type,
                "event_date": event_date,
                "new_ticker": ticker_change,
                "name": name,
                "composite_figi": composite_figi,
                "cik": cik,
            })

        return event_rows

    except Exception as e:
        return [{"ticker": ticker, "error": str(e)}]


# Collect all event data
all_events = []
for i, ticker in enumerate(tickers):
    print(f"🔄 ({i+1}/{len(tickers)}) Fetching events for: {ticker}")
    events = fetch_ticker_events(ticker)
    all_events.extend(events)
    time.sleep(0.3)

# Save to CSV
events_df = pd.DataFrame(all_events)
output_file = "C:/Work_Git/DataEngineer/CAPSTONE/polygon_ticker_events.csv"
events_df.to_csv(output_file, index=False)
print(f"✅ Events saved to {output_file}")
