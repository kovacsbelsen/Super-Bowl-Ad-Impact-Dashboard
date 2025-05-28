import pandas as pd
import yfinance as yf
import os

# Paths
input_csv_path = r"C:\Work_Git\DataEngineer\CAPSTONE\ingest\all_years_merged_with_stocks_with_youtube.csv"
output_dir = r"C:\Work_Git\DataEngineer\CAPSTONE\yahoofinance_stock_data"

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

# Read main dataset
df = pd.read_csv(input_csv_path)

# Extract unique tickers
tickers = df['stock_ticker'].dropna().unique()

print(f"Found {len(tickers)} unique tickers.")

# Download and save stock history
for ticker in tickers:
    try:
        print(f"Fetching: {ticker}")
        hist = yf.Ticker(ticker).history(period="max")  # You can adjust to '1y', '5y', etc.
        if not hist.empty:
            output_path = os.path.join(output_dir, f"{ticker}.csv")
            hist.to_csv(output_path)
            print(f"Saved: {output_path}")
        else:
            print(f"No data found for: {ticker}")
    except Exception as e:
        print(f"Error fetching {ticker}: {e}")
