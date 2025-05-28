import pandas as pd
import os

# --- CONFIG ---
superbowl_csv_path = r"C:\Work_Git\DataEngineer\CAPSTONE\ingest\all_years_merged_with_stocks_with_youtube.csv"
stock_data_dir = r"C:\Work_Git\DataEngineer\CAPSTONE\stock_data"
missing_output_path = r"C:\Work_Git\DataEngineer\CAPSTONE\missing_stock_tickers.csv"
ticker_column_name = "stock_ticker"  # Adjust if your column is named differently

# --- LOAD SUPER BOWL TICKERS ---
try:
    df = pd.read_csv(superbowl_csv_path)
    superbowl_tickers = set(df[ticker_column_name].dropna().unique())
    print(f"📊 Found {len(superbowl_tickers)} unique tickers in Super Bowl data.")
except Exception as e:
    print(f"❌ Failed to read Super Bowl CSV: {e}")
    exit(1)

# --- CHECK FOR STOCK FILES ---
missing_tickers = []

for ticker in superbowl_tickers:
    file_path = os.path.join(stock_data_dir, f"{ticker}.csv")
    if not os.path.isfile(file_path):
        missing_tickers.append(ticker)

# --- REPORT ---
if missing_tickers:
    print(f"\n⚠️ Missing stock files for {len(missing_tickers)} tickers:")
    for t in missing_tickers:
        print(f" - {t}")
    # Save to CSV
    pd.DataFrame(missing_tickers, columns=["missing_ticker"]).to_csv(missing_output_path, index=False)
    print(f"\n💾 Missing tickers saved to: {missing_output_path}")
else:
    print("\n✅ All Super Bowl tickers have stock data.")
