import pandas as pd
import os

def load_matching_stock_data(main_csv_path, stock_data_folder):
    # Load main CSV with ticker info
    main_df = pd.read_csv(main_csv_path)
    
    # Get unique stock tickers (remove NaN and strip spaces if needed)
    tickers = main_df['stock_ticker'].dropna().unique()
    tickers = [str(ticker).strip().upper() for ticker in tickers]

    # Initialize list to collect matching stock data
    all_stock_data = []

    # Iterate through tickers and find matching CSVs
    for ticker in tickers:
        file_path = os.path.join(stock_data_folder, f"{ticker}.csv")
        if os.path.exists(file_path):
            stock_df = pd.read_csv(file_path)
            stock_df['ticker'] = ticker  # Optional: tag source
            all_stock_data.append(stock_df)
        else:
            print(f"⚠️ No CSV found for ticker: {ticker}")

    # Combine all into one DataFrame
    if all_stock_data:
        combined_df = pd.concat(all_stock_data, ignore_index=True)
        print(f"\n✅ Total rows loaded: {len(combined_df)}")
    else:
        print("\n⚠️ No matching stock files found.")
        combined_df = pd.DataFrame()

    return combined_df

# Example usage
main_csv = r"C:\Work_Git\DataEngineer\CAPSTONE\ingest\all_years_merged_with_stocks_with_youtube.csv"
stock_data_dir = r"C:\Work_Git\DataEngineer\CAPSTONE\stock_data"

combined_stock_data = load_matching_stock_data(main_csv, stock_data_dir)
