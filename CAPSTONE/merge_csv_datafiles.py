import os
import pandas as pd

def merge_csvs_with_ticker_column(input_folder, output_file):
    """
    Merges all CSV files in the input folder into a single CSV.
    Adds a 'ticker' column based on the filename if it's missing.

    Parameters:
        input_folder (str): Path to the folder with CSV files.
        output_file (str): Full path to the output CSV file.
    """
    all_dfs = []
    
    for filename in os.listdir(input_folder):
        if filename.endswith(".csv"):
            file_path = os.path.join(input_folder, filename)
            try:
                df = pd.read_csv(file_path)

                # Extract ticker from filename (remove .csv)
                ticker_name = os.path.splitext(filename)[0]

                # If 'ticker' column not in df, add it
                if 'ticker' not in df.columns:
                    df['ticker'] = ticker_name

                all_dfs.append(df)
            except Exception as e:
                print(f"Failed to process {filename}: {e}")

    # Concatenate all DataFrames and save
    if all_dfs:
        merged_df = pd.concat(all_dfs, ignore_index=True)
        merged_df.to_csv(output_file, index=False)
        print(f"Merged CSV saved to: {output_file}")
    else:
        print("No valid CSV files found to merge.")


merge_csvs_with_ticker_column("C:\Work_Git\DataEngineer\CAPSTONE\polygon_financials", "C:\Work_Git\DataEngineer\CAPSTONE\data\polygon_financials_merged_data.csv")

merge_csvs_with_ticker_column("C:\Work_Git\DataEngineer\CAPSTONE\polygon_stock_data", "C:\Work_Git\DataEngineer\CAPSTONE\data\polygon_stock_merged_data.csv")

merge_csvs_with_ticker_column("C:\Work_Git\DataEngineer\CAPSTONE\yahoofinance_financial_metadata", "C:\Work_Git\DataEngineer\CAPSTONE\data\yahoofinance_financial_metadata_merged_data.csv")

merge_csvs_with_ticker_column("C:\Work_Git\DataEngineer\CAPSTONE\yahoofinance_stock_data", "C:\Work_Git\DataEngineer\CAPSTONE\data\yahoofinance_stock_merged_data.csv")

merge_csvs_with_ticker_column("C:\Work_Git\DataEngineer\CAPSTONE\yahoofinance_related_tickers", "C:\Work_Git\DataEngineer\CAPSTONE\data\yahoofinance_related_merged_data.csv")