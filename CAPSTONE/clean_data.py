import pandas as pd
import numpy as np
import os

# Utility functions
def clean_dataframe(df: pd.DataFrame, date_columns=None, numeric_columns=None) -> pd.DataFrame:
    # Normalize column names
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

    # Strip strings and replace empty strings / common null indicators with np.nan
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    df.replace(
        to_replace=["", "null", "NULL", "na", "NA", "N/A", "n/a", "-", "--"],
        value=np.nan,
        inplace=True
    )

    # Convert date columns
    if date_columns:
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce', utc=True).dt.tz_convert(None)
                df[col] = df[col].dt.tz_localize(None)

    # Convert numeric columns
    if numeric_columns:
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

    return df

# Set file paths accordingly
file_paths = {
    "polygon_metadata": "C:\Work_Git\DataEngineer\CAPSTONE\data\polygon_company_metadata.csv",
    "polygon_financials": "C:\Work_Git\DataEngineer\CAPSTONE\data\polygon_financials_merged_data.csv",
    "polygon_ticker_metadata": "C:\Work_Git\DataEngineer\CAPSTONE\data\polygon_ticker_metadata.csv",
    "polygon_stock_data": "C:\Work_Git\DataEngineer\CAPSTONE\data\polygon_stock_merged_data.csv",
    "yahoo_financial_metadata": "C:\Work_Git\DataEngineer\CAPSTONE\data\yahoofinance_financial_metadata_merged_data.csv",
    "yahoo_stock_data": "C:\Work_Git\DataEngineer\CAPSTONE\data\yahoofinance_stock_merged_data.csv",
    "yahoo_related_tickers": "C:\Work_Git\DataEngineer\CAPSTONE\data\yahoofinance_related_merged_data.csv",
    "superbowl_ads": "C:\Work_Git\DataEngineer\CAPSTONE\data\superbowl_data_all_years_merged_with_stocks_with_youtube.csv"
}

# Define cleaning configs for each file
cleaning_configs = {
    "polygon_metadata": {
        "date_columns": ["list_date"],
        "numeric_columns": ["market_cap", "employees"]
    },
    "polygon_financials": {
        "date_columns": ["filing_date", "start_date", "end_date"],
        "numeric_columns": None  # We'll auto-detect below
    },
    "polygon_ticker_metadata": {
        "date_columns": ["list_date"],
        "numeric_columns": ["cik"]
    },
    "polygon_stock_data": {
        "date_columns": ["date"],
        "numeric_columns": ["open", "high", "low", "close", "volume"]
    },
    "yahoo_financial_metadata": {
        "date_columns": None,
        "numeric_columns": None  # Will infer
    },
    "yahoo_stock_data": {
        "date_columns": ["date"],
        "numeric_columns": ["open", "high", "low", "close", "volume", "dividends", "stock_splits"]
    },
    "yahoo_related_tickers": {
        "date_columns": None,
        "numeric_columns": None
    },
    "superbowl_ads": {
        "date_columns": ["publishedat"],
        "numeric_columns": ["year", "viewcount", "likecount", "commentcount", "dislikecount"]
    }
}

# Apply cleaning
output_dir = "C:\Work_Git\DataEngineer\CAPSTONE\data\cleaned_csvs"
os.makedirs(output_dir, exist_ok=True)
cleaned_files = []

for name, path in file_paths.items():
    try:
        df = pd.read_csv(path)
        config = cleaning_configs[name]

        # Auto-detect numeric columns if not defined
        if config["numeric_columns"] is None:
            config["numeric_columns"] = df.select_dtypes(include=[np.number]).columns.tolist()

        cleaned_df = clean_dataframe(df, config["date_columns"], config["numeric_columns"])

        out_path = os.path.join(output_dir, f"{name}_cleaned.csv")
        cleaned_df.to_csv(out_path, index=False)
        cleaned_files.append(out_path)
    except Exception as e:
        cleaned_files.append(f"{name} FAILED: {str(e)}")

