
import yfinance as yf
import requests
import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, max as sf_max
from datetime import datetime, timedelta

# Snowflake connection parameters
connection_parameters = {
    "account": 'aab46027',
    "user": 'dataexpert_student',
    "password": 'DataExpert123!',
    "role": "all_users_role",
    "warehouse": 'COMPUTE_WH',
    "database": 'dataexpert_student',
    "schema": 'BENCEKOVACS'
}

# Initialize Snowflake session
session = Session.builder.configs(connection_parameters).create()

# Fetch distinct tickers from ads table
tickers_df = session.sql("""
    SELECT DISTINCT ticker
    FROM BENCEKOVACS.superbowl_ads_cleaned
    WHERE ticker IS NOT NULL
""").to_pandas()

tickers = tickers_df['TICKER'].tolist()

# === YFinance Update ===
def update_yfinance_data():
    for ticker in tickers:
        # Get latest date in Snowflake
        latest_date_result = session.sql(f"""
            SELECT MAX(date) AS max_date
            FROM BENCEKOVACS.yahoo_stock_data_cleaned
            WHERE ticker = '{ticker}'
        """).to_pandas()

        start_date = latest_date_result['MAX_DATE'].iloc[0]
        if pd.isna(start_date):
            start_date = "2015-01-01"
        else:
            start_date = (pd.to_datetime(start_date) + timedelta(days=1)).strftime('%Y-%m-%d')

        end_date = datetime.today().strftime('%Y-%m-%d')

        # Fetch and upload new data
        data = yf.download(ticker, start=start_date, end=end_date)
        if data.empty:
            continue
        data.reset_index(inplace=True)
        data['Ticker'] = ticker
        df_upload = data[['Date', 'Close', 'Ticker']].rename(columns={'Date': 'date', 'Close': 'close', 'Ticker': 'ticker'})
        session.write_pandas(df_upload, "yahoo_stock_data_cleaned", auto_create_table=False, overwrite=False)

# === Polygon Update ===
def fetch_polygon_data(ticker, start_date):
    API_KEY = "<your_polygon_api_key>"
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{datetime.today().strftime('%Y-%m-%d')}?adjusted=true&sort=asc&apiKey={API_KEY}"
    response = requests.get(url)
    if response.status_code != 200:
        return pd.DataFrame()
    results = response.json().get("results", [])
    if not results:
        return pd.DataFrame()
    df = pd.DataFrame(results)
    df['date'] = pd.to_datetime(df['t'], unit='ms').dt.date
    df['close'] = df['c']
    df['ticker'] = ticker
    return df[['date', 'close', 'ticker']]

def update_polygon_data():
    for ticker in tickers:
        latest_date_result = session.sql(f"""
            SELECT MAX(date) AS max_date
            FROM BENCEKOVACS.polygon_stock_data_cleaned
            WHERE ticker = '{ticker}'
        """).to_pandas()

        start_date = latest_date_result['MAX_DATE'].iloc[0]
        if pd.isna(start_date):
            start_date = "2015-01-01"
        else:
            start_date = (pd.to_datetime(start_date) + timedelta(days=1)).strftime('%Y-%m-%d')

        df = fetch_polygon_data(ticker, start_date)
        if not df.empty:
            session.write_pandas(df, "polygon_stock_data_cleaned", auto_create_table=False, overwrite=False)

# Run the update jobs
update_yfinance_data()
update_polygon_data()
print("✅ Data pipeline completed.")
