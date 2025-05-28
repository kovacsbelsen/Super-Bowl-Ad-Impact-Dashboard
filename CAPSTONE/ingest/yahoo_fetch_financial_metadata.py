import os
import yfinance as yf
import pandas as pd
import time

# === Paths ===
input_dir = r"C:\Work_Git\DataEngineer\CAPSTONE\yahoofinance_stock_data"
output_dir = r"C:\Work_Git\DataEngineer\CAPSTONE\yahoofinance_financial_metadata"
os.makedirs(output_dir, exist_ok=True)

# === Function to fetch financials ===
def fetch_financial_data(ticker_symbol):
    try:
        ticker = yf.Ticker(ticker_symbol)
        metadata = ticker.info
        income = ticker.financials
        balance = ticker.balance_sheet
        cashflow = ticker.cashflow

        result = {
            "ticker": ticker_symbol,
            "name": metadata.get("longName"),
            "industry": metadata.get("industry"),
            "description": metadata.get("longBusinessSummary"),
            "employees": metadata.get("fullTimeEmployees"),
            "website": metadata.get("website"),
            "city": metadata.get("city"),
            "state": metadata.get("state"),
            "country": metadata.get("country"),
            "address": metadata.get("address1")
        }

        # Most recent Income Statement
        if not income.empty:
            latest = income.columns[0]
            for row in income.index:
                result[f"income_statement.{row}"] = income.loc[row, latest]

        # Most recent Balance Sheet
        if not balance.empty:
            latest = balance.columns[0]
            for row in balance.index:
                result[f"balance_sheet.{row}"] = balance.loc[row, latest]

        # Most recent Cash Flow
        if not cashflow.empty:
            latest = cashflow.columns[0]
            for row in cashflow.index:
                result[f"cash_flow_statement.{row}"] = cashflow.loc[row, latest]

        return result

    except Exception as e:
        print(f"Error fetching data for {ticker_symbol}: {e}")
        return None

# === Iterate through tickers ===
for file in os.listdir(input_dir):
    if file.endswith(".csv"):
        ticker = file.replace(".csv", "").strip().upper()
        print(f"Fetching financials for {ticker}...")

        data = fetch_financial_data(ticker)
        if data:
            df = pd.DataFrame([data])
            output_path = os.path.join(output_dir, f"{ticker}_financials.csv")
            df.to_csv(output_path, index=False)
            print(f"Saved to {output_path}")
        else:
            print(f"No data found for {ticker}.")

        # Optional: to avoid throttling
        time.sleep(1)
