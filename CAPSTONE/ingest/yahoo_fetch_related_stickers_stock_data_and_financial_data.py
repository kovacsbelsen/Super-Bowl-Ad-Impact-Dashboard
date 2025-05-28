import os
import pandas as pd
import yfinance as yf
import time

# === Input and Output Paths ===
related_tickers_dir = r"C:\Work_Git\DataEngineer\CAPSTONE\yahoofinance_related_stickers"
stock_data_dir = r"C:\Work_Git\DataEngineer\CAPSTONE\yahoofinance_stock_data"
financials_dir = r"C:\Work_Git\DataEngineer\CAPSTONE\yahoofinance_financial_metadata"

# Ensure output directories exist
os.makedirs(stock_data_dir, exist_ok=True)
os.makedirs(financials_dir, exist_ok=True)

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

        if not income.empty:
            latest = income.columns[0]
            for row in income.index:
                result[f"income_statement.{row}"] = income.loc[row, latest]

        if not balance.empty:
            latest = balance.columns[0]
            for row in balance.index:
                result[f"balance_sheet.{row}"] = balance.loc[row, latest]

        if not cashflow.empty:
            latest = cashflow.columns[0]
            for row in cashflow.index:
                result[f"cash_flow_statement.{row}"] = cashflow.loc[row, latest]

        return result

    except Exception as e:
        print(f"Error fetching data for {ticker_symbol}: {e}")
        return None

# === Main Loop: Iterate through all CSVs and fetch data ===
for file in os.listdir(related_tickers_dir):
    if file.endswith(".csv"):
        file_path = os.path.join(related_tickers_dir, file)
        print(f"\n📂 Processing related tickers from: {file}")

        try:
            df = pd.read_csv(file_path)
            tickers = df['Related Ticker'].dropna().unique()

            for ticker in tickers:
                ticker = ticker.strip().upper()
                if not ticker.isalpha():
                    continue  # skip invalid tickers

                print(f"\n🔄 Processing ticker: {ticker}")

                # === Save Stock History ===
                try:
                    hist = yf.Ticker(ticker).history(period="max")
                    if not hist.empty:
                        stock_output = os.path.join(stock_data_dir, f"{ticker}.csv")
                        hist.to_csv(stock_output)
                        print(f"✅ Stock data saved: {stock_output}")
                    else:
                        print(f"⚠️ No stock history found for: {ticker}")
                except Exception as e:
                    print(f"❌ Error fetching stock history for {ticker}: {e}")

                # === Save Financial Metadata ===
                try:
                    data = fetch_financial_data(ticker)
                    if data:
                        df_fin = pd.DataFrame([data])
                        financial_output = os.path.join(financials_dir, f"{ticker}_financials.csv")
                        df_fin.to_csv(financial_output, index=False)
                        print(f"✅ Financials saved: {financial_output}")
                    else:
                        print(f"⚠️ No financial metadata found for: {ticker}")
                except Exception as e:
                    print(f"❌ Error fetching financials for {ticker}: {e}")

                time.sleep(1)  # Optional delay to avoid rate limits

        except Exception as e:
            print(f"❌ Error reading file {file_path}: {e}")
