import os
import time
import pandas as pd
from pathlib import Path
from polygon import RESTClient

API_KEY = "Yv_mB9YaD_ezGA5ibmudIOTXEzdrvXeG"  # Replace with your real API key
client = RESTClient(API_KEY)


def fetch_financials_for_ticker(ticker, years=10):
    results = []
    current_year = pd.Timestamp.now().year

    try:
        for f in client.vx.list_stock_financials(ticker=ticker, sort="filing_date", order="desc", limit=100):
            fy = f.fiscal_year
            if fy and int(fy) < current_year - years:
                break  # skip if older than 10 years

            data = {
                "ticker": ticker,
                "filing_date": getattr(f, "filing_date", None),
                "fiscal_year": fy,
                "fiscal_period": getattr(f, "fiscal_period", None),
                "start_date": getattr(f, "start_date", None),
                "end_date": getattr(f, "end_date", None),
                "source_filing_url": getattr(f, "source_filing_url", None),
            }

            # Direct attribute access from f.financials.*
            for section_name in ["income_statement", "balance_sheet", "cash_flow_statement", "comprehensive_income"]:
                section = getattr(f.financials, section_name, None)
                if section:
                    for key, val in vars(section).items():
                        data[f"{section_name}.{key}"] = getattr(val, "value", None)

            results.append(data)

    except Exception as e:
        print(f"❌ Error fetching financials for {ticker}: {e}")
    return pd.DataFrame(results)


def save_all_financials(tickers, output_dir="polygon_financials", years=10):
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)

    for i, ticker in enumerate(tickers):
        print(f"🔄 ({i+1}/{len(tickers)}) Processing: {ticker}")
        df = fetch_financials_for_ticker(ticker, years=years)
        if not df.empty:
            out_path = output_path / f"{ticker}.csv"
            df.to_csv(out_path, index=False)
            print(f"✅ Saved: {out_path}")
        else:
            print(f"⚠️ No financials found for {ticker}")
        time.sleep(0.5)


# 🟢 ENTRY POINT
if __name__ == "__main__":
    # Optional: load tickers from a previously saved metadata file
    metadata_file = Path("polygon_company_data") / "company_metadata.csv"
    if metadata_file.exists():
        df_meta = pd.read_csv(metadata_file)
        tickers = df_meta["ticker"].dropna().unique().tolist()
    else:
        # Or fallback: use top N tickers for testing
        tickers = ["AAPL", "MSFT", "GOOGL", "AMZN"]

    save_all_financials(tickers, output_dir="polygon_financials", years=10)
