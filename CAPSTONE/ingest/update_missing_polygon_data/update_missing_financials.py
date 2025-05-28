import os
import time
import pandas as pd
from pathlib import Path
from polygon import RESTClient

API_KEY = "Yv_mB9YaD_ezGA5ibmudIOTXEzdrvXeG"
client = RESTClient(API_KEY)

TICKERS = list(set([
    "HYMTF", "BAMXF", "UL", "HYMLF", "KIMTF", "OTLY", "RKUNF", "TCEHY", "VLKAF", "PSNYW",
    "NSRGF", "ULVR", "MCARY", "LDSVF", "DANOY", "TM", "KWHIY", "DEO", "BUD", "LPL",
    "WW", "HENOF", "STBFY", "GRUB", "MHJ", "VWAGY", "BMWYY", "AZN", "NWFAX", "NSANF",
    "REMYF", "HLTHQ", "SONY", "HMC", "PDD", "NVS", "HINKF", "LRLCF", "NTDOY", "ZAGG", "MBGAF"
]))

def fetch_financials_for_ticker(ticker, years=10):
    results = []
    current_year = pd.Timestamp.now().year

    try:
        for f in client.vx.list_stock_financials(ticker=ticker, sort="filing_date", order="desc", limit=100):
            fy = f.fiscal_year
            if fy and int(fy) < current_year - years:
                break

            data = {
                "ticker": ticker,
                "filing_date": getattr(f, "filing_date", None),
                "fiscal_year": fy,
                "fiscal_period": getattr(f, "fiscal_period", None),
                "start_date": getattr(f, "start_date", None),
                "end_date": getattr(f, "end_date", None),
                "source_filing_url": getattr(f, "source_filing_url", None),
            }

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


# 🟢 Run this block
if __name__ == "__main__":
    save_all_financials(TICKERS, output_dir="polygon_financials", years=10)
