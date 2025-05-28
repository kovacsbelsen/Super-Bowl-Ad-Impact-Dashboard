import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

# Directories
input_dir = r"C:\Work_Git\DataEngineer\CAPSTONE\yahoofinance_stock_data"
output_dir = r"C:\Work_Git\DataEngineer\CAPSTONE\yahoofinance_related_stickers"

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

# Headers to mimic a browser visit
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
}

# Iterate over each CSV file in the input directory
for filename in os.listdir(input_dir):
    if filename.endswith(".csv"):
        ticker = filename.replace(".csv", "")
        url = f"https://finance.yahoo.com/quote/{ticker}/history/"

        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            # Find the 'Related Tickers' section
            related_section = soup.find("h3", string="Related Tickers")
            if related_section:
                section = related_section.find_parent("section")
                if section:
                    links = section.find_all("a", class_="card-link")
                    related_tickers = []
                    for link in links:
                        title = link.get("title", "").strip()
                        if title:
                            related_tickers.append(title)

                    # Save to CSV
                    if related_tickers:
                        df = pd.DataFrame(related_tickers, columns=["Related Ticker"])
                        output_path = os.path.join(output_dir, f"{ticker}_related.csv")
                        df.to_csv(output_path, index=False)
                        print(f"Saved related tickers for {ticker} to {output_path}")
                    else:
                        print(f"No related tickers found for {ticker}")
                else:
                    print(f"'Related Tickers' section not found for {ticker}")
            else:
                print(f"'Related Tickers' header not found for {ticker}")

        except requests.RequestException as e:
            print(f"Error fetching data for {ticker}: {e}")

        # Respectful delay to avoid overwhelming the server
        time.sleep(1)
