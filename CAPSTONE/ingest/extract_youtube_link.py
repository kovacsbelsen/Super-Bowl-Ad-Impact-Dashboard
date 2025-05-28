import requests
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path
import time
import json

HEADERS = {"User-Agent": "Mozilla/5.0"}
BASE_PATH = Path("superbowl_ads")

from bs4 import BeautifulSoup

def extract_youtube_from_iframe(ad_url):
    try:
        response = requests.get(ad_url, headers=HEADERS, timeout=10)
        if response.status_code != 200:
            print(f"❌ Failed to fetch page: {ad_url}")
            return None
        
        soup = BeautifulSoup(response.content, "html.parser")
        iframe = soup.find("iframe", src=lambda x: x and "youtube.com/embed/" in x)
        
        if iframe:
            embed_url = iframe['src']
            print(f"🎯 iframe found: {embed_url}")
            return f"{embed_url}"
        else:
            print(f"⚠️ No iframe found on page: {ad_url}")
    except Exception as e:
        print(f"❌ Exception while processing {ad_url}: {e}")
    return None



def enhance_year_with_youtube_links(year):
    # Load ads for the year
    file_path = BASE_PATH / f"{year}.json"
    with open(file_path, "r") as f:
        ads = json.load(f)

    df = pd.DataFrame(ads)
    df["youtube_link"] = None

    for idx, row in df.iterrows():
        print(f"Processing {year} ad {idx+1}/{len(df)}: {row['title']}")
        youtube_url = extract_youtube_from_iframe(row["link"])
        df.at[idx, "youtube_link"] = youtube_url
        time.sleep(1)  # Be polite to server

    # Save to enhanced CSV or JSON
    df.to_csv(BASE_PATH / f"{year}_enhanced.csv", index=False)
    print(f"✅ Finished processing {year} and saved to {year}_enhanced.csv")

# Example usage
# Loop through each year from 2015 to 2025
for year in range(2015, 2026):
    print(f"\n📅 Starting year: {year}")
    try:
        enhance_year_with_youtube_links(year)
    except Exception as e:
        print(f"❌ Error processing {year}: {e}")

