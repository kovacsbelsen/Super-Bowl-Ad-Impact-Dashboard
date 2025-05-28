import requests
from bs4 import BeautifulSoup
import time
import json
from urllib.parse import urljoin
from pathlib import Path

BASE_URL = "https://www.superbowl-ads.com"
HEADERS = {"User-Agent": "Mozilla/5.0"}

def extract_ads_from_page(url):
    response = requests.get(url, headers=HEADERS)
    if response.status_code != 200:
        return [], None

    soup = BeautifulSoup(response.content, "html.parser")
    ad_elements = soup.select("h3.cactus-post-title.entry-title.h4 > a")
    ads = [{"title": a.get_text(strip=True), "link": a['href']} for a in ad_elements]

    # Check for next page
    next_link = soup.select_one("div.nav-next a")
    next_page_url = urljoin(BASE_URL, next_link['href']) if next_link else None
    return ads, next_page_url

def scrape_ads_for_year(year):
    print(f"Scraping ads for {year}...")
    all_ads = []
    page_url = f"{BASE_URL}/category/video/{year}_ads/"

    while page_url:
        ads, next_page = extract_ads_from_page(page_url)
        all_ads.extend(ads)
        page_url = next_page
        time.sleep(1)  # Be polite to the server

    return all_ads

# Scrape all years and save
output_dir = Path("superbowl_ads")
output_dir.mkdir(exist_ok=True)

for year in range(2015, 2026):
    ads = scrape_ads_for_year(year)
    with open(output_dir / f"{year}.json", "w") as f:
        json.dump(ads, f, indent=2)
