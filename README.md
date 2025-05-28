
# 🏈 [Super Bowl Ad Impact Dashboard](https://app.snowflake.com/lywbbpj/odb66944/#/streamlit-apps/DATAEXPERT_STUDENT.BENCEKOVACS.EL98HTPA_K2BNB0U?ref=snowsight_shared)
_A Capstone Project for Data Engineering_

---

## 📖 Introduction

This project investigates how **Super Bowl advertisements** influence **public company stock performance** and **audience engagement metrics**. Using a **multi-source data pipeline**—spanning APIs, scraping, and Snowflake warehousing—we deliver an **interactive dashboard** that tracks trends, identifies patterns, and visualizes the intersection between **marketing spend and financial impact**.

---

## 🌍 Data Sources & Ingestion Strategy

| Source                   | Type        | Method              | Data Includes                                      |
|--------------------------|-------------|---------------------|----------------------------------------------------|
| **YouTube**              | Dynamic API | YouTube Data API    | View count, likes, comments, upload dates          |
| **Yahoo Finance**        | API         | yfinance wrapper    | Historical stock prices, metadata, financials      |
| **Polygon.io**           | API         | REST (Python)       | Public company stock history and metrics           |
| **Super Bowl Metadata**  | Custom Scrape | BeautifulSoup      | Titles, companies, brands, links (2005–2024)       |
| **Manual CSV Uploads**   | Static      | Snowflake Stage     | Backup for validation and consistency checks       |

- ✅ APIs were used for all live metrics.  
- ✅ Web scraping was implemented to build an initial dataset of Super Bowl ads.  
- ✅ Final datasets were stored and processed in **Snowflake**, with transformations written in **SQL** and **Python**.

---

## 🛠️ Technical Stack

| Tool         | Role                                       |
|--------------|--------------------------------------------|
| **Snowflake** | Data warehouse, staging & transformation   |
| **Python**    | Scraping, API ingestion, ETL scripts       |
| **Streamlit** | Dashboard frontend                         |
| **Plotly**    | Interactive charts                         |
| **GitHub**    | Version control and final submission       |

---

## 📈 Dashboard Features

### Tab 1: 📊 Ticker Details
- Select any ticker that has participated in Super Bowl advertising
- Interactive charts:
  - 📉 Stock prices (Polygon & Yahoo)
  - 📺 YouTube ad views
  - 🔁 Related tickers performance
- Financial and business metadata
- Ad engagement metrics (views, likes, comments)

### Tab 2: 📈 Global Trends
- Yearly ad trends  
- Companies with highest ad frequency  
- Industries with most Super Bowl investment  
- 📊 Public vs Private company participation  
- Complete table of all Super Bowl ads  

---

## 🧪 Data Engineering Steps

### 1. **Scraping & API Integration**
- Scraped and enriched 10 years of Super Bowl ad metadata (title, brand, company, year)
- Pulled ad video metrics from the YouTube API where available
- Fetched stock price and company metadata from Yahoo and Polygon APIs

### 2. **Staging & Cleaning**
- Loaded raw data into Snowflake
- Applied transformations to normalize schemas
- Casted types, removed duplicates, handled NULLs
- Joined across sources using `ticker`

### 3. **Modeling & Serving**
- Designed a **star schema** with:
  - `superbowl_ads_cleaned` as fact table
  - Financial metadata, stock prices, and industry dimensions
- Deployed analytical dashboard using Streamlit

### 4. **Quality Checks**
- Referential integrity on ticker joins  
- Yearly ad count sanity checks  
- NULL checks and value ranges for numerical columns
- Handle all NaN values  

---

## 🗃️ Key Tables

- `superbowl_ads_cleaned`  
- `yahoo_stock_data_cleaned`  
- `polygon_stock_data_cleaned`  
- `yahoo_financial_metadata_cleaned`  
- `polygon_metadata_cleaned`  
- `youtube_ad_metrics_cleaned`  
- `yahoo_related_tickers_cleaned`  

---

## 📤 Project Summary

| Metric                     | Value         |
|---------------------------|---------------|
| Total Ads Analyzed        | 670+          |
| Unique Tickers            | 700+          |
| Distinct Industries       | 20+           |
| YouTube Views Aggregated  | 1B+           |
| Snowflake Tables          | 7             |
| Dashboard Pages           | 2 (Ticker & Global Trends) |

---

## 📆 Future Enhancements

- 🚀 Daily ETL pipeline for automated YouTube + stock data updates  
- 📬 Email/Slack alert for abnormal engagement spikes  
- 🧠 Sentiment analysis on YouTube comments  
- 🗃️ Archive historical versions using time-travel in Snowflake  
- 📊 Export API for downstream analytics teams  
