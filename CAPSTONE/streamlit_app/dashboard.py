import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from snowflake_connector import run_query

@st.cache_data
def get_ads_per_year():
    return run_query("""
        SELECT "year", COUNT(*) AS ad_count
        FROM "BENCEKOVACS"."superbowl_ads_cleaned"
        GROUP BY "year"
        ORDER BY "year"
    """)

@st.cache_data
def get_ads_with_videos():
    return run_query("""
        SELECT "year", COUNT(*) AS ads_with_youtube
        FROM "BENCEKOVACS"."superbowl_ads_cleaned"
        WHERE "video_id" IS NOT NULL
        GROUP BY "year"
        ORDER BY "year"
    """)

@st.cache_data
def get_ad_ticker_counts():
    return run_query("""
        SELECT "ticker", COUNT(*) AS ad_count
        FROM "BENCEKOVACS"."superbowl_ads_cleaned"
        WHERE "ticker" IS NOT NULL
        GROUP BY "ticker"
        ORDER BY ad_count DESC
    """)

@st.cache_data
def get_total_views():
    return run_query("""
        SELECT "ticker", SUM("viewcount") AS total_views
        FROM "BENCEKOVACS"."superbowl_ads_cleaned"
        WHERE "ticker" IS NOT NULL AND "viewcount" IS NOT NULL
        GROUP BY "ticker"
        ORDER BY total_views DESC
    """)

@st.cache_data
def get_industry_counts():
    return run_query("""
        SELECT f."industry", COUNT(*) AS ad_count
        FROM "BENCEKOVACS"."superbowl_ads_cleaned" a
        JOIN "BENCEKOVACS"."yahoo_financial_metadata_cleaned" f
        ON a."ticker" = f."ticker"
        WHERE a."ticker" IS NOT NULL AND f."industry" IS NOT NULL
        GROUP BY f."industry"
        ORDER BY ad_count DESC
    """)

@st.cache_data
def get_all_ads():
    return run_query("""
        SELECT "year", "ticker", "brand", "company_name", "title", "link", "youtube_link"
        FROM "BENCEKOVACS"."superbowl_ads_cleaned"
        ORDER BY "year" DESC
    """)

@st.cache_data
def get_public_vs_private():
    return run_query("""
        SELECT 
            "year",
            CASE 
                WHEN "ticker" IS NULL THEN 'Private / Non-profit'
                ELSE 'Public'
            END AS company_type,
            COUNT(*) AS ad_count
        FROM "BENCEKOVACS"."superbowl_ads_cleaned"
        GROUP BY "year", company_type
        ORDER BY "year", company_type
    """)


st.set_page_config(layout="wide")
st.title("🏈 Super Bowl Ad Impact Dashboard")

# === Sidebar Filters ===
tickers_df = run_query("""
    SELECT DISTINCT "ticker"
    FROM "BENCEKOVACS"."superbowl_ads_cleaned"
    WHERE "ticker" IS NOT NULL
    ORDER BY "ticker"
""")

ticker_list = tickers_df['ticker'].tolist()
selected_ticker = st.sidebar.selectbox("Select a Stock Ticker", ticker_list)

# === Tabs ===
tab1, tab2 = st.tabs(["📊 Ticker Details", "📈 Global Ad & Trend Insights"])

with tab1:
    # === Fetch Ad Data ===
    ads = run_query(f"""
        SELECT "ticker", "year", "publishedate"::DATE AS "ad_date", "viewcount", "likecount", "commentcount", "title"
        FROM "BENCEKOVACS"."superbowl_ads_cleaned"
        WHERE "ticker" = '{selected_ticker}'
    """)

    # === Stock Prices ===
    yahoo_prices = run_query(f"""
        SELECT "date"::DATE AS date, "close" 
        FROM "BENCEKOVACS"."yahoo_stock_data_cleaned"
        WHERE "ticker" = '{selected_ticker}'
        ORDER BY date
    """)

    polygon_prices = run_query(f"""
        SELECT "date"::DATE AS date, "close" 
        FROM "BENCEKOVACS"."polygon_stock_data_cleaned"
        WHERE "ticker" = '{selected_ticker}'
        ORDER BY date
    """)

    # === Financial Metadata - Yahoo ===
    yahoo_meta = run_query(f"""
        SELECT "name", "industry", "employees", "website",
               "income_statement.net_income" AS net_income,
               "income_statement.gross_profit" AS gross_profit,
               "description"
        FROM "BENCEKOVACS"."yahoo_financial_metadata_cleaned"
        WHERE "ticker" = '{selected_ticker}'
        LIMIT 1
    """)

    # === Financial Metadata - Polygon ===
    polygon_meta = run_query(f"""
        SELECT "name", "industry", "employees", "website", "exchange", "market_cap",  "list_date", "description"
        FROM "BENCEKOVACS"."polygon_metadata_cleaned"
        WHERE "ticker" = '{selected_ticker}'
        LIMIT 1
    """)

    # === Financials from polygon_financials_cleaned ===
    polygon_financials = run_query(f"""
        SELECT "filing_date"::DATE AS filing_date, "fiscal_year",
               "income_statement.gross_profit" AS gross_profit
        FROM "BENCEKOVACS"."polygon_financials_cleaned"
        WHERE "ticker" = '{selected_ticker}'
        ORDER BY filing_date
    """)

    # === Stock Prices and Viewcount Plot ===
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=polygon_prices["DATE"], y=polygon_prices["close"], name="Polygon Close", mode="lines", line=dict(color="blue")))
    fig.add_trace(go.Scatter(x=yahoo_prices["DATE"], y=yahoo_prices["close"], name="Yahoo Close", mode="lines", line=dict(color="purple", dash="dot")))
    fig.add_trace(go.Bar(x=ads["ad_date"], y=ads["viewcount"], name="Ad Viewcount", marker_color="green", yaxis="y2", width=1_000_000_000))
    fig.add_trace(go.Scatter(x=ads["ad_date"], y=[max(polygon_prices["close"].max(), yahoo_prices["close"].max()) * 1.05] * len(ads),
                             mode="markers", name="Ad Date", marker=dict(color="red", size=10), yaxis="y"))
    fig.update_layout(
        title=f"{selected_ticker} - Stock Prices & Super Bowl Ad Viewcount",
        xaxis_title="Date",
        yaxis=dict(title="Stock Close Price"),
        yaxis2=dict(title="Ad Viewcount", overlaying="y", side="right", showgrid=False),
        legend=dict(x=0, y=1.1, orientation="h")
    )
    st.plotly_chart(fig, use_container_width=True)

    # === Show Engagement Table ===
    if not ads.empty:
        st.subheader("📺 Ad Engagement Metrics")
        st.write(ads)
    else:
        st.info("No Super Bowl ads found for this ticker.")

    # === Metadata & Financials ===
    if not yahoo_meta.empty:
        st.subheader("🌐 Yahoo Financial Metadata")
        st.dataframe(yahoo_meta.drop(columns=["description"]))
        st.markdown(f"**Description:** {yahoo_meta['description'].iloc[0]}")

    if not polygon_meta.empty:
        st.subheader("💳 Polygon Company Metadata")
        st.dataframe(polygon_meta.drop(columns=["description"]))
        st.markdown(f"**Description:** {polygon_meta['description'].iloc[0]}")

    if not polygon_financials.empty:
        st.subheader("📊 Polygon Gross Profit Over Time")
        fig2 = px.line(polygon_financials, x="FILING_DATE", y="GROSS_PROFIT", markers=True, title="Gross Profit by Filing Date")
        st.plotly_chart(fig2, use_container_width=True)

    # === Related Tickers ===
    st.subheader("🔁 Related Tickers Performance")
    related_df = run_query(f"""
        SELECT DISTINCT "related_ticker"
        FROM "BENCEKOVACS"."yahoo_related_tickers_cleaned"
        WHERE "ticker" = '{selected_ticker}'
    """)
    related_tickers = related_df['related_ticker'].dropna().tolist()
    if not related_tickers:
        st.info(f"No related tickers found for {selected_ticker}.")
    else:
        st.markdown(f"Related tickers for **{selected_ticker}**: `{', '.join(related_tickers)}`")
        related_fig = go.Figure()
        color_palette = px.colors.qualitative.Set3 + px.colors.qualitative.Dark24
        for i, ticker in enumerate(related_tickers):
            for source, color_prefix in [("yahoo", "Yahoo"), ("polygon", "Polygon")]:
                table = "yahoo_stock_data_cleaned" if source == "yahoo" else "polygon_stock_data_cleaned"
                df = run_query(f"""
                    SELECT "date"::DATE AS date, "close"
                    FROM "BENCEKOVACS"."{table}"
                    WHERE "ticker" = '{ticker}'
                    AND "date" >= DATEADD(year, -1, CURRENT_DATE)
                    ORDER BY date
                """)
                if not df.empty:
                    related_fig.add_trace(go.Scatter(
                        x=df["DATE"], y=df["close"], mode="lines",
                        name=f"{ticker} ({color_prefix})",
                        line=dict(color=color_palette[i % len(color_palette)], dash="solid" if source == "yahoo" else "dot")
                    ))
        related_fig.update_layout(
            title="📉 Related Tickers - Stock Price Performance (Past Year)",
            xaxis_title="Date", yaxis_title="Close Price",
            legend=dict(x=0, y=1.1, orientation="h")
        )
        st.plotly_chart(related_fig, use_container_width=True)

with tab2:
    st.header("🧠 Market-Wide Super Bowl Ad Insights")

    col1, col2 = st.columns(2)
    with col1:
        ads_per_year = get_ads_per_year()
        fig_ads_year = px.bar(ads_per_year, x="year", y="AD_COUNT", title="Super Bowl Ads Per Year")
        st.plotly_chart(fig_ads_year, use_container_width=True)

    with col2:
        ads_with_videos = get_ads_with_videos()
        fig_vids = px.bar(ads_with_videos, x="year", y="ADS_WITH_YOUTUBE", title="Ads with YouTube Videos Per Year")
        st.plotly_chart(fig_vids, use_container_width=True)

    col3, col4 = st.columns(2)
    with col3:
        ad_ticker_count = get_ad_ticker_counts()
        fig_ticker_ads = px.bar(ad_ticker_count, x="ticker", y="AD_COUNT", title="Tickers by Super Bowl Ad Occurrence")
        st.plotly_chart(fig_ticker_ads, use_container_width=True)

    with col4:
        top_views = get_total_views()
        fig_views = px.bar(top_views, x="ticker", y="TOTAL_VIEWS", title="Tickers by Total YouTube Views")
        st.plotly_chart(fig_views, use_container_width=True)

    st.subheader("🏢 Industries by Super Bowl Ad Count")
    industry_counts = get_industry_counts()
    fig_industry = px.bar(industry_counts, x="industry", y="AD_COUNT", title="Super Bowl Ads by Industry")
    fig_industry.update_layout(xaxis_tickangle=-45)
    st.plotly_chart(fig_industry, use_container_width=True)

    st.subheader("📋 All Super Bowl Ads")
    all_ads = get_all_ads()
    st.dataframe(all_ads, use_container_width=True)

    st.subheader("🏢 Super Bowl Ad Sponsors: Public vs Private Companies")
    public_vs_private = get_public_vs_private()
    fig_companies = px.bar(
        public_vs_private,
        x="year", y="AD_COUNT", color="COMPANY_TYPE",
        title="Public vs Private Super Bowl Advertisers Per Year",
        labels={"AD_COUNT": "Number of Ads", "COMPANY_TYPE": "Company Type"},
    )

    fig_companies.update_layout(barmode="stack", xaxis_title="Year", yaxis_title="Number of Ads")
    st.plotly_chart(fig_companies, use_container_width=True)

