import time

from googleapiclient.discovery import build


import pandas as pd
import re

def extract_video_id(url):
    if pd.isna(url):
        return None
    patterns = [
        r"youtube\.com/watch\?v=([^\s&#]+)",
        r"youtu\.be/([^\s&#]+)",
        r"youtube\.com/embed/([^\s&#?]+)"
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None

def add_video_id_column(csv_path, output_path=None):
    df = pd.read_csv(csv_path)
    df["video_id"] = df["youtube_link"].apply(extract_video_id)

    output_path = output_path or csv_path.replace(".csv", "_with_video_id.csv")
    df.to_csv(output_path, index=False)
    print(f"✅ Added video_id column and saved to: {output_path}")
    return df




def fetch_youtube_stats(video_id, api_key):
    try:
        youtube = build("youtube", "v3", developerKey=api_key)
        response = youtube.videos().list(
            part="snippet,statistics",
            id=video_id
        ).execute()

        item = response['items'][0]
        stats = item.get("statistics", {})
        snippet = item.get("snippet", {})

        return {
            "title_api": snippet.get("title"),
            "publishedAt": snippet.get("publishedAt"),
            "channelTitle": snippet.get("channelTitle"),
            "viewCount": stats.get("viewCount"),
            "likeCount": stats.get("likeCount"),
            "commentCount": stats.get("commentCount")
        }
    except Exception as e:
        print(f"Error for video {video_id}: {e}")
        return {}


import requests

def fetch_dislike_estimate(video_id):
    try:
        url = f"https://returnyoutubedislikeapi.com/votes?videoId={video_id}"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            return data.get("dislikes")
    except Exception as e:
        print(f"Dislike API failed for {video_id}: {e}")
    return None

"""

def enrich_dataset_with_youtube_data(csv_path, api_key, output_path=None):
    df = pd.read_csv(csv_path)

    # Ensure video_id exists
    if "video_id" not in df.columns:
        df["video_id"] = df["youtube_link"].apply(extract_video_id)

    # Add empty columns
    for col in ["title_api", "publishedAt", "channelTitle", "viewCount", "likeCount", "commentCount", "dislikeCount"]:
        df[col] = None

    for idx, row in df.iterrows():
        vid = row.get("video_id")
        if pd.notna(vid):
            print(f"📺 Fetching data for: {vid}")
            stats = fetch_youtube_stats(vid, api_key)
            dislikes = fetch_dislike_estimate(vid)

            for key, value in stats.items():
                df.at[idx, key] = value
            df.at[idx, "dislikeCount"] = dislikes

            time.sleep(1)  # avoid rate-limiting

    output_path = output_path or csv_path.replace(".csv", "_enriched.csv")
    df.to_csv(output_path, index=False)
    print(f"✅ Enriched CSV saved to: {output_path}")
    return df
"""

def enrich_dataset_with_youtube_data(csv_path, api_key, output_path=None):
    df = pd.read_csv(csv_path)

    # Ensure video_id exists
    if "video_id" not in df.columns:
        df["video_id"] = df["youtube_link"].apply(extract_video_id)

    # Ensure all columns exist
    required_cols = ["title_api", "publishedAt", "channelTitle", "viewCount", "likeCount", "commentCount", "dislikeCount"]
    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    for idx, row in df.iterrows():
        vid = row.get("video_id")

        # Only fetch data if all fields are still missing
        if pd.notna(vid) and all(pd.isna(row.get(col)) for col in required_cols):
            print(f"📺 Fetching data for: {vid}")
            try:
                stats = fetch_youtube_stats(vid, api_key)
                dislikes = fetch_dislike_estimate(vid)

                for key, value in stats.items():
                    df.at[idx, key] = value
                df.at[idx, "dislikeCount"] = dislikes

                time.sleep(1)  # avoid rate-limiting
            except Exception as e:
                print(f"❌ Error for video {vid}: {e}")
        else:
            print(f"⏩ Skipping already processed video: {vid}")

    output_path = output_path or csv_path.replace(".csv", "_enriched.csv")
    df.to_csv(output_path, index=False)
    print(f"✅ Updated CSV saved to: {output_path}")
    return df



enrich_dataset_with_youtube_data(r"C:\Work_Git\DataEngineer\CAPSTONE\ingest\all_years_merged_with_stocks_with_video_id.csv", api_key="AIzaSyBUSqXpwRksGIz8uPeNcgfgNuhriBwXZzI")
