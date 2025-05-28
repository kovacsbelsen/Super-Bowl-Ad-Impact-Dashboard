import pandas as pd

def summarize_column_counts(csv_path):
    try:
        df = pd.read_csv(csv_path)
        counts = df.count().sort_values(ascending=False)
        print(f"\n📊 Non-null value count per column in {csv_path}:\n")
        print(counts)
        return counts
    except Exception as e:
        print(f"❌ Failed to read or process the file: {e}")
        return None

summarize_column_counts(r"C:\Work_Git\DataEngineer\CAPSTONE\ingest\all_years_merged_with_stocks_with_youtube.csv")
