import pandas as pd
from pathlib import Path

def merge_yearly_ad_data(folder_path, output_filename="all_years_merged.csv"):
    folder = Path(folder_path)
    all_files = sorted(folder.glob("*_enhanced.csv"))

    merged_df = pd.DataFrame()

    for file in all_files:
        try:
            # Extract year from filename
            year = int(file.stem.split("_")[0])
            df = pd.read_csv(file)
            df["year"] = year
            merged_df = pd.concat([merged_df, df], ignore_index=True)
            print(f"✅ Processed {file.name}")
        except Exception as e:
            print(f"❌ Failed to process {file.name}: {e}")

    # Save the merged DataFrame
    output_path = folder / output_filename
    merged_df.to_csv(output_path, index=False)
    print(f"\n📁 Merged CSV saved to: {output_path}")

    return merged_df


merged_df = merge_yearly_ad_data("superbowl_ads")