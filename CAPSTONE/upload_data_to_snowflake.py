import pandas as pd
from snowflake.snowpark import Session
import os

# === Force datetime columns to be timezone-aware UTC ===
def force_datetime_utc(df):
    for col in df.columns:
        if "date" in col.lower():
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce', utc=True)
            except Exception:
                pass  # Just skip any non-date-like junk
    return df

# === Snowflake connection parameters ===
connection_params = {
    "account": 'aab46027',
    "user": 'dataexpert_student',
    "password": 'DataExpert123!',
    "role": "all_users_role",
    "warehouse": 'COMPUTE_WH',
    "database": 'dataexpert_student',
    "schema": 'BENCEKOVACS'
}

# === Initialize Snowflake session ===
session = Session.builder.configs(connection_params).create()
print("✅ Snowflake session created.")

# === Folder where cleaned CSVs live ===
cleaned_folder = r"C:\Work_Git\DataEngineer\CAPSTONE\data\cleaned_csvs"

# === Upload each CSV file ===
for file in os.listdir(cleaned_folder):
    if file.endswith(".csv"):
        file_path = os.path.join(cleaned_folder, file)
        table_name = os.path.splitext(file)[0].lower()

        print(f"\n📤 Uploading: {file} ➜ Table: {table_name}")

        try:
            df = pd.read_csv(file_path)

            # Force all columns containing "date" to UTC datetime
            df = force_datetime_utc(df)

            # Confirm fix:
            print("📌 Column types before upload:")
            print(df.dtypes)

            # Upload to Snowflake
            session.write_pandas(
                df=df,
                table_name=table_name,
                auto_create_table=True,
                overwrite=True,
                use_logical_type=True  # <<< 🔥 THIS is the fix
            )


            print(f"✅ Uploaded to Snowflake table: {table_name}")

        except Exception as e:
            print(f"❌ Failed to upload {file}: {e}")
