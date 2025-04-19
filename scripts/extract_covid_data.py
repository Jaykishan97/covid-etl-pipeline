import pandas as pd
import datetime
import os
import logging
import json

# Directories
log_dir = "./logs"
metadata_dir = "./metadata"
staging_dir = "./staging"

# Create folders if not exist
os.makedirs(log_dir, exist_ok=True)
os.makedirs(metadata_dir, exist_ok=True)
os.makedirs(staging_dir, exist_ok=True)

# Setup logging
logging.basicConfig(filename=f"{log_dir}/extraction.log", level=logging.INFO)

# Constants
CSV_URL = "https://data.cdc.gov/api/views/9bhg-hcku/rows.csv?accessType=DOWNLOAD"
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
filename = f"covid_raw_{timestamp}.csv"
staging_path = os.path.join(staging_dir, filename)
metadata_path = os.path.join(metadata_dir, f"covid_metadata_{timestamp}.json")

# Expected schema
EXPECTED_COLUMNS = [
    "Data As Of", "Start Date", "End Date", "Group", "Year", "Month",
    "State", "Sex", "Age Group", "COVID-19 Deaths", "Total Deaths",
    "Pneumonia Deaths", "Pneumonia and COVID-19 Deaths", "Influenza Deaths",
    "Pneumonia, Influenza, or COVID-19 Deaths", "Footnote"
]

def extract_data():
    try:
        df = pd.read_csv(CSV_URL)
        df.to_csv(staging_path, index=False)

        # ✅ Schema validation
        if list(df.columns) != EXPECTED_COLUMNS:
            logging.error(f"{timestamp} - SCHEMA MISMATCH!")
            print("❌ Schema mismatch!")
            return

        # ✅ Metadata capture
        metadata = {
            "file_name": filename,
            "timestamp": timestamp,
            "row_count": df.shape[0],
            "column_count": df.shape[1],
            "columns": list(df.columns),
            "file_size_kb": round(os.path.getsize(staging_path) / 1024, 2),
            "source_url": CSV_URL
        }

        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=4)

        logging.info(f"{timestamp} - SUCCESS: Data saved to {staging_path}")
        print(f"✅ Data saved to: {staging_path}")
        print(f"📄 Metadata saved to: {metadata_path}")

    except Exception as e:
        logging.error(f"{timestamp} - ERROR: {str(e)}")
        print(f"❌ Failed: {e}")

if __name__ == "__main__":
    extract_data()
