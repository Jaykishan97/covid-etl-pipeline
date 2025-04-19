import pandas as pd
import datetime
import os
import logging

# Setup logging
log_dir = "./logs"
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(filename=f"{log_dir}/extraction.log", level=logging.INFO)

# CDC dataset URL
CSV_URL = "https://data.cdc.gov/api/views/9bhg-hcku/rows.csv?accessType=DOWNLOAD"

# File and path setup
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
filename = f"covid_raw_{timestamp}.csv"
staging_path = f"./staging/{filename}"

def extract_data():
    try:
        os.makedirs("staging", exist_ok=True)
        df = pd.read_csv(CSV_URL)
        df.to_csv(staging_path, index=False)
        logging.info(f"{timestamp} - SUCCESS: Data saved to {staging_path}")
        print(f"✅ Data saved to: {staging_path}")
    except Exception as e:
        logging.error(f"{timestamp} - ERROR: {str(e)}")
        print(f"❌ Failed: {e}")

if __name__ == "__main__":
    extract_data()
