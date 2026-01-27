import wbgapi as wb
import pandas as pd
from google.cloud import bigquery

# 1. Configuration - Double check these match your console!
PROJECT_ID = "peppy-appliance-460822-e0"
DATASET_ID = "global_analysis"
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.world_bank_indicators"

# Initialize the BigQuery Client
# It will automatically look for the credentials you just saved in Step 1
client = bigquery.Client(project=PROJECT_ID)


def run_etl():
    print("🚀 Fetching data from World Bank API...")

    # Indicators: GDP per capita and Poverty headcount ratio
    indicators = {
        'NY.GDP.PCAP.CD': 'gdp_per_capita',
        'SI.POV.DDAY': 'poverty_ratio'
    }

    # Pull data for the last 15 years
    df = wb.data.DataFrame(indicators.keys(), time=range(2010, 2025), labels=True)

    # 2. Transformation (The "Shablona" Clean)
    print("🧹 Cleaning data for BigQuery...")
    df = df.reset_index()
    # BigQuery column names cannot have dots or spaces
    df.columns = [c.lower().replace('.', '_').replace(' ', '_') for c in df.columns]

    # 3. Load to BigQuery
    print(f"📤 Uploading to {TABLE_ID}...")
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

    try:
        job = client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
        job.result()  # Wait for the upload to finish
        print("✅ Success! Data is now live in BigQuery.")
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    run_etl()