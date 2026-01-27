import pandas as pd
import requests
from google.cloud import bigquery

# --- CONFIGURATION ---
PROJECT_ID = "peppy-appliance-460822-e0"
DATASET_ID = "global_analysis"
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.wfp_realtime_metrics"

# THP Countries (ISO3)
THP_ISO_CODES = ['BGD', 'BEN', 'BFA', 'ETH', 'GHA', 'IND', 'MWI', 'MEX', 'MOZ', 'PER', 'SEN', 'UGA', 'ZMB']


def fetch_wfp_hungermap():
    print("🛰️ Accessing WFP HungerMap LIVE via HDX...")

    # We use the WFP global data endpoint on HDX
    # This URL provides a real-time CSV of the global hunger monitoring system
    wfp_url = "https://data.humdata.org/hxlproxy/data.csv?url=https%3A%2F%2Fdocs.google.com%2Fspreadsheets%2Fd%2Fe%2F2PACX-1vSbeS6_6m7pUfK8T5tH_MhG8p5k5Y5f9p-r1L1p%2Fpub%3Foutput%3Dcsv&select=adm0_id%2Cadm0_name%2Ciso3%2Cdate%2Cfcs_prevalence%2Crcsi_prevalence%2Cmarket_access_prevalence%2Chealth_access_prevalence"

    try:
        # 1. Fetch the CSV
        df = pd.read_csv(wfp_url)

        # 2. Filter for THP Countries
        df_thp = df[df['iso3'].isin(THP_ISO_CODES)].copy()

        # 3. Clean and Select Columns
        # fcs = Food Consumption Score (Standard hunger metric)
        # rcsi = Reduced Coping Strategy Index (How people adapt to hunger)
        df_thp = df_thp[['iso3', 'date', 'fcs_prevalence', 'rcsi_prevalence', 'market_access_prevalence']]

        # 4. Rename for BigQuery
        df_thp.rename(columns={'iso3': 'country_code'}, inplace=True)

        # 5. Convert date column to actual datetime
        df_thp['date'] = pd.to_datetime(df_thp['date'])
        df_thp['last_updated'] = pd.Timestamp.now()

        print(f"✅ Filtered {len(df_thp)} real-time records. Uploading to BigQuery...")

        # 6. Upload (Write Truncate since we want the LATEST snapshot)
        client = bigquery.Client(project=PROJECT_ID)
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

        client.load_table_from_dataframe(df_thp, TABLE_ID, job_config=job_config).result()
        print("🌟 WFP Real-Time Integration Complete.")

    except Exception as e:
        print(f"❌ WFP Fetch Failed: {e}")


if __name__ == "__main__":
    fetch_wfp_hungermap()