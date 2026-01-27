import pandas as pd
import requests
import base64
from google.cloud import bigquery

# --- CONFIGURATION ---
PROJECT_ID = "peppy-appliance-460822-e0"
DATASET_ID = "global_analysis"
# Corrected table name to reflect Poverty data
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.thp_poverty_mpi"

# THP Focus Countries
THP_ISO_CODES = ['BGD', 'BEN', 'BFA', 'ETH', 'GHA', 'IND', 'MWI', 'MEX', 'MOZ', 'PER', 'SEN', 'UGA', 'ZMB']


def get_hapi_identifier(name, email):
    """Encodes name:email for the HAPI header."""
    creds = f"{name.strip()}:{email.strip()}"
    return base64.b64encode(creds.encode('utf-8')).decode('utf-8')


def fetch_hapi_poverty_data():
    print("🛰️ Connecting to HDX HAPI (Multidimensional Poverty Index)...")

    # 1. Use your actual credentials here
    app_id = get_hapi_identifier("THP_Project", "aidangrambihler@gmail.com")

    # 2. Poverty-Rate endpoint (MPI)
    base_url = "https://hapi.humdata.org/api/v2/food-security-nutrition-poverty/poverty-rate"

    all_data = []
    for iso in THP_ISO_CODES:
        params = {
            'app_identifier': app_id,
            'location_code': iso,
            'output_format': 'json'
        }

        try:
            r = requests.get(base_url, params=params, timeout=10)
            if r.status_code == 200:
                batch = r.json().get('data', [])
                if batch:
                    # Append all records found for this country
                    all_data.extend(batch)
            else:
                print(f"⚠️ Error {r.status_code} for {iso}")
        except Exception as e:
            print(f"❌ Connection error for {iso}: {e}")

    if not all_data:
        print("❌ No data retrieved. The HAPI server might be down or your ID is rejected.")
        return

    # 3. Process Data
    df = pd.DataFrame(all_data)

    # Filter for the columns that tell the THP story
    # MPI = Overall index, headcount = % of people poor, intensity = how poor they are
    cols_to_keep = ['location_code', 'mpi', 'headcount_ratio', 'intensity_of_deprivation', 'reference_period_start']
    df = df[cols_to_keep].copy()

    # Rename for cleaner SQL queries later
    df.columns = ['country_code', 'mpi_score', 'poverty_headcount_pct', 'deprivation_intensity', 'report_year']

    # 4. Professional Upload to BigQuery
    print(f"📤 Uploading {len(df)} rows to {TABLE_ID}...")
    client = bigquery.Client(project=PROJECT_ID)

    # Using WRITE_TRUNCATE to keep the table fresh and clean
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

    client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config).result()
    print("🌟 Poverty MPI data successfully synced to BigQuery.")


if __name__ == "__main__":
    fetch_hapi_poverty_data()