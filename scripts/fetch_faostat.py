import pandas as pd
import requests
import zipfile
import io
import os
from google.cloud import bigquery

# --- CONFIGURATION ---
PROJECT_ID = "peppy-appliance-460822-e0"
DATASET_ID = "global_analysis"
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.faostat_metrics"

# THP Countries and their FAO name mappings
# Mapping to ISO3 ensures this JOINs perfectly with your World Bank data later
COUNTRY_MAP = {
    'Bangladesh': 'BGD', 'Benin': 'BEN', 'Burkina Faso': 'BFA',
    'Ethiopia': 'ETH', 'Ghana': 'GHA', 'India': 'IND',
    'Malawi': 'MWI', 'Mexico': 'MEX', 'Mozambique': 'MOZ',
    'Peru': 'PER', 'Senegal': 'SEN', 'Uganda': 'UGA', 'Zambia': 'ZMB'
}


def fetch_faostat_bulk():
    """Automated ETL for FAOSTAT Food Security Suite."""
    print("🚀 Starting FAOSTAT Bulk ETL...")

    # Static URL for the 'Suite of Food Security Indicators' (Normalized/Long Format)
    bulk_url = "https://fenixservices.fao.org/faostat/static/bulkdownloads/Food_Security_Data_E_All_Data_(Normalized).zip"

    try:
        # 1. Download Zip in memory
        response = requests.get(bulk_url, timeout=60)
        response.raise_for_status()

        # 2. Extract the main CSV from the zip
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            csv_filename = [f for f in z.namelist() if f.endswith('.csv')][0]
            with z.open(csv_filename) as f:
                # FAO uses 'latin1' encoding for many files
                df = pd.read_csv(f, encoding='latin1')

        print(f"✅ Downloaded {len(df)} raw rows. Filtering for THP countries...")

        # 3. Filter for THP Countries & Map to ISO3
        df_thp = df[df['Area'].isin(COUNTRY_MAP.keys())].copy()
        df_thp['country_code'] = df_thp['Area'].map(COUNTRY_MAP)

        # 4. Filter for relevant indicators
        # We focus on PoU (Undernourishment) and FIES (Food Insecurity)
        relevant_items = [
            'Prevalence of undernourishment (percent) (3-year average)',
            'Prevalence of moderate or severe food insecurity in the total population (percent) (3-year average)',
            'Average dietary energy supply adequacy (percent) (3-year average)'
        ]
        df_thp = df_thp[df_thp['Item'].isin(relevant_items)]

        # 5. Clean Years (FAO often uses ranges like '2020-2022'; we take the end year)
        df_thp['year'] = df_thp['Year'].apply(lambda x: int(x[-4:]) if '-' in str(x) else int(x))

        # 6. Pivot to Wide Format (One row per Country/Year)
        df_final = df_thp.pivot_table(
            index=['country_code', 'year'],
            columns='Item',
            values='Value',
            aggfunc='first'
        ).reset_index()

        # Clean column names for BigQuery (no spaces or special chars)
        df_final.columns = [
            c.lower().replace(" ", "_").replace("(", "").replace(")", "").replace("-", "_")
            for c in df_final.columns
        ]

        # 7. Upload to BigQuery
        client = bigquery.Client(project=PROJECT_ID)
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

        print(f"📤 Uploading {len(df_final)} cleaned rows to {TABLE_ID}...")
        client.load_table_from_dataframe(df_final, TABLE_ID, job_config=job_config).result()

        print("🌟 FAOSTAT Integration Complete.")

    except Exception as e:
        print(f"❌ Automation failed: {e}")


if __name__ == "__main__":
    fetch_faostat_bulk()