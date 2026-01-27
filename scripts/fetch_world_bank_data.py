import wbgapi as wb
import pandas as pd
from google.cloud import bigquery

# 1. Configuration - Double check these match your console!
PROJECT_ID = "peppy-appliance-460822-e0"
DATASET_ID = "global_analysis"
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.thp_global_metrics"

# Initialize the BigQuery Client
client = bigquery.Client(project=PROJECT_ID)

# 2. THP Countries (ISO-3 Codes)
THP_COUNTRIES = [
    'BGD', 'BEN', 'BFA', 'ETH', 'GHA', 'IND', 'MWI',
    'MEX', 'MOZ', 'PER', 'SEN', 'UGA', 'ZMB'
]

# 3. THP Relevant Indicators
INDICATORS = {
    'SH.STA.STNT.ZS': 'child_stunting_pct',
    'SG.GEN.LSWP.ZS': 'women_parliament_pct',
    'SL.AGR.EMPL.ZS': 'agri_employment_pct',
    'SH.STA.BRTW.ZS': 'low_birthweight_pct',
    'SI.POV.DDAY': 'poverty_ratio'
}

def run_thp_etl():
    print(f"🚀 Fetching THP-specific data for {len(THP_COUNTRIES)} countries...")

    # Fetch data - filtering by country list and indicator list
    raw_data = wb.data.fetch(INDICATORS.keys(), economy=THP_COUNTRIES, time=range(2010, 2025))

    rows = []
    for item in raw_data:
        rows.append({
            'country_code': item['economy'],
            'year': int(item['time'][2:]),
            'indicator': INDICATORS.get(item['series']),
            'value': item['value']
        })

    # Pivot to clean format
    df = pd.DataFrame(rows)
    df = df.pivot(index=['country_code', 'year'], columns='indicator', values='value').reset_index()

    print(f"📤 Uploading THP metrics to {TABLE_ID}...")
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

    try:
        job = client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
        job.result()
        print(f"✅ Success! {len(df)} rows uploaded.")
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    run_thp_etl()