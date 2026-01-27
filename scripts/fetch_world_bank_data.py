import wbgapi as wb
import pandas as pd
from google.cloud import bigquery

# Search for indicators by keyword
print("--- Gender & Women's Empowerment ---")
print(wb.series.info(q='gender'))

print("\n--- Nutrition & Hunger ---")
print(wb.series.info(q='nutrition'))

print("\n--- Rural Development & Agriculture ---")
print(wb.series.info(q='agriculture'))


# 1. Configuration - Double check these match your console!
PROJECT_ID = "peppy-appliance-460822-e0"
DATASET_ID = "global_analysis"
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.thp_global_metrics"

# Initialize the BigQuery Client
client = bigquery.Client(project=PROJECT_ID)

"""Global BaselineWLDThe ultimate average for the planet.
Sub-Saharan Africa SSF Context for Benin, Ethiopia, Ghana, etc.
South Asia SAS Context for Bangladesh and India.
Latin America LCN Context for Mexico and Peru.
Low Income LIC Benchmarking against the most vulnerable peers.
Lower Middle Inc. LMC Benchmarking against countries in transition."""

# 2. THP Countries and Benchmarks (ISO-3 Codes)
THP_AND_BENCHMARKS = [
    # THP Countries
    'BGD', 'BEN', 'BFA', 'ETH', 'GHA', 'IND', 'MWI', 
    'MEX', 'MOZ', 'PER', 'SEN', 'UGA', 'ZMB',
    # Benchmarks (Aggregates)
    'WLD', 'SSF', 'SAS', 'LCN', 'LIC', 'LMC' 
]

# 3. THP Relevant Indicators
INDICATORS = {
    # Existing
    'SH.STA.STNT.ZS': 'child_stunting_pct',
    'SG.GEN.LSWP.ZS': 'women_parliament_pct',
    'SL.AGR.EMPL.ZS': 'agri_employment_pct',
    'SH.STA.BRTW.ZS': 'low_birthweight_pct',
    'SI.POV.DDAY': 'poverty_ratio',

    # NEW: Food & Resilience
    'SN.ITK.DEFC.ZS': 'undernourishment_prev_pct',  # Basic hunger metric
    'AG.LND.AGRI.ZS': 'agricultural_land_pct',  # Resource base

    # NEW: Health & Water (THP focus on sanitation)
    'SH.H2O.BASW.ZS': 'basic_water_access_pct',
    'SH.STA.BASS.ZS': 'basic_sanitation_access_pct',

    # NEW: Human Capital (Self-reliance)
    'SE.PRM.CMPT.ZS': 'primary_completion_rate',  # Education baseline
    'SL.TLF.CACT.FE.ZS': 'female_labor_participation'  # Economic empowerment
}

def run_contextual_etl():
    print(f"🚀 Fetching THP-specific data...")

    # Fetch data - filtering by country list and indicator list
    raw_data = wb.data.fetch(INDICATORS.keys(), economy=THP_AND_BENCHMARKS, time=range(2010, 2025))

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
    run_contextual_etl()