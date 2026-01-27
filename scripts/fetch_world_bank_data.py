import wbgapi as wb
import pandas as pd
from google.cloud import bigquery

# 1. Configuration - Double check these match your console!
PROJECT_ID = "peppy-appliance-460822-e0"
DATASET_ID = "global_analysis"
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.thp_global_metrics"

# Initialize the BigQuery Client
client = bigquery.Client(project=PROJECT_ID)

"""Global Baseline WLD The ultimate average for the planet.
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

    # Fetch all data first
    raw_data = wb.data.fetch(INDICATORS.keys(), economy=THP_AND_BENCHMARKS, time=range(2010, 2025))

    rows = []
    for item in raw_data:
        rows.append({
            'country_code': item['economy'],
            'year': int(item['time'][2:]),
            'indicator': INDICATORS.get(item['series']),
            'value': item['value']
        })

    # --- MOVED OUTSIDE THE LOOP ---
    # 1. Pivot to initial clean format
    df = pd.DataFrame(rows)
    df = df.pivot(index=['country_code', 'year'], columns='indicator', values='value').reset_index()

    # --- SAFETY CHECK: Ensure columns exist before math ---
    # If the API didn't return data for these, we create them as empty columns
    required_cols = ['child_stunting_pct', 'poverty_ratio']
    for col in required_cols:
        if col not in df.columns:
            df[col] = pd.NA

    # 2. PRO FEATURE: Synthetic Benchmark Generation
    print("📈 Calculating Synthetic World Averages from country data...")

    regional_codes = ['WLD', 'SSF', 'SAS', 'LCN', 'LIC', 'LMC']

    # Calculate the average of ONLY the 13 countries to create a "Portfolio Average"
    annual_averages = df[~df['country_code'].isin(regional_codes)].groupby('year')[required_cols].mean().reset_index()

    # Rename columns to reflect they are benchmarks
    annual_averages = annual_averages.rename(columns={
        'child_stunting_pct': 'world_stunting_avg',
        'poverty_ratio': 'world_poverty_avg'
    })

    # 3. Handle the "Swiss Cheese" (Forward Fill)
    annual_averages[['world_stunting_avg', 'world_poverty_avg']] = annual_averages[
        ['world_stunting_avg', 'world_poverty_avg']].ffill()

    # 4. Merge back to the main dataframe
    df = df.merge(annual_averages, on='year', how='left')

    # 5. Final Cleanup: Keep only the 13 THP Countries
    thp_countries_only = ['BGD', 'BEN', 'BFA', 'ETH', 'GHA', 'IND', 'MWI', 'MEX', 'MOZ', 'PER', 'SEN', 'UGA', 'ZMB']
    df_final = df[df['country_code'].isin(thp_countries_only)].copy()
    df_final = df_final.round(2)

    # --- UPLOAD SECTION ---
    print(f"📤 Uploading {len(df_final)} benchmarked rows to {TABLE_ID}...")
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

    try:
        client.load_table_from_dataframe(df_final, TABLE_ID, job_config=job_config).result()
        print(f"✅ Success! World benchmarks are now embedded in the raw table.")
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    run_contextual_etl()