import wbgapi as wb
import pandas as pd
from google.cloud import bigquery

# 1. Configuration
PROJECT_ID = "peppy-appliance-460822-e0"
DATASET_ID = "global_analysis"
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.thp_global_metrics"

client = bigquery.Client(project=PROJECT_ID)

# 2. Global Constants
THP_COUNTRIES = ['BGD', 'BEN', 'BFA', 'ETH', 'GHA', 'IND', 'MWI', 'MEX', 'MOZ', 'PER', 'SEN', 'UGA', 'ZMB']
BENCHMARKS = ['WLD', 'SSF', 'SAS', 'LCN', 'LIC', 'LMC']
ALL_CODES = THP_COUNTRIES + BENCHMARKS

INDICATORS = {
    'SH.STA.STNT.ZS': 'child_stunting_pct',
    'SG.GEN.LSWP.ZS': 'women_parliament_pct',
    'SL.AGR.EMPL.ZS': 'agri_employment_pct',
    'SH.STA.BRTW.ZS': 'low_birthweight_pct',
    'SI.POV.DDAY': 'poverty_ratio',
    'SN.ITK.DEFC.ZS': 'undernourishment_prev_pct',
    'AG.LND.AGRI.ZS': 'agricultural_land_pct',
    'SH.H2O.BASW.ZS': 'basic_water_access_pct',
    'SH.STA.BASS.ZS': 'basic_sanitation_access_pct',
    'SE.PRM.CMPT.ZS': 'primary_completion_rate',
    'SL.TLF.CACT.FE.ZS': 'female_labor_participation',

# POPULATION & GROWTH
    'SP.POP.TOTL': 'total_population',
    'SP.POP.GROW': 'population_growth_annual_pct',
    'SP.RUR.TOTL.ZS': 'rural_population_pct',
    'SP.DYN.TFRT.IN': 'fertility_rate_total'
}


def run_contextual_etl():
    print(f"🚀 Fetching data...")

    # 1. Fetch data
    raw_data = wb.data.fetch(INDICATORS.keys(), economy=ALL_CODES, time=range(2010, 2025))

    rows = []
    for item in raw_data:
        rows.append({
            'country_code': item['economy'],
            'year': int(item['time'][2:]),
            'indicator': INDICATORS.get(item['series']),
            'value': item['value']
        })

    df = pd.DataFrame(rows)
    df['year'] = df['year'].astype(int)
    df = df.pivot(index=['country_code', 'year'], columns='indicator', values='value').reset_index()

    # 2. Extract Poverty Benchmarks (Removing Stunting Benchmarks)
    print("📈 Processing Poverty Benchmarks...")
    benchmarks_df = df[df['country_code'].isin(BENCHMARKS)].copy()

    # We only pivot poverty_ratio now
    bench_pivot = benchmarks_df.pivot(index='year', columns='country_code',
                                      values=['poverty_ratio'])

    # Flatten names (e.g., poverty_ratio_WLD)
    bench_pivot.columns = [f"{col[0]}_{col[1]}" for col in bench_pivot.columns]
    bench_pivot = bench_pivot.reset_index()

    # 3. Rename Map (Only Poverty)
    rename_map = {
        'poverty_ratio_WLD': 'world_poverty_avg',
        'poverty_ratio_SSF': 'ssf_poverty_avg',
        'poverty_ratio_SAS': 'sas_poverty_avg',
        'poverty_ratio_LCN': 'lcn_poverty_avg',
        'poverty_ratio_LIC': 'poverty_ratio_LIC',
        'poverty_ratio_LMC': 'poverty_ratio_LMC'
    }
    bench_pivot = bench_pivot.rename(columns=rename_map)

    # 4. Fill gaps in poverty data
    bench_pivot = bench_pivot.sort_values('year').ffill().bfill()

    # 5. Merge back to THP Countries
    df_final = df[df['country_code'].isin(THP_COUNTRIES)].copy()
    df_final = df_final.merge(bench_pivot, on='year', how='left')
    df_final = df_final.round(4)

    # 6. BigQuery Upload
    print(f"📤 Resetting table and uploading {len(df_final)} rows...")
    client.delete_table(TABLE_ID, not_found_ok=True)

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)

    try:
        client.load_table_from_dataframe(df_final, TABLE_ID, job_config=job_config).result()
        print(f"✅ Success! Table is clean and poverty benchmarks are populated.")
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    run_contextual_etl()