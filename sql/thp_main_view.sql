CREATE OR REPLACE VIEW `peppy-appliance-460822-e0.global_analysis.v_thp_master_dashboard` AS
WITH raw_joined AS (
    SELECT
        wb.country_code,
        CASE wb.country_code
            WHEN 'BGD' THEN 'Bangladesh' WHEN 'BEN' THEN 'Benin'
            WHEN 'BFA' THEN 'Burkina Faso' WHEN 'ETH' THEN 'Ethiopia'
            WHEN 'GHA' THEN 'Ghana' WHEN 'IND' THEN 'India'
            WHEN 'MWI' THEN 'Malawi' WHEN 'MEX' THEN 'Mexico'
            WHEN 'MOZ' THEN 'Mozambique' WHEN 'PER' THEN 'Peru'
            WHEN 'SEN' THEN 'Senegal' WHEN 'UGA' THEN 'Uganda'
            WHEN 'ZMB' THEN 'Zambia' ELSE wb.country_code
        END AS country_name,
        wb.year,
        -- World Bank Core
        SAFE_CAST(wb.poverty_ratio AS FLOAT64) as poverty_ratio,
        SAFE_CAST(wb.child_stunting_pct AS FLOAT64) as child_stunting_pct,
        SAFE_CAST(wb.low_birthweight_pct AS FLOAT64) as low_birthweight_pct,
        SAFE_CAST(wb.agri_employment_pct AS FLOAT64) as agri_employment_pct,
        -- Population & Growth
        SAFE_CAST(wb.total_population AS FLOAT64) as total_pop_raw,
        SAFE_CAST(wb.population_growth_annual_pct AS FLOAT64) as pop_growth_raw,
        SAFE_CAST(wb.rural_population_pct AS FLOAT64) as rural_pop_raw,
        SAFE_CAST(wb.fertility_rate_total AS FLOAT64) as fertility_raw,
        -- Poverty Benchmarks
        SAFE_CAST(wb.world_poverty_avg AS FLOAT64) as world_pov_raw,
        SAFE_CAST(wb.ssf_poverty_avg AS FLOAT64) as ssf_pov_raw,
        SAFE_CAST(wb.sas_poverty_avg AS FLOAT64) as sas_pov_raw,
        SAFE_CAST(wb.lcn_poverty_avg AS FLOAT64) as lcn_pov_raw,
        SAFE_CAST(wb.poverty_ratio_LIC AS FLOAT64) as lic_pov_raw,
        SAFE_CAST(wb.poverty_ratio_LMC AS FLOAT64) as lmc_pov_raw,
        -- FAO & MPI
        SAFE_CAST(fao.average_dietary_energy_supply_adequacy_percent_3_year_average AS FLOAT64) AS energy_adequacy,
        SAFE_CAST(fao.prevalence_of_undernourishment_percent_3_year_average AS FLOAT64) AS fao_undernourishment,
        SAFE_CAST(fao.prevalence_of_moderate_or_severe_food_insecurity_in_the_total_population_percent_3_year_average AS FLOAT64) AS food_insecurity_pct,
        SAFE_CAST(poverty.poverty_headcount_pct AS FLOAT64) as mpi_headcount,
        SAFE_CAST(poverty.deprivation_intensity AS FLOAT64) as mpi_intensity,
        SAFE_CAST(poverty.mpi_score AS FLOAT64) as mpi_score,
        -- Provenance
        CASE WHEN wb.poverty_ratio IS NOT NULL THEN wb.year ELSE NULL END as pov_yr,
        CASE WHEN wb.child_stunting_pct IS NOT NULL THEN wb.year ELSE NULL END as stunt_yr
    FROM `peppy-appliance-460822-e0.global_analysis.thp_global_metrics` wb
    LEFT JOIN `peppy-appliance-460822-e0.global_analysis.faostat_metrics` fao ON wb.country_code = fao.country_code AND wb.year = fao.year
    LEFT JOIN `peppy-appliance-460822-e0.global_analysis.thp_poverty_mpi` poverty ON wb.country_code = poverty.country_code AND CAST(wb.year AS STRING) = LEFT(CAST(poverty.report_year AS STRING), 4)
),
aggregated_data AS (
    SELECT
        country_code, country_name, year,
        MAX(poverty_ratio) as pov_raw, MAX(child_stunting_pct) as stunt_raw,
        MAX(low_birthweight_pct) as lbw_raw, MAX(agri_employment_pct) as agri_raw,
        MAX(total_pop_raw) as total_pop_raw, MAX(pop_growth_raw) as pop_growth_raw,
        MAX(rural_pop_raw) as rural_pop_raw, MAX(fertility_raw) as fertility_raw,
        MAX(world_pov_raw) as world_pov_raw, MAX(ssf_pov_raw) as ssf_pov_raw,
        MAX(sas_pov_raw) as sas_pov_raw, MAX(lcn_pov_raw) as lcn_pov_raw,
        MAX(lic_pov_raw) as lic_pov_raw, MAX(lmc_pov_raw) as lmc_pov_raw,
        MAX(energy_adequacy) as energy_raw, MAX(fao_undernourishment) as hunger_raw,
        MAX(food_insecurity_pct) as insecurity_raw, MAX(mpi_headcount) as mpi_hc_raw,
        MAX(mpi_intensity) as mpi_int_raw, MAX(mpi_score) as mpi_score_raw,
        MAX(pov_yr) as pov_yr_raw, MAX(stunt_yr) as stunt_yr_raw
    FROM raw_joined GROUP BY 1, 2, 3
)
SELECT
    country_code, country_name, year,
    -- 1. Population (RAW)
    ROUND(total_pop_raw / 1000000, 2) AS total_population_millions,
    ROUND(pop_growth_raw / 100, 4) AS population_growth_pct,
    ROUND(rural_pop_raw / 100, 4) AS rural_population_pct,
    ROUND(fertility_raw, 2) AS fertility_rate,
    -- 2. Regional Benchmark
    CASE
        WHEN country_code IN ('BGD', 'IND') THEN ROUND(sas_pov_raw / 100, 4)
        WHEN country_code IN ('MEX', 'PER') THEN ROUND(lcn_pov_raw / 100, 4)
        ELSE ROUND(ssf_pov_raw / 100, 4)
    END AS regional_poverty_avg,
    -- 3. Filled Metrics
    ROUND(LAST_VALUE(pov_raw IGNORE NULLS) OVER (PARTITION BY country_code ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / 100, 4) AS poverty_ratio,
    ROUND(LAST_VALUE(stunt_raw IGNORE NULLS) OVER (PARTITION BY country_code ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / 100, 4) AS child_stunting_pct,
    ROUND(LAST_VALUE(lbw_raw IGNORE NULLS) OVER (PARTITION BY country_code ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / 100, 4) AS low_birthweight_pct,
    ROUND(LAST_VALUE(agri_raw IGNORE NULLS) OVER (PARTITION BY country_code ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / 100, 4) AS agri_employment_pct,
    ROUND(LAST_VALUE(energy_raw IGNORE NULLS) OVER (PARTITION BY country_code ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / 100, 4) AS energy_adequacy,
    ROUND(LAST_VALUE(hunger_raw IGNORE NULLS) OVER (PARTITION BY country_code ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / 100, 4) AS fao_undernourishment_pct,
    ROUND(LAST_VALUE(insecurity_raw IGNORE NULLS) OVER (PARTITION BY country_code ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / 100, 4) AS food_insecurity_pct,
    ROUND(LAST_VALUE(mpi_hc_raw IGNORE NULLS) OVER (PARTITION BY country_code ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / 100, 4) AS mpi_headcount_pct,
    ROUND(LAST_VALUE(mpi_int_raw IGNORE NULLS) OVER (PARTITION BY country_code ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / 100, 4) AS mpi_intensity,
    ROUND(LAST_VALUE(mpi_score_raw IGNORE NULLS) OVER (PARTITION BY country_code ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 3) AS mpi_score,
    -- 4. Raw Metrics
    ROUND(pov_raw / 100, 4) AS poverty_ratio_raw,
    ROUND(stunt_raw / 100, 4) AS child_stunting_pct_raw,
    ROUND(mpi_score_raw, 3) AS mpi_score_raw,
    -- 5. Poverty Benchmarks
    ROUND(world_pov_raw / 100, 4) AS world_poverty_avg,
    ROUND(lic_pov_raw / 100, 4) AS low_income_poverty_avg,
    ROUND(lmc_pov_raw / 100, 4) AS lower_middle_income_poverty_avg,
    -- 6. Provenance
    CAST(LAST_VALUE(pov_yr_raw IGNORE NULLS) OVER (PARTITION BY country_code ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS INT64) AS poverty_data_year,
    CAST(LAST_VALUE(stunt_yr_raw IGNORE NULLS) OVER (PARTITION BY country_code ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS INT64) AS stunting_data_year
FROM aggregated_data;