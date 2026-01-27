CREATE OR REPLACE VIEW `peppy-appliance-460822-e0.global_analysis.v_thp_master_dashboard` AS
WITH raw_joined AS (
    SELECT
        wb.country_code,
        wb.year,
        -- Force numeric types using SAFE_CAST to prevent crashes
        SAFE_CAST(wb.poverty_ratio AS FLOAT64) as poverty_ratio,
        SAFE_CAST(wb.world_poverty_avg AS FLOAT64) as world_poverty_avg,
        SAFE_CAST(wb.child_stunting_pct AS FLOAT64) as child_stunting_pct,
        SAFE_CAST(wb.world_stunting_avg AS FLOAT64) as world_stunting_avg,
        SAFE_CAST(wb.low_birthweight_pct AS FLOAT64) as low_birthweight_pct,
        SAFE_CAST(wb.agri_employment_pct AS FLOAT64) as agri_employment_pct,
        SAFE_CAST(fao.average_dietary_energy_supply_adequacy_percent_3_year_average AS FLOAT64) AS energy_adequacy,
        SAFE_CAST(fao.prevalence_of_undernourishment_percent_3_year_average AS FLOAT64) AS undernourishment_pct,
        SAFE_CAST(fao.prevalence_of_moderate_or_severe_food_insecurity_in_the_total_population_percent_3_year_average AS FLOAT64) AS food_insecurity_pct,
        SAFE_CAST(poverty.mpi_score AS FLOAT64) as mpi_score,
        -- Provenance Logic
        CASE WHEN wb.poverty_ratio IS NOT NULL THEN wb.year ELSE NULL END as pov_yr,
        CASE WHEN wb.child_stunting_pct IS NOT NULL THEN wb.year ELSE NULL END as stunt_yr,
        CASE WHEN poverty.mpi_score IS NOT NULL THEN wb.year ELSE NULL END as mpi_yr
    FROM
        `peppy-appliance-460822-e0.global_analysis.thp_global_metrics` wb
    LEFT JOIN
        `peppy-appliance-460822-e0.global_analysis.faostat_metrics` fao
        ON wb.country_code = fao.country_code AND wb.year = fao.year
    LEFT JOIN
        `peppy-appliance-460822-e0.global_analysis.thp_poverty_mpi` poverty
        ON wb.country_code = poverty.country_code
        AND CAST(wb.year AS STRING) = LEFT(CAST(poverty.report_year AS STRING), 4)
),
deduplicated AS (
    SELECT
        country_code, year,
        MAX(poverty_ratio) as pov_raw,
        MAX(world_poverty_avg) as world_pov_raw,
        MAX(child_stunting_pct) as stunt_raw,
        MAX(world_stunting_avg) as world_stunt_raw,
        MAX(mpi_score) as mpi_raw,
        MAX(pov_yr) as pov_yr_raw,
        MAX(stunt_yr) as stunt_yr_raw,
        MAX(mpi_yr) as mpi_yr_raw,
        MAX(energy_adequacy) as energy_adequacy,
        MAX(undernourishment_pct) as undernourishment_pct,
        MAX(food_insecurity_pct) as food_insecurity_pct,
        MAX(agri_employment_pct) as agri_employment_pct
    FROM raw_joined
    GROUP BY 1, 2
)

SELECT
    country_code,
    year,
    -- 1. FORWARD-FILLED COUNTRY METRICS (Rounded)
    ROUND(LAST_VALUE(pov_raw IGNORE NULLS) OVER (PARTITION BY country_code ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2) AS poverty_ratio,
    ROUND(LAST_VALUE(stunt_raw IGNORE NULLS) OVER (PARTITION BY country_code ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2) AS child_stunting_pct,
    ROUND(LAST_VALUE(mpi_raw IGNORE NULLS) OVER (PARTITION BY country_code ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2) AS mpi_score,

    -- 2. BENCHMARKS
    ROUND(world_pov_raw, 2) AS world_poverty_avg,
    ROUND(world_stunt_raw, 2) AS world_stunting_avg,

    -- 3. DATA PROVENANCE
    LAST_VALUE(pov_yr_raw IGNORE NULLS) OVER (PARTITION BY country_code ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS poverty_data_year,
    LAST_VALUE(stunt_yr_raw IGNORE NULLS) OVER (PARTITION BY country_code ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS stunting_data_year,
    LAST_VALUE(mpi_yr_raw IGNORE NULLS) OVER (PARTITION BY country_code ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS mpi_data_year,

    -- 4. ANNUAL METRICS (Rounded)
    ROUND(energy_adequacy, 2) as energy_adequacy,
    ROUND(undernourishment_pct, 2) as undernourishment_pct,
    ROUND(food_insecurity_pct, 2) as food_insecurity_pct,
    ROUND(agri_employment_pct, 2) as agri_employment_pct,

    -- 5. GAP ANALYTICS
    ROUND(LAST_VALUE(stunt_raw IGNORE NULLS) OVER (PARTITION BY country_code ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) - world_stunt_raw, 2) AS stunting_gap_to_world

FROM deduplicated