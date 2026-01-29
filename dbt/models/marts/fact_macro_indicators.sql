-- depends_on: {{ ref('stg_wb_indicators') }}
-- depends_on: {{ ref('stg_hdr_indicators') }}
-- depends_on: {{ ref('stg_imf_indicators') }}
-- depends_on: {{ ref('dim_country') }}

{{
  config(
    materialized='table',
    schema='marts'
  )
}}

WITH wb AS (
    SELECT * FROM {{ ref('stg_wb_indicators') }}
),

hdr AS (
    SELECT * FROM {{ ref('stg_hdr_indicators') }}
),

imf AS (
    SELECT * FROM {{ ref('stg_imf_indicators') }}
),

all_country_years AS (
    SELECT DISTINCT country_code, year
    FROM (
        SELECT country_code, year FROM wb
        UNION ALL
        SELECT country_code, year FROM hdr
        UNION ALL
        SELECT country_code, year FROM imf
    )
)

SELECT
    spine.country_code,
    dim.country_name,
    spine.year,
    
    -- World Bank indicators (28 total)
    wb.wb_gdp_constant_2015_usd,
    wb.wb_gdp_ppp_current_intl,
    wb.wb_gdp_per_capita_ppp,
    wb.wb_gdp_per_capita_current_usd,
    wb.wb_gdp_growth_pct,
    wb.wb_inflation_consumer_pct,
    wb.wb_real_interest_rate_pct,
    wb.wb_gross_capital_formation_pct_gdp,
    wb.wb_agriculture_value_added_pct_gdp,
    wb.wb_industry_value_added_pct_gdp,
    wb.wb_services_value_added_pct_gdp,
    wb.wb_exports_pct_gdp,
    wb.wb_imports_pct_gdp,
    wb.wb_current_account_balance_pct_gdp,
    wb.wb_trade_pct_gdp,
    wb.wb_total_reserves_current_usd,
    wb.wb_tax_revenue_pct_gdp,
    wb.wb_domestic_credit_private_pct_gdp,
    wb.wb_population_total,
    wb.wb_unemployment_pct,
    wb.wb_gdp_per_person_employed,
    wb.wb_labor_force_participation_pct,
    wb.wb_gini_index,
    wb.wb_poverty_headcount_pct,
    wb.wb_school_enrollment_secondary_pct,
    wb.wb_education_expenditure_pct_gdp,
    wb.wb_health_expenditure_current_pct_gdp,
    wb.wb_health_expenditure_govt_pct,
    
    -- HDR indicators (23 total)
    hdr.hdr_human_development_index,
    hdr.hdr_gender_inequality_index,
    hdr.hdr_gender_development_index,
    hdr.hdr_multidimensional_poverty_index,
    hdr.hdr_inequality_coefficient,
    hdr.hdr_inequality_income,
    hdr.hdr_inequality_education,
    hdr.hdr_inequality_life_expectancy,
    hdr.hdr_mean_years_schooling,
    hdr.hdr_expected_years_schooling,
    hdr.hdr_life_expectancy_years,
    hdr.hdr_labor_force_male_pct,
    hdr.hdr_labor_force_female_pct,
    hdr.hdr_mpi_nutrition_pct,
    hdr.hdr_mpi_child_mortality_pct,
    hdr.hdr_mpi_years_schooling_pct,
    hdr.hdr_mpi_school_attendance_pct,
    hdr.hdr_mpi_cooking_fuel_pct,
    hdr.hdr_mpi_sanitation_pct,
    hdr.hdr_mpi_drinking_water_pct,
    hdr.hdr_mpi_electricity_pct,
    hdr.hdr_mpi_housing_pct,
    hdr.hdr_mpi_assets_pct,
    
    -- IMF indicators (5 total)
    imf.imf_public_debt_pct_gdp,
    imf.imf_primary_balance_pct_gdp,
    imf.imf_current_account_balance,
    imf.imf_ppp_conversion_rate,
    imf.imf_financial_development_index

FROM all_country_years spine
LEFT JOIN {{ ref('dim_country') }} dim
    ON spine.country_code = dim.country_code
LEFT JOIN wb
    ON spine.country_code = wb.country_code
    AND spine.year = wb.year
LEFT JOIN hdr
    ON spine.country_code = hdr.country_code
    AND spine.year = hdr.year
LEFT JOIN imf
    ON spine.country_code = imf.country_code
    AND spine.year = imf.year
