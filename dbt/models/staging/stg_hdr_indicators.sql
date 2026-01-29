{{
  config(
    materialized='view',
    schema='staging'
  )
}}

SELECT
    countryIsoCode AS country_code,
    country AS country_name,
    year,
    -- Composite indices
    hdi AS hdr_human_development_index,
    gii AS hdr_gender_inequality_index,
    gdi AS hdr_gender_development_index,
    mpi_value AS hdr_multidimensional_poverty_index,
    -- Inequality
    coef_ineq AS hdr_inequality_coefficient,
    ineq_inc AS hdr_inequality_income,
    ineq_edu AS hdr_inequality_education,
    ineq_le AS hdr_inequality_life_expectancy,
    -- Education
    mys AS hdr_mean_years_schooling,
    eys AS hdr_expected_years_schooling,
    -- Health
    le AS hdr_life_expectancy_years,
    -- Labor
    lfpr_m AS hdr_labor_force_male_pct,
    lfpr_f AS hdr_labor_force_female_pct,
    -- MPI components
    nutrition AS hdr_mpi_nutrition_pct,
    child_mortality AS hdr_mpi_child_mortality_pct,
    years_of_schooling AS hdr_mpi_years_schooling_pct,
    school_attendance AS hdr_mpi_school_attendance_pct,
    cooking_fuel AS hdr_mpi_cooking_fuel_pct,
    sanitation AS hdr_mpi_sanitation_pct,
    drinking_water AS hdr_mpi_drinking_water_pct,
    electricity AS hdr_mpi_electricity_pct,
    housing AS hdr_mpi_housing_pct,
    assets AS hdr_mpi_assets_pct
FROM {{ source('raw', 'hdr_indicators') }} hdr
INNER JOIN {{ source('raw', 'country_metadata') }} cm
    ON hdr.countryIsoCode = cm.iso3