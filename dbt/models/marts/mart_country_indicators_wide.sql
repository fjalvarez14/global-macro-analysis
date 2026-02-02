{{
  config(
    materialized='table',
    schema='marts',
    description='Wide format table with latest available value and year for each indicator per country'
  )
}}

WITH latest_values AS (
  SELECT 
    country_code,
    country_name,
    year,
    -- World Bank indicators
    wb_gdp_constant_2015_usd,
    wb_gdp_ppp_current_intl,
    wb_gdp_per_capita_ppp,
    wb_gdp_per_capita_current_usd,
    wb_gdp_growth_pct,
    wb_inflation_consumer_pct,
    wb_real_interest_rate_pct,
    wb_gross_capital_formation_pct_gdp,
    wb_agriculture_value_added_pct_gdp,
    wb_industry_value_added_pct_gdp,
    wb_services_value_added_pct_gdp,
    wb_exports_pct_gdp,
    wb_imports_pct_gdp,
    wb_current_account_balance_pct_gdp,
    wb_trade_pct_gdp,
    wb_total_reserves_current_usd,
    wb_tax_revenue_pct_gdp,
    wb_domestic_credit_private_pct_gdp,
    wb_population_total,
    wb_unemployment_pct,
    wb_gdp_per_person_employed,
    wb_labor_force_participation_pct,
    wb_gini_index,
    wb_poverty_headcount_pct,
    wb_school_enrollment_secondary_pct,
    wb_education_expenditure_pct_gdp,
    wb_health_expenditure_current_pct_gdp,
    wb_health_expenditure_govt_pct,
    -- HDR indicators
    hdr_human_development_index,
    hdr_gender_inequality_index,
    hdr_gender_development_index,
    hdr_multidimensional_poverty_index,
    hdr_inequality_coefficient,
    hdr_inequality_income,
    hdr_inequality_education,
    hdr_inequality_life_expectancy,
    hdr_mean_years_schooling,
    hdr_expected_years_schooling,
    hdr_life_expectancy_years,
    hdr_labor_force_male_pct,
    hdr_labor_force_female_pct,
    hdr_mpi_nutrition_pct,
    hdr_mpi_child_mortality_pct,
    hdr_mpi_years_schooling_pct,
    hdr_mpi_school_attendance_pct,
    hdr_mpi_cooking_fuel_pct,
    hdr_mpi_sanitation_pct,
    hdr_mpi_drinking_water_pct,
    hdr_mpi_electricity_pct,
    hdr_mpi_housing_pct,
    hdr_mpi_assets_pct,
    -- IMF indicators
    imf_public_debt_pct_gdp,
    imf_primary_balance_pct_gdp,
    imf_current_account_balance,
    imf_ppp_conversion_rate,
    imf_financial_development_index
  FROM {{ ref('fact_macro_indicators') }}
),

country_base AS (
  SELECT DISTINCT
    country_code,
    country_name
  FROM {{ ref('dim_country') }}
)

SELECT 
  cb.country_code,
  cb.country_name,
  
  -- World Bank indicators with year
  MAX(CASE WHEN lv.wb_gdp_constant_2015_usd IS NOT NULL THEN lv.wb_gdp_constant_2015_usd END) AS wb_gdp_constant_2015_usd,
  MAX(CASE WHEN lv.wb_gdp_constant_2015_usd IS NOT NULL THEN lv.year END) AS wb_gdp_constant_2015_usd_year,
  
  MAX(CASE WHEN lv.wb_gdp_ppp_current_intl IS NOT NULL THEN lv.wb_gdp_ppp_current_intl END) AS wb_gdp_ppp_current_intl,
  MAX(CASE WHEN lv.wb_gdp_ppp_current_intl IS NOT NULL THEN lv.year END) AS wb_gdp_ppp_current_intl_year,
  
  MAX(CASE WHEN lv.wb_gdp_per_capita_ppp IS NOT NULL THEN lv.wb_gdp_per_capita_ppp END) AS wb_gdp_per_capita_ppp,
  MAX(CASE WHEN lv.wb_gdp_per_capita_ppp IS NOT NULL THEN lv.year END) AS wb_gdp_per_capita_ppp_year,
  
  MAX(CASE WHEN lv.wb_gdp_per_capita_current_usd IS NOT NULL THEN lv.wb_gdp_per_capita_current_usd END) AS wb_gdp_per_capita_current_usd,
  MAX(CASE WHEN lv.wb_gdp_per_capita_current_usd IS NOT NULL THEN lv.year END) AS wb_gdp_per_capita_current_usd_year,
  
  MAX(CASE WHEN lv.wb_gdp_growth_pct IS NOT NULL THEN lv.wb_gdp_growth_pct END) AS wb_gdp_growth_pct,
  MAX(CASE WHEN lv.wb_gdp_growth_pct IS NOT NULL THEN lv.year END) AS wb_gdp_growth_pct_year,
  
  MAX(CASE WHEN lv.wb_inflation_consumer_pct IS NOT NULL THEN lv.wb_inflation_consumer_pct END) AS wb_inflation_consumer_pct,
  MAX(CASE WHEN lv.wb_inflation_consumer_pct IS NOT NULL THEN lv.year END) AS wb_inflation_consumer_pct_year,
  
  MAX(CASE WHEN lv.wb_real_interest_rate_pct IS NOT NULL THEN lv.wb_real_interest_rate_pct END) AS wb_real_interest_rate_pct,
  MAX(CASE WHEN lv.wb_real_interest_rate_pct IS NOT NULL THEN lv.year END) AS wb_real_interest_rate_pct_year,
  
  MAX(CASE WHEN lv.wb_gross_capital_formation_pct_gdp IS NOT NULL THEN lv.wb_gross_capital_formation_pct_gdp END) AS wb_gross_capital_formation_pct_gdp,
  MAX(CASE WHEN lv.wb_gross_capital_formation_pct_gdp IS NOT NULL THEN lv.year END) AS wb_gross_capital_formation_pct_gdp_year,
  
  MAX(CASE WHEN lv.wb_agriculture_value_added_pct_gdp IS NOT NULL THEN lv.wb_agriculture_value_added_pct_gdp END) AS wb_agriculture_value_added_pct_gdp,
  MAX(CASE WHEN lv.wb_agriculture_value_added_pct_gdp IS NOT NULL THEN lv.year END) AS wb_agriculture_value_added_pct_gdp_year,
  
  MAX(CASE WHEN lv.wb_industry_value_added_pct_gdp IS NOT NULL THEN lv.wb_industry_value_added_pct_gdp END) AS wb_industry_value_added_pct_gdp,
  MAX(CASE WHEN lv.wb_industry_value_added_pct_gdp IS NOT NULL THEN lv.year END) AS wb_industry_value_added_pct_gdp_year,
  
  MAX(CASE WHEN lv.wb_services_value_added_pct_gdp IS NOT NULL THEN lv.wb_services_value_added_pct_gdp END) AS wb_services_value_added_pct_gdp,
  MAX(CASE WHEN lv.wb_services_value_added_pct_gdp IS NOT NULL THEN lv.year END) AS wb_services_value_added_pct_gdp_year,
  
  MAX(CASE WHEN lv.wb_exports_pct_gdp IS NOT NULL THEN lv.wb_exports_pct_gdp END) AS wb_exports_pct_gdp,
  MAX(CASE WHEN lv.wb_exports_pct_gdp IS NOT NULL THEN lv.year END) AS wb_exports_pct_gdp_year,
  
  MAX(CASE WHEN lv.wb_imports_pct_gdp IS NOT NULL THEN lv.wb_imports_pct_gdp END) AS wb_imports_pct_gdp,
  MAX(CASE WHEN lv.wb_imports_pct_gdp IS NOT NULL THEN lv.year END) AS wb_imports_pct_gdp_year,
  
  MAX(CASE WHEN lv.wb_current_account_balance_pct_gdp IS NOT NULL THEN lv.wb_current_account_balance_pct_gdp END) AS wb_current_account_balance_pct_gdp,
  MAX(CASE WHEN lv.wb_current_account_balance_pct_gdp IS NOT NULL THEN lv.year END) AS wb_current_account_balance_pct_gdp_year,
  
  MAX(CASE WHEN lv.wb_trade_pct_gdp IS NOT NULL THEN lv.wb_trade_pct_gdp END) AS wb_trade_pct_gdp,
  MAX(CASE WHEN lv.wb_trade_pct_gdp IS NOT NULL THEN lv.year END) AS wb_trade_pct_gdp_year,
  
  MAX(CASE WHEN lv.wb_total_reserves_current_usd IS NOT NULL THEN lv.wb_total_reserves_current_usd END) AS wb_total_reserves_current_usd,
  MAX(CASE WHEN lv.wb_total_reserves_current_usd IS NOT NULL THEN lv.year END) AS wb_total_reserves_current_usd_year,
  
  MAX(CASE WHEN lv.wb_tax_revenue_pct_gdp IS NOT NULL THEN lv.wb_tax_revenue_pct_gdp END) AS wb_tax_revenue_pct_gdp,
  MAX(CASE WHEN lv.wb_tax_revenue_pct_gdp IS NOT NULL THEN lv.year END) AS wb_tax_revenue_pct_gdp_year,
  
  MAX(CASE WHEN lv.wb_domestic_credit_private_pct_gdp IS NOT NULL THEN lv.wb_domestic_credit_private_pct_gdp END) AS wb_domestic_credit_private_pct_gdp,
  MAX(CASE WHEN lv.wb_domestic_credit_private_pct_gdp IS NOT NULL THEN lv.year END) AS wb_domestic_credit_private_pct_gdp_year,
  
  MAX(CASE WHEN lv.wb_population_total IS NOT NULL THEN lv.wb_population_total END) AS wb_population_total,
  MAX(CASE WHEN lv.wb_population_total IS NOT NULL THEN lv.year END) AS wb_population_total_year,
  
  MAX(CASE WHEN lv.wb_unemployment_pct IS NOT NULL THEN lv.wb_unemployment_pct END) AS wb_unemployment_pct,
  MAX(CASE WHEN lv.wb_unemployment_pct IS NOT NULL THEN lv.year END) AS wb_unemployment_pct_year,
  
  MAX(CASE WHEN lv.wb_gdp_per_person_employed IS NOT NULL THEN lv.wb_gdp_per_person_employed END) AS wb_gdp_per_person_employed,
  MAX(CASE WHEN lv.wb_gdp_per_person_employed IS NOT NULL THEN lv.year END) AS wb_gdp_per_person_employed_year,
  
  MAX(CASE WHEN lv.wb_labor_force_participation_pct IS NOT NULL THEN lv.wb_labor_force_participation_pct END) AS wb_labor_force_participation_pct,
  MAX(CASE WHEN lv.wb_labor_force_participation_pct IS NOT NULL THEN lv.year END) AS wb_labor_force_participation_pct_year,
  
  MAX(CASE WHEN lv.wb_gini_index IS NOT NULL THEN lv.wb_gini_index END) AS wb_gini_index,
  MAX(CASE WHEN lv.wb_gini_index IS NOT NULL THEN lv.year END) AS wb_gini_index_year,
  
  MAX(CASE WHEN lv.wb_poverty_headcount_pct IS NOT NULL THEN lv.wb_poverty_headcount_pct END) AS wb_poverty_headcount_pct,
  MAX(CASE WHEN lv.wb_poverty_headcount_pct IS NOT NULL THEN lv.year END) AS wb_poverty_headcount_pct_year,
  
  MAX(CASE WHEN lv.wb_school_enrollment_secondary_pct IS NOT NULL THEN lv.wb_school_enrollment_secondary_pct END) AS wb_school_enrollment_secondary_pct,
  MAX(CASE WHEN lv.wb_school_enrollment_secondary_pct IS NOT NULL THEN lv.year END) AS wb_school_enrollment_secondary_pct_year,
  
  MAX(CASE WHEN lv.wb_education_expenditure_pct_gdp IS NOT NULL THEN lv.wb_education_expenditure_pct_gdp END) AS wb_education_expenditure_pct_gdp,
  MAX(CASE WHEN lv.wb_education_expenditure_pct_gdp IS NOT NULL THEN lv.year END) AS wb_education_expenditure_pct_gdp_year,
  
  MAX(CASE WHEN lv.wb_health_expenditure_current_pct_gdp IS NOT NULL THEN lv.wb_health_expenditure_current_pct_gdp END) AS wb_health_expenditure_current_pct_gdp,
  MAX(CASE WHEN lv.wb_health_expenditure_current_pct_gdp IS NOT NULL THEN lv.year END) AS wb_health_expenditure_current_pct_gdp_year,
  
  MAX(CASE WHEN lv.wb_health_expenditure_govt_pct IS NOT NULL THEN lv.wb_health_expenditure_govt_pct END) AS wb_health_expenditure_govt_pct,
  MAX(CASE WHEN lv.wb_health_expenditure_govt_pct IS NOT NULL THEN lv.year END) AS wb_health_expenditure_govt_pct_year,
  
  -- HDR indicators with year
  MAX(CASE WHEN lv.hdr_human_development_index IS NOT NULL THEN lv.hdr_human_development_index END) AS hdr_human_development_index,
  MAX(CASE WHEN lv.hdr_human_development_index IS NOT NULL THEN lv.year END) AS hdr_human_development_index_year,
  
  MAX(CASE WHEN lv.hdr_gender_inequality_index IS NOT NULL THEN lv.hdr_gender_inequality_index END) AS hdr_gender_inequality_index,
  MAX(CASE WHEN lv.hdr_gender_inequality_index IS NOT NULL THEN lv.year END) AS hdr_gender_inequality_index_year,
  
  MAX(CASE WHEN lv.hdr_gender_development_index IS NOT NULL THEN lv.hdr_gender_development_index END) AS hdr_gender_development_index,
  MAX(CASE WHEN lv.hdr_gender_development_index IS NOT NULL THEN lv.year END) AS hdr_gender_development_index_year,
  
  MAX(CASE WHEN lv.hdr_multidimensional_poverty_index IS NOT NULL THEN lv.hdr_multidimensional_poverty_index END) AS hdr_multidimensional_poverty_index,
  MAX(CASE WHEN lv.hdr_multidimensional_poverty_index IS NOT NULL THEN lv.year END) AS hdr_multidimensional_poverty_index_year,
  
  MAX(CASE WHEN lv.hdr_inequality_coefficient IS NOT NULL THEN lv.hdr_inequality_coefficient END) AS hdr_inequality_coefficient,
  MAX(CASE WHEN lv.hdr_inequality_coefficient IS NOT NULL THEN lv.year END) AS hdr_inequality_coefficient_year,
  
  MAX(CASE WHEN lv.hdr_inequality_income IS NOT NULL THEN lv.hdr_inequality_income END) AS hdr_inequality_income,
  MAX(CASE WHEN lv.hdr_inequality_income IS NOT NULL THEN lv.year END) AS hdr_inequality_income_year,
  
  MAX(CASE WHEN lv.hdr_inequality_education IS NOT NULL THEN lv.hdr_inequality_education END) AS hdr_inequality_education,
  MAX(CASE WHEN lv.hdr_inequality_education IS NOT NULL THEN lv.year END) AS hdr_inequality_education_year,
  
  MAX(CASE WHEN lv.hdr_inequality_life_expectancy IS NOT NULL THEN lv.hdr_inequality_life_expectancy END) AS hdr_inequality_life_expectancy,
  MAX(CASE WHEN lv.hdr_inequality_life_expectancy IS NOT NULL THEN lv.year END) AS hdr_inequality_life_expectancy_year,
  
  MAX(CASE WHEN lv.hdr_mean_years_schooling IS NOT NULL THEN lv.hdr_mean_years_schooling END) AS hdr_mean_years_schooling,
  MAX(CASE WHEN lv.hdr_mean_years_schooling IS NOT NULL THEN lv.year END) AS hdr_mean_years_schooling_year,
  
  MAX(CASE WHEN lv.hdr_expected_years_schooling IS NOT NULL THEN lv.hdr_expected_years_schooling END) AS hdr_expected_years_schooling,
  MAX(CASE WHEN lv.hdr_expected_years_schooling IS NOT NULL THEN lv.year END) AS hdr_expected_years_schooling_year,
  
  MAX(CASE WHEN lv.hdr_life_expectancy_years IS NOT NULL THEN lv.hdr_life_expectancy_years END) AS hdr_life_expectancy_years,
  MAX(CASE WHEN lv.hdr_life_expectancy_years IS NOT NULL THEN lv.year END) AS hdr_life_expectancy_years_year,
  
  MAX(CASE WHEN lv.hdr_labor_force_male_pct IS NOT NULL THEN lv.hdr_labor_force_male_pct END) AS hdr_labor_force_male_pct,
  MAX(CASE WHEN lv.hdr_labor_force_male_pct IS NOT NULL THEN lv.year END) AS hdr_labor_force_male_pct_year,
  
  MAX(CASE WHEN lv.hdr_labor_force_female_pct IS NOT NULL THEN lv.hdr_labor_force_female_pct END) AS hdr_labor_force_female_pct,
  MAX(CASE WHEN lv.hdr_labor_force_female_pct IS NOT NULL THEN lv.year END) AS hdr_labor_force_female_pct_year,
  
  MAX(CASE WHEN lv.hdr_mpi_nutrition_pct IS NOT NULL THEN lv.hdr_mpi_nutrition_pct END) AS hdr_mpi_nutrition_pct,
  MAX(CASE WHEN lv.hdr_mpi_nutrition_pct IS NOT NULL THEN lv.year END) AS hdr_mpi_nutrition_pct_year,
  
  MAX(CASE WHEN lv.hdr_mpi_child_mortality_pct IS NOT NULL THEN lv.hdr_mpi_child_mortality_pct END) AS hdr_mpi_child_mortality_pct,
  MAX(CASE WHEN lv.hdr_mpi_child_mortality_pct IS NOT NULL THEN lv.year END) AS hdr_mpi_child_mortality_pct_year,
  
  MAX(CASE WHEN lv.hdr_mpi_years_schooling_pct IS NOT NULL THEN lv.hdr_mpi_years_schooling_pct END) AS hdr_mpi_years_schooling_pct,
  MAX(CASE WHEN lv.hdr_mpi_years_schooling_pct IS NOT NULL THEN lv.year END) AS hdr_mpi_years_schooling_pct_year,
  
  MAX(CASE WHEN lv.hdr_mpi_school_attendance_pct IS NOT NULL THEN lv.hdr_mpi_school_attendance_pct END) AS hdr_mpi_school_attendance_pct,
  MAX(CASE WHEN lv.hdr_mpi_school_attendance_pct IS NOT NULL THEN lv.year END) AS hdr_mpi_school_attendance_pct_year,
  
  MAX(CASE WHEN lv.hdr_mpi_cooking_fuel_pct IS NOT NULL THEN lv.hdr_mpi_cooking_fuel_pct END) AS hdr_mpi_cooking_fuel_pct,
  MAX(CASE WHEN lv.hdr_mpi_cooking_fuel_pct IS NOT NULL THEN lv.year END) AS hdr_mpi_cooking_fuel_pct_year,
  
  MAX(CASE WHEN lv.hdr_mpi_sanitation_pct IS NOT NULL THEN lv.hdr_mpi_sanitation_pct END) AS hdr_mpi_sanitation_pct,
  MAX(CASE WHEN lv.hdr_mpi_sanitation_pct IS NOT NULL THEN lv.year END) AS hdr_mpi_sanitation_pct_year,
  
  MAX(CASE WHEN lv.hdr_mpi_drinking_water_pct IS NOT NULL THEN lv.hdr_mpi_drinking_water_pct END) AS hdr_mpi_drinking_water_pct,
  MAX(CASE WHEN lv.hdr_mpi_drinking_water_pct IS NOT NULL THEN lv.year END) AS hdr_mpi_drinking_water_pct_year,
  
  MAX(CASE WHEN lv.hdr_mpi_electricity_pct IS NOT NULL THEN lv.hdr_mpi_electricity_pct END) AS hdr_mpi_electricity_pct,
  MAX(CASE WHEN lv.hdr_mpi_electricity_pct IS NOT NULL THEN lv.year END) AS hdr_mpi_electricity_pct_year,
  
  MAX(CASE WHEN lv.hdr_mpi_housing_pct IS NOT NULL THEN lv.hdr_mpi_housing_pct END) AS hdr_mpi_housing_pct,
  MAX(CASE WHEN lv.hdr_mpi_housing_pct IS NOT NULL THEN lv.year END) AS hdr_mpi_housing_pct_year,
  
  MAX(CASE WHEN lv.hdr_mpi_assets_pct IS NOT NULL THEN lv.hdr_mpi_assets_pct END) AS hdr_mpi_assets_pct,
  MAX(CASE WHEN lv.hdr_mpi_assets_pct IS NOT NULL THEN lv.year END) AS hdr_mpi_assets_pct_year,
  
  -- IMF indicators with year
  MAX(CASE WHEN lv.imf_public_debt_pct_gdp IS NOT NULL THEN lv.imf_public_debt_pct_gdp END) AS imf_public_debt_pct_gdp,
  MAX(CASE WHEN lv.imf_public_debt_pct_gdp IS NOT NULL THEN lv.year END) AS imf_public_debt_pct_gdp_year,
  
  MAX(CASE WHEN lv.imf_primary_balance_pct_gdp IS NOT NULL THEN lv.imf_primary_balance_pct_gdp END) AS imf_primary_balance_pct_gdp,
  MAX(CASE WHEN lv.imf_primary_balance_pct_gdp IS NOT NULL THEN lv.year END) AS imf_primary_balance_pct_gdp_year,
  
  MAX(CASE WHEN lv.imf_current_account_balance IS NOT NULL THEN lv.imf_current_account_balance END) AS imf_current_account_balance,
  MAX(CASE WHEN lv.imf_current_account_balance IS NOT NULL THEN lv.year END) AS imf_current_account_balance_year,
  
  MAX(CASE WHEN lv.imf_ppp_conversion_rate IS NOT NULL THEN lv.imf_ppp_conversion_rate END) AS imf_ppp_conversion_rate,
  MAX(CASE WHEN lv.imf_ppp_conversion_rate IS NOT NULL THEN lv.year END) AS imf_ppp_conversion_rate_year,
  
  MAX(CASE WHEN lv.imf_financial_development_index IS NOT NULL THEN lv.imf_financial_development_index END) AS imf_financial_development_index,
  MAX(CASE WHEN lv.imf_financial_development_index IS NOT NULL THEN lv.year END) AS imf_financial_development_index_year

FROM country_base cb
LEFT JOIN latest_values lv
  ON cb.country_code = lv.country_code

GROUP BY cb.country_code, cb.country_name
ORDER BY cb.country_name
