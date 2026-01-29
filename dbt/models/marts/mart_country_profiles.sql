-- depends_on: {{ ref('fact_macro_indicators') }}
-- depends_on: {{ ref('dim_country') }}

{{
  config(
    materialized='table',
    schema='marts'
  )
}}

WITH latest_year AS (
    SELECT MAX(year) AS max_year
    FROM {{ ref('fact_macro_indicators') }}
)

SELECT
    f.country_code,
    f.country_name,
    c.continent,
    c.region,
    c.income_group,
    c.is_eu,
    c.is_oecd,
    c.is_g20,
    f.year,
    f.wb_gdp_constant_2015_usd,
    f.wb_gdp_per_capita_ppp,
    f.wb_gdp_growth_pct,
    f.wb_inflation_consumer_pct,
    f.wb_population_total,
    f.wb_unemployment_pct,
    f.hdr_human_development_index,
    f.hdr_gender_inequality_index,
    f.hdr_life_expectancy_years,
    f.hdr_mean_years_schooling,
    f.imf_public_debt_pct_gdp,
    f.imf_financial_development_index
FROM {{ ref('fact_macro_indicators') }} f
INNER JOIN {{ ref('dim_country') }} c
    ON f.country_code = c.country_code
CROSS JOIN latest_year
WHERE f.year = latest_year.max_year
