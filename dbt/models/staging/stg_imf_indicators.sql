{{
  config(
    materialized='view',
    schema='staging'
  )
}}

WITH weo AS (
    SELECT
        w.country_code,
        cm.country_name,
        CAST(w.year AS INTEGER) AS year,
        w.GGXWDG_NGDP AS imf_public_debt_pct_gdp,
        w.GGXCNL_NGDP AS imf_primary_balance_pct_gdp,
        w.BCA AS imf_current_account_balance,
        w.PPPEX AS imf_ppp_conversion_rate
    FROM {{ source('raw', 'imf_weo_indicators') }} w
    INNER JOIN {{ source('raw', 'country_metadata') }} cm
        ON w.country_code = cm.iso3
),

fdi AS (
    SELECT
        cm.iso3 AS country_code,  -- Convert ISO2 to ISO3 using country_metadata
        cm.country_name,
        f.year,
        f.FD_FD_IX AS imf_financial_development_index
    FROM {{ source('raw', 'imf_fdi_indicator') }} f
    INNER JOIN {{ source('raw', 'country_metadata') }} cm
        ON f.country_code = cm.iso2
),

-- Create a complete spine of all country-year combinations from both sources
all_country_years AS (
    SELECT DISTINCT country_code, country_name, year FROM weo
    UNION
    SELECT DISTINCT country_code, country_name, year FROM fdi
)

SELECT
    spine.country_code,
    spine.country_name,
    spine.year,
    weo.imf_public_debt_pct_gdp,
    weo.imf_primary_balance_pct_gdp,
    weo.imf_current_account_balance,
    weo.imf_ppp_conversion_rate,
    fdi.imf_financial_development_index
FROM all_country_years spine
LEFT JOIN weo
    ON spine.country_code = weo.country_code
    AND spine.year = weo.year
LEFT JOIN fdi
    ON spine.country_code = fdi.country_code
    AND spine.year = fdi.year
