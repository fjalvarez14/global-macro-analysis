{{
  config(
    materialized='view',
    schema='staging'
  )
}}

SELECT
    country_code,
    Country AS country_name,
    year,
    -- GDP metrics
    NY_GDP_MKTP_KD AS wb_gdp_constant_2015_usd,
    NY_GDP_MKTP_PP_CD AS wb_gdp_ppp_current_intl,
    NY_GDP_PCAP_PP_CD AS wb_gdp_per_capita_ppp,
    NY_GDP_PCAP_CD AS wb_gdp_per_capita_current_usd,
    NY_GDP_MKTP_KD_ZG AS wb_gdp_growth_pct,
    -- Prices
    FP_CPI_TOTL_ZG AS wb_inflation_consumer_pct,
    FR_INR_RINR AS wb_real_interest_rate_pct,
    -- Investment
    NE_GDI_TOTL_ZS AS wb_gross_capital_formation_pct_gdp,
    -- Sectors
    NV_AGR_TOTL_ZS AS wb_agriculture_value_added_pct_gdp,
    NV_IND_TOTL_ZS AS wb_industry_value_added_pct_gdp,
    NV_SRV_TOTL_ZS AS wb_services_value_added_pct_gdp,
    -- Trade
    NE_EXP_GNFS_ZS AS wb_exports_pct_gdp,
    NE_IMP_GNFS_ZS AS wb_imports_pct_gdp,
    BN_CAB_XOKA_GD_ZS AS wb_current_account_balance_pct_gdp,
    NE_TRD_GNFS_ZS AS wb_trade_pct_gdp,
    -- Debt and reserves
    FI_RES_TOTL_CD AS wb_total_reserves_current_usd,
    -- Fiscal
    GC_TAX_TOTL_GD_ZS AS wb_tax_revenue_pct_gdp,
    -- Credit
    FS_AST_PRVT_GD_ZS AS wb_domestic_credit_private_pct_gdp,
    -- Population and labor
    SP_POP_TOTL AS wb_population_total,
    SL_UEM_TOTL_ZS AS wb_unemployment_pct,
    SL_GDP_PCAP_EM_KD AS wb_gdp_per_person_employed,
    SL_TLF_CACT_ZS AS wb_labor_force_participation_pct,
    -- Inequality and poverty
    SI_POV_GINI AS wb_gini_index,
    SI_POV_DDAY AS wb_poverty_headcount_pct,
    -- Social indicators
    SE_SEC_ENRR AS wb_school_enrollment_secondary_pct,
    SE_XPD_TOTL_GD_ZS AS wb_education_expenditure_pct_gdp,
    SH_XPD_CHEX_GD_ZS AS wb_health_expenditure_current_pct_gdp,
    SH_XPD_GHED_CH_ZS AS wb_health_expenditure_govt_pct
FROM {{ source('raw', 'wb_indicators') }} wb
INNER JOIN {{ source('raw', 'country_metadata') }} cm
    ON wb.country_code = cm.iso3