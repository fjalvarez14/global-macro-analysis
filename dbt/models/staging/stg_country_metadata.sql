{{
  config(
    materialized='view',
    schema='staging'
  )
}}

SELECT
    country_name,
    iso3 AS country_code,
    iso2,
    continent,
    region,
    income_group,
    EU AS is_eu,
    OECD AS is_oecd,
    ASEAN AS is_asean,
    BRICS AS is_brics,
    G20 AS is_g20,
    FCS AS is_fcs
FROM {{ source('raw', 'country_metadata') }}
