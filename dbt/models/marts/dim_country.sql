-- depends_on: {{ ref('stg_country_metadata') }}

{{
  config(
    materialized='table',
    schema='marts'
  )
}}

SELECT
    country_code,
    country_name,
    iso2,
    continent,
    region,
    income_group,
    is_eu,
    is_oecd,
    is_asean,
    is_brics,
    is_g20,
    is_fcs
FROM {{ ref('stg_country_metadata') }}
