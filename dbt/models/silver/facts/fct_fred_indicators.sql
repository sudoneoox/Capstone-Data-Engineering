-- fct_fred_indicators: FRED economic indicators in long format
-- Source data is wide (one column per series, one row per date)
-- This unpivots into: observation_date × series_id × value
-- Maps series IDs to human-readable names

{{
    config(
        materialized='table',
        tags=['silver', 'fact']
    )
}}

{% set fred_series_columns = [
    'UNRATE',
    'JTSJOL',
    'CPIAUCSL',
    'FEDFUNDS',
    'ICSA',
    'RSXFS',
    'PAYEMS',
    'EMRATIO',
    'LNS12300060',
    'CIVPART',
    'OPHNFB',
    'UEMPLT5',
    'UEMP27OV',
    'PCEPI',
    'DFF'
] %}

{% set fred_series_rows = [
    ('UNRATE', 'unemployment rate'),
    ('JTSJOL', 'jolts job openings'),
    ('CPIAUCSL', 'consumer price index'),
    ('FEDFUNDS', 'federal funds rate'),
    ('ICSA', 'initial jobless claims'),
    ('RSXFS', 'retail sales ex food services'),
    ('PAYEMS', 'total nonfarm payrolls'),
    ('EMRATIO', 'employment-population ratio'),
    ('LNS12300060', 'prime-age employment-population ratio'),
    ('CIVPART', 'labor force participation rate'),
    ('OPHNFB', 'nonfarm business labor productivity'),
    ('UEMPLT5', 'unemployed less than 5 weeks'),
    ('UEMP27OV', 'unemployed 27 weeks and over'),
    ('PCEPI', 'pce price index'),
    ('DFF', 'effective federal funds rate')
] %}

WITH series_names AS {{ inline_mapping_2col(fred_series_rows, 'series_id', 'series_name') }},

unpivoted AS (
    {{ unpivot_fred_series(
        relation=ref('stg_fred__series'),
        date_column=col('date'),
        series_columns=fred_series_columns
    ) }}
),

cleaned AS (
    SELECT
        TRY_CAST(date_raw AS DATE) AS observation_date,
        series_id,
        CAST(value AS DOUBLE) AS value
    FROM unpivoted
    WHERE value IS NOT NULL
      AND date_raw IS NOT NULL
)

SELECT
    c.observation_date,
    EXTRACT(YEAR FROM c.observation_date) AS obs_year,
    EXTRACT(MONTH FROM c.observation_date) AS obs_month,
    c.series_id,
    COALESCE(sn.series_name, c.series_id) AS series_name,
    c.value
FROM cleaned AS c
LEFT JOIN series_names AS sn
    USING(series_id)
ORDER BY c.series_id, c.observation_date
