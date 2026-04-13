-- fct_bls_observations: BLS time-series observations cleaned and typed
-- Converts string values to numeric, builds proper dates from year+period,
-- maps series IDs to human-readable names, preserves month-over-month changes

{{
    config(
        materialized='table',
        tags=['silver', 'fact']
    )
}}

WITH series_names AS (
    -- Map known series IDs to readable names
    -- This acts as an inline reference table — if you add new series to
    -- conf/ingestion.yml, add the mapping here too
    SELECT col0 AS series_id, col1 AS series_name FROM (VALUES
        ('LNS14000000',               'unemployment rate'),
        ('CES0000000001',             'total nonfarm employment'),
        ('CES6500000001',             'tech sector employment'),
        ('LNS11300060',               'labor force participation 25-54'),
        ('LNS12300060',               'employment-population ratio 25-54'),
        ('JTS000000000000000JOL',     'jolts job openings'),
        ('JTS000000000000000HIR',     'jolts hires'),
        ('JTS000000000000000QUL',     'jolts quits'),
        ('JTS000000000000000LDL',     'jolts layoffs and discharges'),
        ('JTS000000000000000TSL',     'jolts total separations')
    )
),

raw_observations AS (
    SELECT
        TRIM("seriesID")                            AS series_id,
        "year"                                      AS obs_year,
        "period"                                    AS period_code,
        TRIM("periodName")                          AS period_name,
        TRY_CAST("value" AS DOUBLE)                 AS value,
        "latest"                                    AS is_latest,

        -- Build observation_date: M01..M12 → proper date, M13 = annual avg
        CASE
            WHEN "period" NOT IN ('M13', 'S01', 'S02', 'Q01', 'Q02', 'Q03', 'Q04', 'Q05')
            THEN TRY_CAST(
                "year" || '-' || LPAD(REPLACE("period", 'M', ''), 2, '0') || '-01'
                AS DATE
            )
            ELSE NULL
        END                                         AS observation_date,

        "period" = 'M13'                            AS is_annual_average,

        -- Month-over-month and year-over-year changes
        TRY_CAST("calculations.net_changes.1" AS DOUBLE)  AS net_change_1m,
        TRY_CAST("calculations.net_changes.12" AS DOUBLE) AS net_change_12m,
        TRY_CAST("calculations.pct_changes.1" AS DOUBLE)  AS pct_change_1m,
        TRY_CAST("calculations.pct_changes.12" AS DOUBLE) AS pct_change_12m

    FROM {{ ref("stg_bls__series") }}
    WHERE "value" IS NOT NULL
      AND TRIM("value") != ''
)

SELECT
    r.series_id,
    COALESCE(sn.series_name, r.series_id)           AS series_name,
    r.observation_date,
    r.obs_year,
    r.period_code,
    r.period_name,
    r.value,
    r.is_annual_average,
    r.net_change_1m,
    r.net_change_12m,
    r.pct_change_1m,
    r.pct_change_12m
FROM raw_observations AS r
LEFT JOIN series_names AS sn
    USING(series_id)
WHERE r.value IS NOT NULL
