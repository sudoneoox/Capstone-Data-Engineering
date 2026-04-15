-- gold_economic_dashboard: combined BLS + FRED economic indicators
-- Grain: report_month × series_name
-- Dashboard: "Market Pulse" — trend lines overlaid on posting volume


WITH bls_monthly AS (
    SELECT
        DATE_TRUNC('month', observation_date)       AS report_month,
        series_id,
        series_name,
        'bls'                                       AS source_system,
        -- BLS is already monthly, take the single value per month
        MAX(value)                                  AS value
    FROM {{ ref("fct_bls_observations") }}
    WHERE observation_date IS NOT NULL
      AND is_annual_average = FALSE
    GROUP BY 1, 2, 3
),

fred_monthly AS (
    SELECT
        DATE_TRUNC('month', observation_date)       AS report_month,
        series_id,
        series_name,
        'fred'                                      AS source_system,
        -- FRED daily series (DFF, ICSA) need monthly averaging
        AVG(value)                                  AS value
    FROM {{ ref("fct_fred_indicators") }}
    WHERE observation_date IS NOT NULL
    GROUP BY 1, 2, 3
),

-- Deduplicate overlapping series: UNRATE and JTSJOL appear in both BLS and FRED
-- Prefer BLS as primary source since it's the origin
dedup_fred AS (
    SELECT f.*
    FROM fred_monthly AS f
    LEFT JOIN bls_monthly AS b
        ON f.report_month = b.report_month
       AND f.series_id = b.series_id
    WHERE b.series_id IS NULL
),

combined AS (
    SELECT * FROM bls_monthly
    UNION ALL
    SELECT * FROM dedup_fred
),

with_category AS (
    SELECT
        *,
        CASE
            WHEN series_name IN (
                'unemployment rate',
                'total nonfarm employment',
                'tech sector employment',
                'total nonfarm payrolls',
                'employment-population ratio',
                'prime-age employment-population ratio',
                'labor force participation rate',
                'labor force participation 25-54',
                'employment-population ratio 25-54'
            ) THEN 'employment'

            WHEN series_name IN (
                'jolts job openings',
                'jolts hires',
                'jolts quits',
                'jolts layoffs and discharges',
                'jolts total separations',
                'initial jobless claims'
            ) THEN 'labor_demand'

            WHEN series_name IN (
                'consumer price index',
                'pce price index',
                'federal funds rate',
                'effective federal funds rate'
            ) THEN 'inflation'

            WHEN series_name IN (
                'nonfarm business labor productivity'
            ) THEN 'productivity'

            WHEN series_name IN (
                'retail sales ex food services'
            ) THEN 'consumer'

            WHEN series_name IN (
                'unemployed less than 5 weeks',
                'unemployed 27 weeks and over'
            ) THEN 'unemployment_duration'

            ELSE 'other'
        END                                         AS category
    FROM combined
),

with_yoy AS (
    SELECT
        *,
        LAG(value, 12) OVER (
            PARTITION BY series_id ORDER BY report_month
        )                                           AS value_12m_ago
    FROM with_category
)

SELECT
    report_month,
    series_id,
    series_name,
    source_system,
    category,
    ROUND(value, 4)                                 AS value,
    ROUND(value_12m_ago, 4)                         AS value_12m_ago,
    ROUND(value - value_12m_ago, 4)                 AS yoy_change,
    ROUND(
        (value - value_12m_ago) / NULLIF(ABS(value_12m_ago), 0),
        4
    )                                               AS yoy_change_pct,
    EXTRACT(YEAR FROM report_month)                 AS year,
    EXTRACT(QUARTER FROM report_month)              AS quarter
FROM with_yoy
ORDER BY series_name, report_month
