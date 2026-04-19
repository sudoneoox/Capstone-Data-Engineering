-- gold_market_trends: monthly time-series job market metrics
-- Grain: report_month × data_source
-- Dashboard: "Market Pulse" — posting volume trend lines, salary trends
--
-- ONLY includes non-snapshot sources (is_snapshot = FALSE):
--   - kaggle_linkedin: April 2024 window with real date distribution
--   - adzuna: ongoing live collection (accumulates over daily runs)
--
-- Excludes kaggle_linkedin_large and kaggle_ds because all their postings
-- land in a single month (January 2024), making MoM analysis meaningless.
-- For those sources, see gold_market_snapshot.

{{
    config(
        materialized='table',
        tags=['gold']
    )
}}

WITH monthly_postings AS (
    SELECT
        DATE_TRUNC('month', posted_at)              AS report_month,
        data_source,
        COUNT(*)                                    AS total_postings,
        COUNT(DISTINCT company_name)                AS unique_companies,

        -- salary
        COUNT(annual_salary_estimate)               AS postings_with_salary,
        MEDIAN(annual_salary_estimate)              AS median_salary_raw,
        AVG(annual_salary_estimate)                 AS avg_salary_raw,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY annual_salary_estimate)
                                                    AS salary_p25_raw,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY annual_salary_estimate)
                                                    AS salary_p75_raw,

        -- remote
        COUNT(*) FILTER (WHERE is_remote = TRUE)    AS remote_postings,

        -- SOC matching
        COUNT(*) FILTER (WHERE soc_match_type = 'exact_match')
                                                    AS soc_matched_postings

    FROM {{ ref("fct_job_postings") }}
    WHERE posted_at IS NOT NULL
      AND is_snapshot = FALSE
    GROUP BY 1, 2
),

with_calculations AS (
    SELECT
        report_month,
        data_source,
        total_postings,
        unique_companies,
        postings_with_salary,

        -- only show salary when we have enough data points
        CASE WHEN postings_with_salary >= 5
            THEN CAST(ROUND(median_salary_raw) AS INTEGER)
        END                                         AS median_salary,
        CASE WHEN postings_with_salary >= 5
            THEN CAST(ROUND(avg_salary_raw) AS INTEGER)
        END                                         AS avg_salary,
        CASE WHEN postings_with_salary >= 5
            THEN CAST(ROUND(salary_p25_raw) AS INTEGER)
        END                                         AS salary_p25,
        CASE WHEN postings_with_salary >= 5
            THEN CAST(ROUND(salary_p75_raw) AS INTEGER)
        END                                         AS salary_p75,

        remote_postings,
        ROUND(remote_postings * 1.0 / NULLIF(total_postings, 0), 4)
                                                    AS pct_remote,
        ROUND(soc_matched_postings * 1.0 / NULLIF(total_postings, 0), 4)
                                                    AS soc_match_rate,

        -- MoM changes
        LAG(total_postings) OVER (
            PARTITION BY data_source ORDER BY report_month
        )                                           AS prev_month_postings,

        LAG(median_salary_raw) OVER (
            PARTITION BY data_source ORDER BY report_month
        )                                           AS prev_month_median_salary

    FROM monthly_postings
    -- filter out months with negligible data
    WHERE total_postings >= 5
)

SELECT
    report_month,
    data_source,
    total_postings,
    unique_companies,
    postings_with_salary,
    median_salary,
    avg_salary,
    salary_p25,
    salary_p75,
    remote_postings,
    pct_remote,
    soc_match_rate,

    -- posting volume MoM
    total_postings - prev_month_postings             AS postings_mom_change,
    ROUND(
        (total_postings - prev_month_postings) * 1.0
        / NULLIF(prev_month_postings, 0),
        4
    )                                               AS postings_mom_change_pct,

    EXTRACT(YEAR FROM report_month)                 AS year,
    EXTRACT(QUARTER FROM report_month)              AS quarter,
    EXTRACT(MONTH FROM report_month)                AS month_num

FROM with_calculations
ORDER BY data_source, report_month
