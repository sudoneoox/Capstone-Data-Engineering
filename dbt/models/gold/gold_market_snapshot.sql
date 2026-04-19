-- gold_market_snapshot: cross-sectional job market summary per source
-- Grain: one row per data_source
-- Dashboard: "Market Pulse" — KPI cards showing totals by source
--
-- This model answers: "what does each data source tell us about the market?"
-- It does NOT try to show trends over time — snapshot sources (kaggle_large, kaggle_ds)
-- have all their postings in a single month, so time-series analysis is meaningless for them.
-- See gold_market_trends for actual time-series data from non-snapshot sources.

{{
    config(
        materialized='table',
        tags=['gold']
    )
}}

WITH source_stats AS (
    SELECT
        data_source,
        is_snapshot,
        COUNT(*)                                    AS total_postings,
        COUNT(DISTINCT company_name)                AS unique_companies,

        -- time coverage
        MIN(posted_at)                              AS earliest_posting,
        MAX(posted_at)                              AS latest_posting,
        COUNT(DISTINCT DATE_TRUNC('month', posted_at))
                                                    AS distinct_months,

        -- salary
        COUNT(annual_salary_estimate)               AS postings_with_salary,
        CAST(ROUND(MEDIAN(annual_salary_estimate)) AS INTEGER)
                                                    AS median_salary,
        CAST(ROUND(AVG(annual_salary_estimate)) AS INTEGER)
                                                    AS avg_salary,
        CAST(ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY annual_salary_estimate)) AS INTEGER)
                                                    AS salary_p25,
        CAST(ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY annual_salary_estimate)) AS INTEGER)
                                                    AS salary_p75,

        -- remote
        COUNT(*) FILTER (WHERE is_remote = TRUE)    AS remote_postings,
        ROUND(
            COUNT(*) FILTER (WHERE is_remote = TRUE) * 1.0 / COUNT(*),
            4
        )                                           AS pct_remote,

        -- SOC matching quality
        COUNT(*) FILTER (WHERE soc_match_type = 'exact_match')
                                                    AS soc_matched_postings,
        ROUND(
            COUNT(*) FILTER (WHERE soc_match_type = 'exact_match') * 1.0 / COUNT(*),
            4
        )                                           AS soc_match_rate,

        -- skills coverage (has at least one skill associated)
        COUNT(DISTINCT posting_id)                  AS distinct_posting_ids

    FROM {{ ref("fct_job_postings") }}
    GROUP BY 1, 2
)

SELECT
    data_source,
    is_snapshot,
    total_postings,
    unique_companies,
    earliest_posting,
    latest_posting,
    distinct_months,
    postings_with_salary,
    CASE WHEN postings_with_salary >= 5 THEN median_salary ELSE NULL END AS median_salary,
    CASE WHEN postings_with_salary >= 5 THEN avg_salary ELSE NULL END AS avg_salary,
    CASE WHEN postings_with_salary >= 5 THEN salary_p25 ELSE NULL END AS salary_p25,
    CASE WHEN postings_with_salary >= 5 THEN salary_p75 ELSE NULL END AS salary_p75,
    remote_postings,
    pct_remote,
    soc_matched_postings,
    soc_match_rate
FROM source_stats
ORDER BY total_postings DESC
