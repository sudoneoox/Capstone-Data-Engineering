-- gold_skills_demand: skills mentioned in postings tracked over time
-- Grain: skill_name × report_month
-- Dashboard: "Skills Tracker" — line charts, trending badges

{{
    config(
        materialized='table',
        tags=['gold']
    )
}}

WITH skill_mentions AS (
    -- Count distinct postings mentioning each skill per month
    SELECT
        ps.skill_name,
        DATE_TRUNC('month', fp.posted_at)           AS report_month,
        COUNT(DISTINCT ps.posting_id)               AS distinct_postings,
        COUNT(*)                                    AS mention_count
    FROM {{ ref("fct_posting_skills") }} AS ps
    INNER JOIN {{ ref("fct_job_postings") }} AS fp
        ON ps.posting_id = fp.posting_id
       AND ps.data_source = fp.data_source
    WHERE fp.posted_at IS NOT NULL
      AND ps.skill_name IS NOT NULL
      AND LENGTH(ps.skill_name) >= 2
    GROUP BY 1, 2
),

-- Total distinct postings per month (denominator for pct_of_postings)
monthly_totals AS (
    SELECT
        DATE_TRUNC('month', posted_at)              AS report_month,
        COUNT(DISTINCT posting_id)                  AS total_postings_that_month
    FROM {{ ref("fct_job_postings") }}
    WHERE posted_at IS NOT NULL
    GROUP BY 1
),

-- Filter to skills with enough total volume to be meaningful
qualified_skills AS (
    SELECT skill_name
    FROM skill_mentions
    GROUP BY 1
    HAVING SUM(mention_count) >= 50
),

with_totals AS (
    SELECT
        sm.skill_name,
        sm.report_month,
        sm.mention_count,
        sm.distinct_postings,
        mt.total_postings_that_month,
        ROUND(
            sm.distinct_postings * 1.0 / NULLIF(mt.total_postings_that_month, 0),
            6
        )                                           AS pct_of_postings
    FROM skill_mentions AS sm
    INNER JOIN qualified_skills AS qs
        USING(skill_name)
    LEFT JOIN monthly_totals AS mt
        USING(report_month)
),

with_lag AS (
    SELECT
        *,
        LAG(mention_count) OVER (
            PARTITION BY skill_name ORDER BY report_month
        )                                           AS prev_month_mentions,
        LAG(pct_of_postings) OVER (
            PARTITION BY skill_name ORDER BY report_month
        )                                           AS prev_month_pct
    FROM with_totals
)

SELECT
    skill_name,
    report_month,
    mention_count,
    distinct_postings,
    total_postings_that_month,
    pct_of_postings,
    prev_month_mentions,

    ROUND(
        (mention_count - prev_month_mentions) * 1.0
        / NULLIF(prev_month_mentions, 0),
        4
    )                                               AS mom_change_pct,

    -- Trending flag: growing > 10% MoM AND at least 10 mentions
    -- (prevents low-volume skills from being falsely "trending")
    CASE
        WHEN mention_count >= 10
         AND prev_month_mentions IS NOT NULL
         AND prev_month_mentions > 0
         AND (mention_count - prev_month_mentions) * 1.0
             / prev_month_mentions > 0.10
        THEN TRUE
        ELSE FALSE
    END                                             AS is_trending,

    EXTRACT(YEAR FROM report_month)                 AS year,
    EXTRACT(MONTH FROM report_month)                AS month_num

FROM with_lag
WHERE mention_count >= 5
ORDER BY skill_name, report_month
