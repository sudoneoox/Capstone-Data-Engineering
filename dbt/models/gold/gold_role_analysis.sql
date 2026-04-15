-- gold_role_analysis: per-occupation analytics
-- Grain: one row per onet_soc_code (minimum 10 matched postings)
-- Dashboard: "Role Deep-Dive" — role profile cards

{{
    config(
        materialized='table',
        tags=['gold']
    )
}}

WITH matched_postings AS (
    SELECT *
    FROM {{ ref("fct_job_postings") }}
    WHERE soc_match_type = 'exact_match'
      AND onet_soc_code IS NOT NULL
),

posting_stats AS (
    SELECT
        onet_soc_code,
        soc_code,
        COUNT(*)                                    AS total_postings,

        COUNT(*) FILTER (
            WHERE posted_at >= CURRENT_DATE - INTERVAL '30 days'
        )                                           AS postings_last_30d,

        COUNT(*) FILTER (
            WHERE posted_at >= CURRENT_DATE - INTERVAL '90 days'
        )                                           AS postings_last_90d,

        COUNT(DISTINCT company_name)                AS unique_companies,

        -- salary distribution
        COUNT(annual_salary_estimate)               AS postings_with_salary,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY annual_salary_estimate)
                                                    AS salary_p25_raw,
        MEDIAN(annual_salary_estimate)              AS median_salary_raw,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY annual_salary_estimate)
                                                    AS salary_p75_raw,

        -- remote work via boolean flag
        COUNT(*) FILTER (WHERE is_remote = TRUE)    AS remote_postings,
        ROUND(
            COUNT(*) FILTER (WHERE is_remote = TRUE) * 1.0 / COUNT(*),
            4
        )                                           AS pct_remote,

        -- source breakdown
        COUNT(*) FILTER (WHERE data_source = 'adzuna')              AS adzuna_postings,
        COUNT(*) FILTER (WHERE data_source = 'kaggle_linkedin')     AS linkedin_postings,
        COUNT(*) FILTER (WHERE data_source = 'kaggle_linkedin_large') AS linkedin_large_postings,
        COUNT(*) FILTER (WHERE data_source = 'kaggle_ds')           AS ds_postings

    FROM matched_postings
    GROUP BY 1, 2
    HAVING COUNT(*) >= 10
),

experience_mode AS (
    SELECT
        onet_soc_code,
        experience_level                            AS most_common_experience_level,
        ROW_NUMBER() OVER (
            PARTITION BY onet_soc_code
            ORDER BY COUNT(*) DESC
        )                                           AS rn
    FROM matched_postings
    WHERE experience_level IS NOT NULL
      AND TRIM(experience_level) != ''
    GROUP BY 1, 2
),

work_type_mode AS (
    SELECT
        onet_soc_code,
        work_type                                   AS most_common_work_type,
        ROW_NUMBER() OVER (
            PARTITION BY onet_soc_code
            ORDER BY COUNT(*) DESC
        )                                           AS rn
    FROM matched_postings
    WHERE work_type IS NOT NULL
      AND TRIM(work_type) != ''
    GROUP BY 1, 2
),

top_companies AS (
    SELECT
        onet_soc_code,
        STRING_AGG(company_name, ', ')              AS top_hiring_companies
    FROM (
        SELECT
            onet_soc_code,
            company_name,
            ROW_NUMBER() OVER (
                PARTITION BY onet_soc_code
                ORDER BY COUNT(*) DESC
            ) AS rn
        FROM matched_postings
        WHERE company_name IS NOT NULL
          AND TRIM(company_name) != ''
        GROUP BY 1, 2
    )
    WHERE rn <= 5
    GROUP BY 1
)

SELECT
    ps.onet_soc_code,
    ps.soc_code,
    occ.occupation_title,
    occ.occupation_description,
    occ.job_zone,

    ps.total_postings,
    ps.postings_last_30d,
    ps.postings_last_90d,
    ps.unique_companies,

    ps.postings_with_salary,
    CASE WHEN ps.postings_with_salary >= 5
        THEN CAST(ROUND(ps.salary_p25_raw) AS INTEGER)
    END                                             AS salary_p25,
    CASE WHEN ps.postings_with_salary >= 5
        THEN CAST(ROUND(ps.median_salary_raw) AS INTEGER)
    END                                             AS median_salary,
    CASE WHEN ps.postings_with_salary >= 5
        THEN CAST(ROUND(ps.salary_p75_raw) AS INTEGER)
    END                                             AS salary_p75,

    ps.remote_postings,
    ps.pct_remote,
    exp.most_common_experience_level,
    wt.most_common_work_type,
    tc.top_hiring_companies,

    edu.pct_bachelors_degree,
    edu.pct_masters_degree,
    edu.pct_doctoral_degree,

    ps.adzuna_postings,
    ps.linkedin_postings,
    ps.linkedin_large_postings,
    ps.ds_postings

FROM posting_stats AS ps

INNER JOIN {{ ref("dim_occupations") }} AS occ
    ON ps.onet_soc_code = occ.onet_soc_code

LEFT JOIN {{ ref("dim_education_requirements") }} AS edu
    ON ps.onet_soc_code = edu.onet_soc_code

LEFT JOIN experience_mode AS exp
    ON ps.onet_soc_code = exp.onet_soc_code AND exp.rn = 1

LEFT JOIN work_type_mode AS wt
    ON ps.onet_soc_code = wt.onet_soc_code AND wt.rn = 1

LEFT JOIN top_companies AS tc
    ON ps.onet_soc_code = tc.onet_soc_code

ORDER BY ps.total_postings DESC
