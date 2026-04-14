-- int_kaggle_ds__postings_enriched
-- Enriches asaniczka data science job postings with skills + summaries
-- Same schema pattern as large dataset (same author), plus job_summary from summary table

WITH postings_cleaned AS (
    SELECT
        TRIM(job_link)                              AS posting_id,
        LOWER(TRIM(job_title))                      AS job_title,
        LOWER(TRIM(company))                        AS company_name,
        LOWER(TRIM(job_location))                   AS location,
        LOWER(TRIM(job_level))                      AS experience_level,
        LOWER(TRIM(job_type))                       AS work_type,
        LOWER(TRIM(search_position))                AS search_role,
        LOWER(TRIM(search_city))                    AS search_city,
        LOWER(TRIM(search_country))                 AS search_country,

        CASE
            WHEN first_seen IS NOT NULL AND TRIM(first_seen) != ''
            THEN TRY_CAST(TRIM(first_seen) AS DATE)
            ELSE NULL
        END                                         AS posted_at

    FROM {{ ref("stg_kaggle_ds__postings") }}
    WHERE job_title IS NOT NULL
      AND TRIM(job_title) != ''
),

skills_cleaned AS (
    SELECT
        TRIM(job_link)                              AS posting_id,
        LOWER(TRIM(job_skills))                     AS skills_raw
    FROM {{ ref("stg_kaggle_ds__job_skills") }}
    WHERE job_skills IS NOT NULL
      AND TRIM(job_skills) != ''
),

summaries AS (
    SELECT
        TRIM(job_link)                              AS posting_id,
        TRIM(job_summary)                           AS description
    FROM {{ ref("stg_kaggle_ds__summary") }}
    WHERE job_summary IS NOT NULL
      AND TRIM(job_summary) != ''
)

SELECT
    p.posting_id,
    p.job_title,
    p.company_name,
    p.location,
    p.experience_level,
    p.work_type,
    p.search_role,
    p.posted_at,
    s.skills_raw,
    -- no salary data in this dataset
    CAST(NULL AS INTEGER)                           AS annual_salary_estimate,
    sm.description,
    'kaggle_ds'                                     AS data_source
FROM postings_cleaned AS p
LEFT JOIN skills_cleaned AS s
    USING(posting_id)
LEFT JOIN summaries AS sm
    USING(posting_id)
