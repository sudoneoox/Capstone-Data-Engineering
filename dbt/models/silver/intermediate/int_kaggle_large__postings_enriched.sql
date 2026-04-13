-- int_kaggle_large__postings_enriched
-- Enriches asaniczka 1.3M LinkedIn postings with exploded skills
-- Skills come as comma-separated string in job_skills — we keep as array here,
-- long-format explosion happens in fct_posting_skills

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

        -- first_seen is a date string like "2024-01-15"
        CASE
            WHEN first_seen IS NOT NULL AND TRIM(first_seen) != ''
            THEN TRY_CAST(TRIM(first_seen) AS DATE)
            ELSE NULL
        END                                         AS posted_at

    FROM {{ ref("stg_kaggle_linkedin_large__postings") }}
    WHERE job_title IS NOT NULL
      AND TRIM(job_title) != ''
),

skills_cleaned AS (
    -- job_skills is a comma-separated string like "python, sql, spark"
    SELECT
        TRIM(job_link)                              AS posting_id,
        LOWER(TRIM(job_skills))                     AS skills_raw
    FROM {{ ref("stg_kaggle_linkedin_large__job_skills") }}
    WHERE job_skills IS NOT NULL
      AND TRIM(job_skills) != ''
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
    -- no description in this dataset
    CAST(NULL AS VARCHAR)                           AS description,
    'kaggle_linkedin_large'                         AS data_source
FROM postings_cleaned AS p
LEFT JOIN skills_cleaned AS s
    USING(posting_id)
