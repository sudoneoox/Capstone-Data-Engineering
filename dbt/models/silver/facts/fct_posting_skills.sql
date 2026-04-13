-- fct_posting_skills: one row per posting × skill (long format)
-- Unions skills from all posting sources:
--   - arshkon LinkedIn: resolves skill_abr → full name via mapping table
--   - asaniczka large + DS: explodes comma-separated job_skills string
--   - Adzuna: no structured skills (not included here)

{{
    config(
        materialized='table',
        tags=['silver', 'fact']
    )
}}

WITH linkedin_skills AS (
    -- arshkon dataset: skill_abr resolved to full names via mapping
    SELECT
        CAST(js.job_id AS VARCHAR)                  AS posting_id,
        LOWER(TRIM(ms.skill_name))                  AS skill_name,
        'kaggle_linkedin'                           AS data_source
    FROM {{ ref("stg_kaggle_linkedin__job_skills") }} AS js
    INNER JOIN {{ ref("stg_kaggle_linkedin__mapping_skills") }} AS ms
        USING(skill_abr)
    WHERE ms.skill_name IS NOT NULL
      AND TRIM(ms.skill_name) != ''
),

large_skills AS (
    -- asaniczka 1.3M dataset: comma-separated → exploded rows
    SELECT
        TRIM(job_link)                              AS posting_id,
        LOWER(TRIM(skill))                          AS skill_name,
        'kaggle_linkedin_large'                     AS data_source
    FROM {{ ref("stg_kaggle_linkedin_large__job_skills") }},
    {{ explode_csv('job_skills') }}
    WHERE job_skills IS NOT NULL
      AND TRIM(job_skills) != ''
),

ds_skills AS (
    -- asaniczka data science dataset: comma-separated → exploded rows
    SELECT
        TRIM(job_link)                              AS posting_id,
        LOWER(TRIM(skill))                          AS skill_name,
        'kaggle_ds'                                 AS data_source
    FROM {{ ref("stg_kaggle_ds__job_skills") }},
    {{ explode_csv('job_skills') }}
    WHERE job_skills IS NOT NULL
      AND TRIM(job_skills) != ''
),

combined AS (
    SELECT * FROM linkedin_skills
    UNION ALL
    SELECT * FROM large_skills
    UNION ALL
    SELECT * FROM ds_skills
)

-- Deduplicate: same posting can have the same skill listed twice
SELECT DISTINCT
    posting_id,
    skill_name,
    data_source
FROM combined
WHERE skill_name IS NOT NULL
  AND LENGTH(skill_name) >= 2
