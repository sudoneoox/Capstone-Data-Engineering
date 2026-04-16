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
),

blocklist AS (
    SELECT UNNEST([
        -- education requirements
        'bachelor''s degree', 'master''s degree', 'associate''s degree',
        'bachelor degree', 'master degree', 'high school diploma',
        'ged', 'phd', 'mba',
        -- age / legal requirements
        '18 years or older', '21 years or older', 'must be 18',
        -- generic attributes that aren't skills
        'can do', '"can do" attributes', 'can-do attitude',
        'team player', 'self-starter', 'detail-oriented',
        'hard worker', 'fast learner', 'go-getter',
        -- experience requirements
        'experience', 'years of experience', 'entry level',
        'mid level', 'senior level',
        -- work logistics
        'full-time', 'part-time', 'remote', 'hybrid', 'on-site',
        'valid driver''s license', 'driver''s license',
        'background check', 'drug test', 'drug screen',
        'us citizen', 'citizenship', 'security clearance',
        'able to lift', 'standing', 'sitting',
        -- benefits / perks (not skills)
        'health insurance', 'dental insurance', '401k', '401(k)',
        'paid time off', 'pto', 'vacation'
    ]) AS blocked_term
)

-- Deduplicate: same posting can have the same skill listed twice
SELECT DISTINCT
    posting_id,
    skill_name,
    data_source
FROM combined
WHERE skill_name IS NOT NULL
  AND LENGTH(skill_name) >= 4
  AND LENGTH(skill_name) <= 50  -- catch garbage long strings
  AND skill_name NOT IN (SELECT blocked_term FROM blocklist)
  -- also filter patterns that are clearly not skills
  AND skill_name NOT ILIKE '%years or older%'
  AND skill_name NOT ILIKE '%must be%'
  AND skill_name NOT ILIKE '%ability to%'
  AND skill_name NOT ILIKE '%required%'
  AND skill_name NOT ILIKE '%attributes%'
  AND skill_name NOT ILIKE '%driver%license%'
  AND skill_name NOT ILIKE '%bonus%'
  AND skill_name NOT ILIKE '%per%year%'
  AND skill_name NOT ILIKE '%salary%'
  AND skill_name NOT ILIKE '%experience%'
  AND skill_name NOT ILIKE '%401%'
  AND skill_name NOT ILIKE '%license%'
  AND skill_name NOT ILIKE '%degree%'
  AND skill_name NOT ILIKE '%years of age%'
  AND skill_name NOT ILIKE '%years old%'
  AND skill_name NOT ILIKE '%or older%'
  AND skill_name NOT ILIKE '%equal opportunity%'
  AND skill_name NOT ILIKE '%affirmative action%'
  AND skill_name NOT ILIKE '%reasonable accommod%'
  AND skill_name NOT ILIKE '%authorized to work%'
  AND skill_name NOT ILIKE '%eligib%to work%'
  AND skill_name NOT ILIKE '%disability%'
  AND skill_name NOT ILIKE '%insurance%'
  AND skill_name NOT ILIKE '%reimbursement%'
  AND skill_name NOT ILIKE '%stakeholder%'
  AND skill_name NOT ILIKE '%early years%'
  AND skill_name NOT ILIKE '%company paid%'
  AND skill_name NOT ILIKE '%company policies%'
  AND skill_name NOT ILIKE '%accredited%'
  AND skill_name NOT ILIKE '%certification%'
