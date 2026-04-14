-- fct_job_postings: unified job postings fact table
-- Unions: arshkon LinkedIn, asaniczka 1.3M LinkedIn, asaniczka data science, Adzuna
-- Left joins to int_title_to_soc_mapping for occupation classification
-- Each source is normalized to a common schema with NULLs where data doesn't exist

{{
    config(
        materialized='table',
        tags=['silver', 'fact']
    )
}}

WITH linkedin_arshkon AS (
    SELECT
        posting_id,
        job_title,
        description,
        company_name,
        location,
        work_type,
        experience_level,
        CAST(posted_at AS TIMESTAMP)                AS posted_at,
        annual_salary_estimate,
        CAST(NULL AS INTEGER)                       AS salary_min,
        CAST(NULL AS INTEGER)                       AS salary_max,
        CAST(NULL AS VARCHAR)                       AS city,
        CAST(NULL AS VARCHAR)                       AS state,
        data_source
    FROM {{ ref("int_kaggle_linkedin__postings_enriched") }}
),

linkedin_large AS (
    SELECT
        posting_id,
        job_title,
        description,
        company_name,
        location,
        work_type,
        experience_level,
        CAST(posted_at AS TIMESTAMP)                AS posted_at,
        annual_salary_estimate,
        CAST(NULL AS INTEGER)                       AS salary_min,
        CAST(NULL AS INTEGER)                       AS salary_max,
        CAST(NULL AS VARCHAR)                       AS city,
        CAST(NULL AS VARCHAR)                       AS state,
        data_source
    FROM {{ ref("int_kaggle_large__postings_enriched") }}
),

ds_jobs AS (
    SELECT
        posting_id,
        job_title,
        description,
        company_name,
        location,
        work_type,
        experience_level,
        CAST(posted_at AS TIMESTAMP)                AS posted_at,
        annual_salary_estimate,
        CAST(NULL AS INTEGER)                       AS salary_min,
        CAST(NULL AS INTEGER)                       AS salary_max,
        CAST(NULL AS VARCHAR)                       AS city,
        CAST(NULL AS VARCHAR)                       AS state,
        data_source
    FROM {{ ref("int_kaggle_ds__postings_enriched") }}
),

adzuna AS (
    SELECT
        posting_id,
        job_title,
        description,
        company                                     AS company_name,
        location,
        work_type,
        CAST(NULL AS VARCHAR)                       AS experience_level,
        CAST(created AS TIMESTAMP)                  AS posted_at,
        salary_mid                                  AS annual_salary_estimate,
        salary_min,
        salary_max,
        city,
        state,
        'adzuna'                                    AS data_source
    FROM {{ ref("int_adzuna__postings_enriched") }}
),

-- Stack all sources
all_postings AS (
    SELECT * FROM linkedin_arshkon
    UNION ALL
    SELECT * FROM linkedin_large
    UNION ALL
    SELECT * FROM ds_jobs
    UNION ALL
    SELECT * FROM adzuna
),

-- Attempt SOC code matching via exact title match
with_soc AS (
    SELECT
        p.*,
        soc.onet_soc_code,
        soc.soc_code,
        CASE
            WHEN soc.onet_soc_code IS NOT NULL THEN 'exact_match'
            ELSE 'unmatched'
        END                                         AS soc_match_type
    FROM all_postings AS p
    LEFT JOIN {{ ref("int_title_to_soc_mapping") }} AS soc
        ON p.job_title = soc.title_lower
)

SELECT
    posting_id,
    job_title,
    description,
    company_name,
    location,
    city,
    state,
    work_type,
    experience_level,
    posted_at,
    annual_salary_estimate,
    salary_min,
    salary_max,
    onet_soc_code,
    soc_code,
    soc_match_type,
    data_source
FROM with_soc
