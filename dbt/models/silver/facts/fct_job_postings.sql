-- fct_job_postings: unified job postings fact table
-- Unions: arshkon LinkedIn, asaniczka 1.3M LinkedIn, asaniczka data science, Adzuna
-- Left joins to int_title_to_soc_mapping for occupation classification
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
        CAST(NULL AS {{ dbt.type_string() }} )                       AS city,
        CAST(NULL AS {{ dbt.type_string() }} )                       AS state,
        -- arshkon has an explicit remote flag from the source
        COALESCE(
            is_remote,
            work_type ILIKE '%remote%'
        )                                           AS is_remote,
        FALSE                                       AS is_snapshot,
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
        CAST(NULL AS {{ dbt.type_string() }} )                       AS city,
        CAST(NULL AS {{ dbt.type_string() }} )                       AS state,
        -- no explicit remote flag, derive from work_type and title
        COALESCE(
            work_type ILIKE '%remote%',
            job_title ILIKE '%remote%',
            FALSE
        )                                           AS is_remote,
        -- all first_seen = January 2024, this is a snapshot not a time-series
        TRUE                                        AS is_snapshot,
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
        CAST(NULL AS {{ dbt.type_string() }})                       AS city,
        CAST(NULL AS {{ dbt.type_string() }})                       AS state,
        COALESCE(
            work_type ILIKE '%remote%',
            job_title ILIKE '%remote%',
            FALSE
        )                                           AS is_remote,
        -- same author as large, same snapshot issue
        TRUE                                        AS is_snapshot,
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
        CAST(NULL AS {{ dbt.type_string() }} )                       AS experience_level,
        CAST(created AS TIMESTAMP)                  AS posted_at,
        salary_mid                                  AS annual_salary_estimate,
        salary_min,
        salary_max,
        city,
        state,
        -- Adzuna: derive from work_type, title, and location
        COALESCE(
            work_type ILIKE '%remote%',
            job_title ILIKE '%remote%',
            location ILIKE '%remote%',
            FALSE
        )                                           AS is_remote,
        -- Adzuna is live collection, not a snapshot
        FALSE                                       AS is_snapshot,
        'adzuna'                                    AS data_source
    FROM {{ ref("int_adzuna__postings_enriched") }}
),

all_postings AS (
    SELECT * FROM linkedin_arshkon
    UNION ALL
    SELECT * FROM linkedin_large
    UNION ALL
    SELECT * FROM ds_jobs
    UNION ALL
    SELECT * FROM adzuna
),

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
    is_remote,
    is_snapshot,
    onet_soc_code,
    soc_code,
    soc_match_type,
    data_source
FROM with_soc
