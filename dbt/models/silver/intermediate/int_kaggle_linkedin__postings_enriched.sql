-- int_kaggle_linkedin__postings_enriched
-- Enriches arshkon LinkedIn postings with annualized salary + resolved skill names
-- Salary logic: corrects mislabeled pay_periods by magnitude, normalizes to annual USD
-- Skills: resolves abbreviations via mapping table, aggregates into array per posting

-- INFO: =============================== LINKEDIN SALARY FIX ==========================
WITH salary_base AS (
-- We join with postings in order to get the job title this is solely for making sure the job salary is within a reasonable range
    SELECT
        s.salary_id,
        s.job_id,
        p.title,
        s.max_salary,
        s.med_salary,
        s.min_salary,
        s.pay_period AS original_pay_period,
        s.currency,
        p.work_type,

        -- Some rows have either of these missing so we COALESCE them into one column to easily compare 
        -- AND for upcoming transformations
        COALESCE(s.med_salary, s.max_salary, s.min_salary) AS observed_salary
    FROM {{ ref("stg_kaggle_linkedin__salaries") }} s
    LEFT JOIN {{ ref("stg_kaggle_linkedin__postings") }} p
        ON s.job_id = p.job_id
), fixed_pay_periods AS (

    SELECT
        salary_id,
        job_id,
        title,
        max_salary,
        med_salary,
        min_salary,
        original_pay_period,
        currency,
        work_type,
        observed_salary,

        CASE
            -- no observed salaries were NULL but we keep this just in CASE
            WHEN observed_salary IS NULL THEN original_pay_period

            -- Extremely large raw values are almost never hourly/weekly/monthly
            WHEN observed_salary >= 250000 THEN 'YEARLY'

            -- 15k to < 250k could be YEARLY or MONTHLY depending on context
            WHEN observed_salary BETWEEN 15000 AND 250000 THEN 
                CASE
                    WHEN original_pay_period = 'YEARLY' THEN 'YEARLY'
                    WHEN original_pay_period = 'MONTHLY' AND observed_salary <= 50000 THEN 'MONTHLY'
                    WHEN work_type IN ('FULL_TIME', 'CONTRACT', 'TEMPORARY') THEN 'YEARLY'
                    ELSE 'YEARLY'
                END

            -- 4k to < 15k is usually MONTHLY or BIWEEKLY, sometimes WEEKLY
            WHEN observed_salary BETWEEN 4000 AND 15000 THEN 
                CASE
                    WHEN original_pay_period = 'MONTHLY' THEN 'MONTHLY'
                    WHEN original_pay_period = 'BIWEEKLY' THEN 'BIWEEKLY'
                    WHEN original_pay_period = 'WEEKLY' THEN 'WEEKLY'
                    WHEN work_type IN ('FULL_TIME', 'PART_TIME', 'CONTRACT', 'TEMPORARY') THEN 'MONTHLY'
                    ELSE 'MONTHLY'
                END

            -- 1k to < 4k is usually WEEKLY or BIWEEKLY
            WHEN observed_salary BETWEEN 1000 AND 4000 THEN
                CASE
                    WHEN original_pay_period = 'BIWEEKLY' THEN 'BIWEEKLY'
                    WHEN original_pay_period = 'WEEKLY' THEN 'WEEKLY'
                    WHEN original_pay_period = 'MONTHLY' AND observed_salary >= 2500 THEN 'MONTHLY'
                    ELSE 'WEEKLY'
                END

            -- < 1k is usually HOURLY, sometimes WEEKLY for very low-paid/short-hour roles
            WHEN observed_salary < 1000 THEN
                CASE
                    WHEN original_pay_period = 'WEEKLY' AND observed_salary >= 200 THEN 'WEEKLY'
                    ELSE 'HOURLY'
                END

            ELSE original_pay_period
        END AS corrected_pay_period

    FROM salary_base
),

annualized AS (
-- Normalize salaries to annual pay after we have corrected their work_types
    SELECT
        salary_id,
        job_id,
        title,
        work_type,
        currency,
        original_pay_period,
        corrected_pay_period,
        max_salary,
        med_salary,
        min_salary,

        CASE
            WHEN min_salary IS NOT NULL AND corrected_pay_period = 'HOURLY'   THEN min_salary * 2080
            WHEN min_salary IS NOT NULL AND corrected_pay_period = 'WEEKLY'   THEN min_salary * 52
            WHEN min_salary IS NOT NULL AND corrected_pay_period = 'BIWEEKLY' THEN min_salary * 26
            WHEN min_salary IS NOT NULL AND corrected_pay_period = 'MONTHLY'  THEN min_salary * 12
            WHEN min_salary IS NOT NULL AND corrected_pay_period = 'YEARLY'   THEN min_salary
            ELSE NULL
        END AS annualized_min_salary,

        CASE
            WHEN med_salary IS NOT NULL AND corrected_pay_period = 'HOURLY'   THEN med_salary * 2080
            WHEN med_salary IS NOT NULL AND corrected_pay_period = 'WEEKLY'   THEN med_salary * 52
            WHEN med_salary IS NOT NULL AND corrected_pay_period = 'BIWEEKLY' THEN med_salary * 26
            WHEN med_salary IS NOT NULL AND corrected_pay_period = 'MONTHLY'  THEN med_salary * 12
            WHEN med_salary IS NOT NULL AND corrected_pay_period = 'YEARLY'   THEN med_salary
            ELSE NULL
        END AS annualized_med_salary,

        CASE
            WHEN max_salary IS NOT NULL AND corrected_pay_period = 'HOURLY'   THEN max_salary * 2080
            WHEN max_salary IS NOT NULL AND corrected_pay_period = 'WEEKLY'   THEN max_salary * 52
            WHEN max_salary IS NOT NULL AND corrected_pay_period = 'BIWEEKLY' THEN max_salary * 26
            WHEN max_salary IS NOT NULL AND corrected_pay_period = 'MONTHLY'  THEN max_salary * 12
            WHEN max_salary IS NOT NULL AND corrected_pay_period = 'YEARLY'   THEN max_salary
            ELSE NULL
        END AS annualized_max_salary

    FROM fixed_pay_periods
), corrected_annualized_estimate AS (
    SELECT
        salary_id,
        job_id,
        COALESCE(
            annualized_med_salary,
            (annualized_min_salary + annualized_max_salary) / 2.0,
            annualized_min_salary,
            annualized_max_salary
        ) AS annual_salary_estimate
    FROM annualized
    WHERE COALESCE(
            annualized_med_salary,
            (annualized_min_salary + annualized_max_salary) / 2.0,
            annualized_min_salary,
            annualized_max_salary
          ) BETWEEN 15000 AND 1000000
      AND UPPER(currency) = 'USD'
), final_linkedin_salaries AS (
    SELECT 
        job_id,
        CAST(ROUND(annual_salary_estimate) AS INTEGER) AS annual_salary_estimate
    FROM corrected_annualized_estimate
), 
-- INFO: =============================== JOB SKILLS ====================================
skill_mapping_join AS (
-- Resolve skill_abr by joining table with mapping_skills to get full skill names
    SELECT 
        ljs.job_id,
        TRIM(LOWER(lms.skill_name)) AS skill_name
    FROM {{ ref("stg_kaggle_linkedin__job_skills") }} AS ljs
    LEFT JOIN {{ ref("stg_kaggle_linkedin__mapping_skills") }} AS lms
    USING(skill_abr)
    WHERE lms.skill_name IS NOT NULL
), skills_per_job AS (
    SELECT
        job_id,
        {{ skills_array_agg('skill_name', distinct=true) }} AS skills_array
    FROM skill_mapping_join
    GROUP BY 1
)

-- INFO: =============================== FINAL JOIN ====================================
-- Join postings with salary and skills, normalize all text columns
SELECT
    CAST(p.job_id AS {{ dbt.type_string() }})                       AS posting_id,
    LOWER(TRIM(p.title))                            AS job_title,
    TRIM(p.description)                             AS description,
    LOWER(TRIM(p.location))                         AS location,
    LOWER(TRIM(p.company_name))                     AS company_name,
    CAST(p.company_id AS BIGINT)                    AS company_id,
    LOWER(TRIM(p.formatted_work_type))              AS work_type,
    LOWER(TRIM(p.formatted_experience_level))       AS experience_level,
    -- listed_time is epoch milliseconds, convert to timestamp
  CASE
      WHEN p.listed_time IS NOT NULL AND p.listed_time > 0
      THEN CAST({{ epoch_ms_to_timestamp("CAST(p.listed_time AS BIGINT)") }} AS TIMESTAMP)
      ELSE NULL
  END                                               AS posted_at,    
  CAST(p.remote_allowed AS BOOLEAN)                 AS is_remote,
    CAST(p.views AS INTEGER)                        AS view_count,
    CAST(p.applies AS INTEGER)                      AS apply_count,
    sal.annual_salary_estimate,
    ski.skills_array,
    'kaggle_linkedin'                               AS data_source
FROM {{ ref("stg_kaggle_linkedin__postings") }} AS p
LEFT JOIN final_linkedin_salaries AS sal
    USING(job_id)
LEFT JOIN skills_per_job AS ski
    USING(job_id)
