
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
),fixed_pay_periods as (

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

            /*
              HARD RULES BY RAW MAGNITUDE
              These are "what period does this raw number most plausibly represent?"
            */

            -- Extremely large raw values are almost never hourly/weekly/monthly
            WHEN observed_salary >= 250000 THEN 'YEARLY'

            -- 15k to < 250k could be YEARLY or MONTHLY depending on context
            WHEN observed_salary BETWEEN 15000 AND 250000 THEN 
                CASE
                    -- monthly values above ~15k happen, but if originally yearly keep yearly
                    WHEN original_pay_period = 'YEARLY' THEN 'YEARLY'
                    -- If originally monthly AND salary is below 50000 THEN keep it as monthly
                    WHEN original_pay_period = 'MONTHLY' AND observed_salary <= 50000 THEN 'MONTHLY'
                    WHEN work_type IN ('FULL_TIME', 'CONTRACT', 'TEMPORARY') THEN 'YEARLY'
                    ELSE 'YEARLY'
                END

            -- 4k to < 15k is usually MONTHLY or BIWEEKLY, sometimes WEEKLY
            WHEN observed_salary BETWEEN 4000 AND 15000 THEN 
                CASE
                    WHEN original_pay_period = 'MONTHLY' THEN 'MONTHLY'
                    WHEN original_pay_period = 'BIWEEKLY' THEN 'BIWEEKLY'
                    WHEN  original_pay_period = 'WEEKLY' THEN 'WEEKLY'
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

annualized as (
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
	-- Filter out for only full time jobs this still leaves us with a lot of rows
	-- Estimate annual salary from the med, max, and min
	-- Filter out outliers that are due to input error from source
	-- Mark whether the pay_period was corrected
	-- Filter out non USD currency job postings
	SELECT
	    salary_id,
	    job_id,
	    title,
	    work_type,
	    currency,
	    original_pay_period,
	    corrected_pay_period,
	    min_salary,
	    med_salary,
	    max_salary,
	    annualized_min_salary,
	    annualized_med_salary,
	    annualized_max_salary,
	    COALESCE(
	        annualized_med_salary,
	        (annualized_min_salary + annualized_max_salary) / 2.0,
	        annualized_min_salary,
	        annualized_max_salary
	    ) as annual_salary_estimate,
	    CASE
	        WHEN original_pay_period <> corrected_pay_period THEN true
	        ELSE false
	    END AS was_pay_period_corrected
	FROM annualized
	WHERE annual_salary_estimate < 1000000 AND annual_salary_estimate > 15000
		AND UPPER(currency) = 'USD'
), final_linkedin_salaries AS (
	-- Finally grab only the columns we need for our intermediate table
	-- We might change and optionally add the med, min, and max annual salaries for more insight if we need to
	SELECT 
		job_id,
		salary_id,
		CAST(ROUND(annual_salary_estimate) AS INTEGER) AS annual_salary_estimate -- round a couple cents don't really matter 
	FROM corrected_annualized_estimate
), 
--INFO: =============================== JOB SKILLS ====================================
skill_mapping_join AS (
-- Resolve skill_abr by joining table with mapping_skills to get full skill names
	SELECT 
		ljs.job_id AS job_id,
		TRIM(LOWER(lms.skill_name)) AS skill_name
	FROM {{ ref("stg_kaggle_linkedin__job_skills") }} AS ljs
	LEFT JOIN {{ ref("stg_kaggle_linkedin__mapping_skills") }} AS lms
	USING(skill_abr)
), skills_salaries AS (
	-- The salaries table has far less rows than the skills table so we use left join here
	-- Also put skills into an array for similar job_ids
	SELECT 
		sal.job_id,
		sal.salary_id,
		sal.annual_salary_estimate,
		LIST(DISTINCT skill_name) FILTER (WHERE ski.skill_name IS NOT NULL) AS skills_array
	FROM final_linkedin_salaries AS sal
	LEFT JOIN skill_mapping_join AS ski
	USING (job_id) 
	GROUP BY 1,2,3
)

-- We might change the order of the join later to exclude rows with missing salaries later on
SELECT
	s.annual_salary_estimate,
	s.skills_array,
	p.company_id,
	p.location
FROM {{ ref("stg_kaggle_linkedin__postings") }} AS p
LEFT JOIN skills_salaries AS s
USING(job_id)

