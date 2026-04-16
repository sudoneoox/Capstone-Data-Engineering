-- gold_metro_comparison: geographic job market comparison
-- Grain: one row per city × state (for matched metros) or state (for unmatched)
-- Dashboard: "Metro Compare" — side-by-side metro cards

{{
    config(
        materialized='table',
        tags=['gold']
    )
}}

WITH bridge AS (
    SELECT
        LOWER(TRIM(state))                          AS state,
        LOWER(TRIM(city))                           AS city,
        TRIM(CAST (cbsa_fips AS VARCHAR(12)))             AS cbsa_fips
    FROM {{ ref("state_to_cbsa_bridge") }}
),

-- Pre-join postings to bridge at the posting level so each posting
-- maps to at most ONE metro (or none). This prevents fan-out.
postings_with_metro AS (
    SELECT
        fp.*,
        b.cbsa_fips
    FROM {{ ref("fct_job_postings") }} AS fp
    LEFT JOIN bridge AS b
        ON LOWER(TRIM(fp.state)) = b.state
       AND LOWER(TRIM(fp.city)) = b.city
    WHERE fp.state IS NOT NULL
      AND TRIM(fp.state) != ''
),

-- Aggregate by metro where we have a match, by state where we don't
geo_postings AS (
    SELECT
        LOWER(TRIM(state))                          AS state,
        cbsa_fips,
        COUNT(*)                                    AS total_postings,
        COUNT(DISTINCT company_name)                AS unique_companies,

        COUNT(annual_salary_estimate)               AS postings_with_salary,
        MEDIAN(annual_salary_estimate)              AS median_salary_raw,
        AVG(annual_salary_estimate)                 AS avg_salary_raw,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY annual_salary_estimate)
                                                    AS salary_p25_raw,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY annual_salary_estimate)
                                                    AS salary_p75_raw,

        COUNT(*) FILTER (WHERE is_remote = TRUE)    AS remote_postings,
        ROUND(
            COUNT(*) FILTER (WHERE is_remote = TRUE) * 1.0 / COUNT(*),
            4
        )                                           AS pct_remote,

        COUNT(*) FILTER (WHERE data_source = 'adzuna')  AS adzuna_postings,
        COUNT(*) FILTER (WHERE data_source != 'adzuna') AS other_source_postings

    FROM postings_with_metro
    GROUP BY 1, 2
    HAVING COUNT(*) >= 5
)

SELECT
    gp.state,
    gp.total_postings,
    gp.unique_companies,
    gp.postings_with_salary,

    CASE WHEN gp.postings_with_salary >= 5
        THEN CAST(ROUND(gp.median_salary_raw) AS INTEGER)
    END                                             AS median_salary,
    CASE WHEN gp.postings_with_salary >= 5
        THEN CAST(ROUND(gp.avg_salary_raw) AS INTEGER)
    END                                             AS avg_salary,
    CASE WHEN gp.postings_with_salary >= 5
        THEN CAST(ROUND(gp.salary_p25_raw) AS INTEGER)
    END                                             AS salary_p25,
    CASE WHEN gp.postings_with_salary >= 5
        THEN CAST(ROUND(gp.salary_p75_raw) AS INTEGER)
    END                                             AS salary_p75,

    gp.remote_postings,
    gp.pct_remote,
    gp.adzuna_postings,
    gp.other_source_postings,

    gp.cbsa_fips,
    dm.metro_name,
    dm.median_household_income,

    CASE
        WHEN gp.postings_with_salary >= 5 AND dm.median_household_income > 0
        THEN ROUND(gp.median_salary_raw / dm.median_household_income, 2)
    END                                             AS salary_to_income_ratio,

    dm.pct_bachelors_or_higher,
    dm.pct_work_from_home,
    dm.pct_information_industry,
    dm.pct_mgmt_business_science_arts,
    dm.pct_professional_scientific_management_admin_waste,
    dm.pct_broadband,
    dm.unemployment_rate                            AS acs_unemployment_rate,
    dm.mean_commute_minutes

FROM geo_postings AS gp
LEFT JOIN {{ ref("dim_metros") }} AS dm
    ON gp.cbsa_fips = dm.cbsa_fips
ORDER BY gp.total_postings DESC
