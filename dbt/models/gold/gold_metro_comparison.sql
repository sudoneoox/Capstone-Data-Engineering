-- gold_metro_comparison: geographic job market comparison
-- Grain: one row per state
-- Dashboard: "Metro Compare" — side-by-side metro cards



WITH geo_postings AS (
    SELECT
        LOWER(TRIM(state))                          AS state,
        COUNT(*)                                    AS total_postings,
        COUNT(DISTINCT company_name)                AS unique_companies,

        COUNT(annual_salary_estimate)               AS postings_with_salary,
        MEDIAN(annual_salary_estimate)              AS median_salary_raw,
        AVG(annual_salary_estimate)                 AS avg_salary_raw,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY annual_salary_estimate)
                                                    AS salary_p25_raw,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY annual_salary_estimate)
                                                    AS salary_p75_raw,

        -- remote via boolean flag
        COUNT(*) FILTER (WHERE is_remote = TRUE)    AS remote_postings,
        ROUND(
            COUNT(*) FILTER (WHERE is_remote = TRUE) * 1.0 / COUNT(*),
            4
        )                                           AS pct_remote,

        COUNT(*) FILTER (WHERE data_source = 'adzuna')  AS adzuna_postings,
        COUNT(*) FILTER (WHERE data_source != 'adzuna') AS other_source_postings

    FROM {{ ref("fct_job_postings") }}
    WHERE state IS NOT NULL
      AND TRIM(state) != ''
    GROUP BY 1
    HAVING COUNT(*) >= 5
),

bridge AS (
    SELECT
        LOWER(TRIM(state))                          AS state,
        LOWER(TRIM(city))                           AS city,
        TRIM(CAST(cbsa_fips AS VARCHAR))                             AS cbsa_fips
    FROM {{ ref("state_to_cbsa_bridge") }}
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

    b.cbsa_fips,
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
LEFT JOIN bridge AS b USING(state)
LEFT JOIN {{ ref("dim_metros") }} AS dm ON b.cbsa_fips = dm.cbsa_fips
ORDER BY gp.total_postings DESC
