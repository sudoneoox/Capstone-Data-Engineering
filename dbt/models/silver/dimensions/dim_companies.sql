-- dim_companies: aggregated company dimension from arshkon LinkedIn dataset
-- Joins companies + industries + specialties + employee_counts
-- Takes the most recent employee/follower count per company

WITH companies_cleaned AS (
    SELECT
        CAST(company_id AS BIGINT)                  AS company_id,
        LOWER(TRIM(name))                           AS company_name,
        TRIM(description)                           AS company_description,
        CAST(company_size AS INTEGER)               AS company_size,
        LOWER(TRIM(city))                           AS city,
        LOWER(TRIM(state))                          AS state,
        LOWER(TRIM(country))                        AS country,
        TRIM(zip_code)                              AS zip_code,
        TRIM(url)                                   AS url
    FROM {{ ref("stg_kaggle_linkedin__companies") }}
    WHERE company_id IS NOT NULL
),

-- Aggregate industries into comma-separated string per company
industries_agg AS (
    SELECT
        CAST(company_id AS BIGINT)                  AS company_id,
        {{ string_agg_sorted_distinct("LOWER(TRIM(industry))") }} AS industries        
    FROM {{ ref("stg_kaggle_linkedin__company_industries") }}
    WHERE industry IS NOT NULL
      AND TRIM(industry) != ''
    GROUP BY 1
),

-- Aggregate specialties into comma-separated string per company
specialties_agg AS (
    SELECT
        CAST(company_id AS BIGINT)                  AS company_id,
        {{ string_agg_sorted_distinct("LOWER(TRIM(speciality))") }} AS specialties
    FROM {{ ref("stg_kaggle_linkedin__company_specialties") }}
    WHERE speciality IS NOT NULL
      AND TRIM(speciality) != ''
    GROUP BY 1
),

-- Take the most recent employee/follower count per company
-- time_recorded is epoch seconds
latest_counts AS (
    SELECT
        CAST(company_id AS BIGINT)                  AS company_id,
        employee_count,
        follower_count,
        ROW_NUMBER() OVER (
            PARTITION BY company_id
            ORDER BY time_recorded DESC
        ) AS rn
    FROM {{ ref("stg_kaggle_linkedin__employee_counts") }}
    WHERE employee_count IS NOT NULL
)

SELECT
    c.company_id,
    c.company_name,
    c.company_description,
    c.company_size,
    c.city,
    c.state,
    c.country,
    c.zip_code,
    c.url,
    i.industries,
    sp.specialties,
    CAST(ec.employee_count AS INTEGER)              AS employee_count,
    CAST(ec.follower_count AS INTEGER)              AS follower_count
FROM companies_cleaned AS c
LEFT JOIN industries_agg AS i
    USING(company_id)
LEFT JOIN specialties_agg AS sp
    USING(company_id)
LEFT JOIN latest_counts AS ec
    ON c.company_id = ec.company_id AND ec.rn = 1
