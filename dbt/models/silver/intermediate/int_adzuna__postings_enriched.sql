-- int_adzuna__postings_enriched
-- Enriches Adzuna live postings with parsed geo from seed reference.
-- Deduplicates by posting id (same job can appear across multiple
-- daily runs or overlapping search queries).

WITH base AS (

    SELECT
        id,
        title,
        description,
        contract_time,
        salary_min,
        salary_max,
        latitude,
        longitude,
        "location.display_name" AS raw_location,
        "company.display_name" AS company,
        "category.label" AS category_label,
        CAST(created AS TIMESTAMP) AS created,
        (salary_min + salary_max) / 2.0 AS salary_mid,

        TRIM(REPLACE(CAST(STRING_SPLIT("location.display_name", ',')->0 AS VARCHAR), '"', '')) AS part_1,
        TRIM(REPLACE(CAST(STRING_SPLIT("location.display_name", ',')->1 AS VARCHAR), '"', '')) AS part_2,

        -- Deduplicate: keep the most recent fetch of each posting
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY created DESC
        ) AS rn

    FROM {{ ref("stg_adzuna__postings") }}
),

deduped AS (
    SELECT * FROM base WHERE rn = 1
),

normalized AS (

    SELECT
        *,
        LOWER(part_1) AS part_1_norm,
        LOWER(part_2) AS part_2_norm,
        LOWER(raw_location) AS raw_location_norm
    FROM deduped
),
-- We will use this seed file to find missing counties, states, or cities and match them
-- We will also use it to distinguish whether a pairing is state,city or city,county
geo AS (

    SELECT DISTINCT
        LOWER(TRIM("City")) AS city_norm,
        LOWER(TRIM("County")) AS county_norm,
        LOWER(TRIM("State full")) AS state_norm,
        "City" AS city,
        "County" AS county,
        "State full" AS state
    FROM {{ ref("us_cities_states_counties") }}
),

matched AS (
-- Join pairings
    SELECT
        n.*,

        gs.city AS city_from_city_state,
        gs.county AS county_from_city_state,
        gs.state AS state_from_city_state,

        gc.city AS city_from_city_county,
        gc.county AS county_from_city_county,
        gc.state AS state_from_city_county

    FROM normalized n

    LEFT JOIN geo gs
        ON n.part_1_norm = gs.city_norm
       AND n.part_2_norm = gs.state_norm

    LEFT JOIN geo gc
        ON n.part_1_norm = gc.city_norm
       AND n.part_2_norm = gc.county_norm
),

final AS (

    SELECT
        id,
        title,
        description,
        contract_time,
        salary_min,
        salary_max,
        latitude,
        longitude,
        raw_location AS location,
        company,
        category_label,
        created,
        salary_mid,

        /* COUNTRY LOGIC */
        'US' AS country,

        /* CITY */
        CASE
            WHEN raw_location_norm = 'us' THEN NULL
            ELSE COALESCE(
                city_from_city_state,
                city_from_city_county,
                part_1
            )
        END AS city,

        /* COUNTY */
        CASE
            WHEN raw_location_norm = 'us' THEN NULL

            WHEN county_from_city_county IS NOT NULL THEN county_from_city_county
            WHEN county_from_city_state IS NOT NULL THEN county_from_city_state
            WHEN part_2_norm LIKE '%county%' THEN part_2

            ELSE NULL
        END AS county,

        /* STATE */
        CASE
            WHEN raw_location_norm = 'us' THEN NULL
            ELSE COALESCE(
                state_from_city_state,
                state_from_city_county
            )
        END AS state,

        CASE
            WHEN raw_location_norm = 'us' THEN 'COUNTRY_ONLY'

            WHEN city_from_city_state IS NOT NULL THEN 'CITY_STATE'
            WHEN city_from_city_county IS NOT NULL THEN 'CITY_COUNTY'
            WHEN part_2_norm LIKE '%county%' THEN 'UNMATCHED_CITY_COUNTY'

            ELSE 'UNMATCHED'
        END AS location_match_type

    FROM matched
)

SELECT
    CAST(id AS VARCHAR) AS posting_id,
    LOWER(TRIM(title)) AS job_title,
    TRIM(description) AS description,
    LOWER(TRIM(contract_time)) AS work_type,
    LOWER(TRIM(company)) AS company,
    LOWER(TRIM(category_label)) AS category_label,
    CAST(ROUND(salary_max) AS INTEGER) AS salary_max,
    CAST(ROUND(salary_mid) AS INTEGER) AS salary_mid,
    CAST(ROUND(salary_min) AS INTEGER) AS salary_min,
    LOWER(TRIM(county)) AS county,
    LOWER(TRIM(city)) AS city,
    LOWER(TRIM(state)) AS state,
    LOWER(TRIM(location)) AS location,
    latitude,
    longitude,
    created
FROM final
