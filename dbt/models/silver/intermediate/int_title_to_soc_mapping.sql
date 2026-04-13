-- int_title_to_soc_mapping
-- Builds a lookup table mapping lowercased job titles to O*NET-SOC codes
-- Sources: O*NET alternate titles (~70K) + main occupation titles (~886)
--
-- Usage: downstream models LEFT JOIN on LOWER(job_title) = title_lower
-- for exact matching. For fuzzy matching, use CONTAINS() or Levenshtein
-- in a more advanced version.
--
-- NOTE: This is the single hardest join in the project. Start with exact
-- matching and measure the match rate. Even 30-40% is useful.

WITH alternate_titles AS (
    SELECT
        "O*NET-SOC Code"                            AS onet_soc_code,
        LEFT("O*NET-SOC Code", 7)                   AS soc_code,
        LOWER(TRIM("Alternate Title"))              AS title_lower,
        LOWER(TRIM("Short Title"))                  AS short_title_lower,
        'alternate_title'                           AS title_source
    FROM {{ ref("stg_onet__alternate_titles") }}
    WHERE "Alternate Title" IS NOT NULL
      AND TRIM("Alternate Title") != ''
),

occupation_titles AS (
    -- Also include the canonical occupation titles themselves
    SELECT
        "O*NET-SOC Code"                            AS onet_soc_code,
        LEFT("O*NET-SOC Code", 7)                   AS soc_code,
        LOWER(TRIM("Title"))                        AS title_lower,
        LOWER(TRIM("Title"))                        AS short_title_lower,
        'occupation_title'                          AS title_source
    FROM {{ ref("stg_onet__occupations") }}
    WHERE "Title" IS NOT NULL
),

combined AS (
    SELECT onet_soc_code, soc_code, title_lower, short_title_lower, title_source
    FROM alternate_titles
    UNION ALL
    SELECT onet_soc_code, soc_code, title_lower, short_title_lower, title_source
    FROM occupation_titles
),

-- Deduplicate: same title text might appear under multiple SOC codes
-- Keep the one from occupation_title source first (canonical), then alternate
deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY title_lower
            ORDER BY
                CASE title_source WHEN 'occupation_title' THEN 0 ELSE 1 END,
                onet_soc_code
        ) AS rn
    FROM combined
    WHERE title_lower IS NOT NULL
      AND LENGTH(title_lower) >= 3
)

SELECT
    onet_soc_code,
    soc_code,
    title_lower,
    short_title_lower,
    title_source
FROM deduplicated
WHERE rn = 1
