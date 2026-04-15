-- gold_occupation_skills_profile: O*NET formal tech requirements vs market demand
-- Grain: onet_soc_code × skill_name
-- Dashboard: "Skills Tracker" — gap analysis view
--
-- Uses dim_technology_skills (concrete tools like Python, SQL Server)
-- rather than dim_skills (abstract categories like Programming)
-- because technology names match posting skill tags much better.

{{
    config(
        materialized='table',
        tags=['gold']
    )
}}

WITH qualified_socs AS (
    -- Only analyze SOC codes with enough matched postings
    SELECT
        onet_soc_code,
        COUNT(DISTINCT posting_id)                  AS total_postings_for_soc
    FROM {{ ref("fct_job_postings") }}
    WHERE soc_match_type = 'exact_match'
      AND onet_soc_code IS NOT NULL
    GROUP BY 1
    HAVING COUNT(DISTINCT posting_id) >= 10
),

-- O*NET side: what does the government say each occupation uses?
onet_tech AS (
    SELECT
        ts.onet_soc_code,
        LOWER(TRIM(ts.technology_name))             AS skill_name,
        ts.is_hot_technology,
        ts.is_in_demand,
        ts.commodity_title
    FROM {{ ref("dim_technology_skills") }} AS ts
    INNER JOIN qualified_socs AS qs
        USING(onet_soc_code)
),

-- Market side: what do employers actually ask for?
market_demand AS (
    SELECT
        fp.onet_soc_code,
        ps.skill_name,
        COUNT(DISTINCT ps.posting_id)               AS posting_mention_count
    FROM {{ ref("fct_posting_skills") }} AS ps
    INNER JOIN {{ ref("fct_job_postings") }} AS fp
        ON ps.posting_id = fp.posting_id
       AND ps.data_source = fp.data_source
    INNER JOIN qualified_socs AS qs
        ON fp.onet_soc_code = qs.onet_soc_code
    WHERE fp.soc_match_type = 'exact_match'
      AND ps.skill_name IS NOT NULL
      AND LENGTH(ps.skill_name) >= 2
    GROUP BY 1, 2
),

-- Full outer join: see skills from both sides
combined AS (
    SELECT
        COALESCE(o.onet_soc_code, m.onet_soc_code) AS onet_soc_code,
        COALESCE(o.skill_name, m.skill_name)        AS skill_name,

        -- O*NET side
        o.is_hot_technology                         AS onet_is_hot_technology,
        o.is_in_demand                              AS onet_is_in_demand,
        o.commodity_title                           AS onet_commodity_title,
        CASE WHEN o.skill_name IS NOT NULL
            THEN TRUE ELSE FALSE
        END                                         AS in_onet,

        -- Market side
        COALESCE(m.posting_mention_count, 0)        AS posting_mention_count,
        CASE WHEN m.skill_name IS NOT NULL
            THEN TRUE ELSE FALSE
        END                                         AS in_market

    FROM onet_tech AS o
    FULL OUTER JOIN market_demand AS m
        ON o.onet_soc_code = m.onet_soc_code
       AND o.skill_name = m.skill_name
),

with_pct AS (
    SELECT
        c.*,
        qs.total_postings_for_soc,
        ROUND(
            c.posting_mention_count * 1.0
            / NULLIF(qs.total_postings_for_soc, 0),
            4
        )                                           AS posting_pct_of_postings
    FROM combined AS c
    INNER JOIN qualified_socs AS qs
        USING(onet_soc_code)
)

SELECT
    wp.onet_soc_code,
    occ.occupation_title,
    wp.skill_name,
    wp.in_onet,
    wp.in_market,
    wp.onet_is_hot_technology,
    wp.onet_is_in_demand,
    wp.onet_commodity_title,
    wp.posting_mention_count,
    wp.total_postings_for_soc,
    wp.posting_pct_of_postings,

    -- Gap classification
    CASE
        WHEN wp.in_market AND NOT wp.in_onet
            THEN 'market_only'
        WHEN wp.in_onet AND NOT wp.in_market
            THEN 'onet_only'
        WHEN wp.in_onet AND wp.in_market
            THEN 'aligned'
        ELSE 'unknown'
    END                                             AS gap_direction

FROM with_pct AS wp

INNER JOIN {{ ref("dim_occupations") }} AS occ
    ON wp.onet_soc_code = occ.onet_soc_code

-- Filter out ultra-low signal: market-only skills with < 3 mentions
WHERE NOT (wp.in_market AND NOT wp.in_onet AND wp.posting_mention_count < 3)

ORDER BY wp.onet_soc_code, wp.posting_mention_count DESC
