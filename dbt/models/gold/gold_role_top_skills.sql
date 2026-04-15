-- gold_role_top_skills: top skills per occupation ranked by market demand
-- Grain: onet_soc_code × skill_name (ranked, top 20)
-- Dashboard: "Role Deep-Dive" — horizontal bar chart of skills per role

{{
    config(
        materialized='table',
        tags=['gold']
    )
}}

WITH posting_skill_counts AS (
    -- Count how many postings per SOC code mention each skill
    SELECT
        fp.onet_soc_code,
        ps.skill_name,
        COUNT(DISTINCT ps.posting_id)               AS mention_count
    FROM {{ ref("fct_posting_skills") }} AS ps
    INNER JOIN {{ ref("fct_job_postings") }} AS fp
        ON ps.posting_id = fp.posting_id
       AND ps.data_source = fp.data_source
    WHERE fp.soc_match_type = 'exact_match'
      AND fp.onet_soc_code IS NOT NULL
      AND ps.skill_name IS NOT NULL
      AND LENGTH(ps.skill_name) >= 2
    GROUP BY 1, 2
),

-- Total postings per SOC (denominator for percentage)
soc_posting_counts AS (
    SELECT
        onet_soc_code,
        COUNT(DISTINCT posting_id)                  AS total_postings_for_soc
    FROM {{ ref("fct_job_postings") }}
    WHERE soc_match_type = 'exact_match'
      AND onet_soc_code IS NOT NULL
    GROUP BY 1
    HAVING COUNT(DISTINCT posting_id) >= 10
),

ranked AS (
    SELECT
        psc.onet_soc_code,
        psc.skill_name,
        psc.mention_count,
        spc.total_postings_for_soc,
        ROUND(
            psc.mention_count * 1.0 / spc.total_postings_for_soc,
            4
        )                                           AS pct_of_postings,
        ROW_NUMBER() OVER (
            PARTITION BY psc.onet_soc_code
            ORDER BY psc.mention_count DESC
        )                                           AS skill_rank
    FROM posting_skill_counts AS psc
    INNER JOIN soc_posting_counts AS spc
        USING(onet_soc_code)
    WHERE psc.mention_count >= 3
)

SELECT
    r.onet_soc_code,
    occ.occupation_title,
    r.skill_name,
    r.skill_rank,
    r.mention_count,
    r.total_postings_for_soc,
    r.pct_of_postings,

    -- Bring in O*NET formal ratings where possible
    -- This join is lossy: LinkedIn tags like "python" won't match
    -- O*NET skill names like "Programming" — NULLs are expected
    ds.importance_value                             AS onet_importance_value,
    ds.level_value                                  AS onet_level_value

FROM ranked AS r

INNER JOIN {{ ref("dim_occupations") }} AS occ
    ON r.onet_soc_code = occ.onet_soc_code

LEFT JOIN {{ ref("dim_skills") }} AS ds
    ON r.onet_soc_code = ds.onet_soc_code
   AND LOWER(TRIM(ds.skill_name)) = r.skill_name

WHERE r.skill_rank <= 20

ORDER BY r.onet_soc_code, r.skill_rank
