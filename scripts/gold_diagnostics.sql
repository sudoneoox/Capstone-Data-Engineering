
-- 1. Market snapshot — all rows (only 4)
COPY (
    SELECT * FROM gold.gold_market_snapshot
    ORDER BY total_postings DESC
) TO 'data/diagnostics/01_market_snapshot.csv' (HEADER, DELIMITER ',');

-- 2. Market trends — all rows (only 3)
COPY (
    SELECT * FROM gold.gold_market_trends
    ORDER BY data_source, report_month
) TO 'data/diagnostics/02_market_trends.csv' (HEADER, DELIMITER ',');

-- 3. Economic dashboard — summary per indicator
COPY (
    SELECT
        category,
        series_name,
        source_system,
        COUNT(*) AS months,
        MIN(report_month) AS earliest,
        MAX(report_month) AS latest,
        ROUND(MIN(value), 2) AS min_val,
        ROUND(AVG(value), 2) AS avg_val,
        ROUND(MAX(value), 2) AS max_val
    FROM gold.gold_economic_dashboard
    GROUP BY 1, 2, 3
    ORDER BY category, series_name
) TO 'data/diagnostics/03_economic_summary.csv' (HEADER, DELIMITER ',');

-- 4. Role analysis — all rows sorted by volume
COPY (
    SELECT
        onet_soc_code,
        occupation_title,
        job_zone,
        total_postings,
        postings_last_30d,
        postings_with_salary,
        median_salary,
        salary_p25,
        salary_p75,
        pct_remote,
        most_common_experience_level,
        most_common_work_type,
        pct_bachelors_degree,
        pct_masters_degree
    FROM gold.gold_role_analysis
    ORDER BY total_postings DESC
) TO 'data/diagnostics/04_role_analysis.csv' (HEADER, DELIMITER ',');

-- 5. Role top skills — top 5 skills per role (top 20 roles by volume)
COPY (
    SELECT
        occupation_title,
        skill_name,
        skill_rank,
        mention_count,
        ROUND(pct_of_postings, 4) AS pct_of_postings,
        onet_importance_value,
        onet_level_value
    FROM gold.gold_role_top_skills
    WHERE onet_soc_code IN (
        SELECT onet_soc_code FROM gold.gold_role_analysis
        ORDER BY total_postings DESC LIMIT 20
    )
    AND skill_rank <= 5
    ORDER BY occupation_title, skill_rank
) TO 'data/diagnostics/05_role_top_skills.csv' (HEADER, DELIMITER ',');

-- 6. Skills demand — top 30 skills overall + trending flag
COPY (
    SELECT
        skill_name,
        SUM(mention_count) AS total_mentions,
        SUM(distinct_postings) AS total_distinct_postings,
        COUNT(DISTINCT report_month) AS months_active,
        ROUND(AVG(pct_of_postings), 4) AS avg_pct,
        COUNT(*) FILTER (WHERE is_trending) AS months_trending
    FROM gold.gold_skills_demand
    GROUP BY 1
    ORDER BY total_mentions DESC
    LIMIT 30
) TO 'data/diagnostics/06_skills_demand_top30.csv' (HEADER, DELIMITER ',');

-- 7. Skills quality check — suspected non-skills
COPY (
    SELECT DISTINCT skill_name, SUM(mention_count) AS total_mentions
    FROM gold.gold_skills_demand
    WHERE skill_name ILIKE '%degree%'
       OR skill_name ILIKE '%years%'
       OR skill_name ILIKE '%older%'
       OR skill_name ILIKE '%attributes%'
       OR skill_name ILIKE '%required%'
       OR skill_name ILIKE '%license%'
       OR skill_name ILIKE '%must%'
       OR skill_name ILIKE '%ability to%'
       OR LENGTH(skill_name) > 40
    GROUP BY 1
    ORDER BY total_mentions DESC
) TO 'data/diagnostics/07_skills_quality_check.csv' (HEADER, DELIMITER ',');

-- 8. Metro comparison — all rows
COPY (
    SELECT
        state,
        total_postings,
        median_salary,
        salary_to_income_ratio,
        pct_remote,
        cbsa_fips,
        metro_name,
        median_household_income,
        pct_bachelors_or_higher,
        pct_work_from_home,
        pct_information_industry
    FROM gold.gold_metro_comparison
    ORDER BY total_postings DESC
) TO 'data/diagnostics/08_metro_comparison.csv' (HEADER, DELIMITER ',');

-- 9. Occupation skills profile — gap summary
COPY (
    SELECT
        gap_direction,
        COUNT(*) AS entries,
        COUNT(DISTINCT onet_soc_code) AS occupations,
        COUNT(DISTINCT skill_name) AS unique_skills,
        ROUND(AVG(posting_mention_count), 1) AS avg_mentions,
        ROUND(AVG(posting_pct_of_postings), 4) AS avg_pct
    FROM gold.gold_occupation_skills_profile
    GROUP BY 1
    ORDER BY entries DESC
) TO 'data/diagnostics/09_skills_gap_summary.csv' (HEADER, DELIMITER ',');

-- 10. Occupation skills profile — top 20 market_only skills (emerging tools)
COPY (
    SELECT
        occupation_title,
        skill_name,
        posting_mention_count,
        ROUND(posting_pct_of_postings, 4) AS pct
    FROM gold.gold_occupation_skills_profile
    WHERE gap_direction = 'market_only'
    ORDER BY posting_mention_count DESC
    LIMIT 20
) TO 'data/diagnostics/10_emerging_market_only_skills.csv' (HEADER, DELIMITER ',');

-- 11. Row counts for all gold tables
COPY (
    SELECT 'gold_market_snapshot' AS model, COUNT(*) AS rows FROM gold.gold_market_snapshot
    UNION ALL SELECT 'gold_market_trends', COUNT(*) FROM gold.gold_market_trends
    UNION ALL SELECT 'gold_economic_dashboard', COUNT(*) FROM gold.gold_economic_dashboard
    UNION ALL SELECT 'gold_role_analysis', COUNT(*) FROM gold.gold_role_analysis
    UNION ALL SELECT 'gold_role_top_skills', COUNT(*) FROM gold.gold_role_top_skills
    UNION ALL SELECT 'gold_skills_demand', COUNT(*) FROM gold.gold_skills_demand
    UNION ALL SELECT 'gold_metro_comparison', COUNT(*) FROM gold.gold_metro_comparison
    UNION ALL SELECT 'gold_occupation_skills_profile', COUNT(*) FROM gold.gold_occupation_skills_profile
    ORDER BY model
) TO 'data/diagnostics/00_row_counts.csv' (HEADER, DELIMITER ',');
