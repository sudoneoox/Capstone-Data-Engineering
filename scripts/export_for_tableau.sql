-- Export gold tables to CSV for dashboard consumption
-- Run: duckdb data/lakehouse_dev.duckdb < scripts/export_for_dashboard.sql

COPY (SELECT * FROM gold.gold_market_snapshot)
TO 'data/tableau/gold_market_snapshot.csv' (HEADER, DELIMITER ',');

COPY (SELECT * FROM gold.gold_market_trends)
TO 'data/tableau/gold_market_trends.csv' (HEADER, DELIMITER ',');

COPY (SELECT * FROM gold.gold_economic_dashboard)
TO 'data/tableau/gold_economic_dashboard.csv' (HEADER, DELIMITER ',');

COPY (SELECT * FROM gold.gold_role_analysis)
TO 'data/tableau/gold_role_analysis.csv' (HEADER, DELIMITER ',');

COPY (SELECT * FROM gold.gold_role_top_skills)
TO 'data/tableau/gold_role_top_skills.csv' (HEADER, DELIMITER ',');

COPY (SELECT * FROM gold.gold_skills_demand)
TO 'data/tableau/gold_skills_demand.csv' (HEADER, DELIMITER ',');

COPY (SELECT * FROM gold.gold_metro_comparison)
TO 'data/tableau/gold_metro_comparison.csv' (HEADER, DELIMITER ',');

COPY (SELECT * FROM gold.gold_occupation_skills_profile)
TO 'data/tableau/gold_occupation_skills_profile.csv' (HEADER, DELIMITER ',');
