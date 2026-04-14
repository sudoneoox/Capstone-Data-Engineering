-- dim_knowledge: O*NET knowledge domain importance + level per occupation
-- Same pivot pattern as dim_skills

WITH pivoted AS (
    SELECT
        "O*NET-SOC Code"    AS onet_soc_code,
        TRIM("Element ID")  AS element_id,
        TRIM("Element Name") AS skill_name,
        MAX(CASE "Scale ID" WHEN 'IM' THEN "Data Value" END) AS importance_value,
        MAX(CASE "Scale ID" WHEN 'LV' THEN "Data Value" END) AS level_value
    FROM {{ ref("stg_onet__knowledge") }}
    WHERE "Recommend Suppress" != 'Y'
    GROUP BY 1, 2, 3
)

SELECT *
FROM pivoted
WHERE importance_value IS NOT NULL
  AND level_value IS NOT NULL
