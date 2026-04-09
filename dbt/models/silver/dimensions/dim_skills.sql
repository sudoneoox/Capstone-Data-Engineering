WITH renamed_pivoted AS (
  -- Pivot Scale ID into two separate columns and their corresponding value
	SELECT 
		"O*NET-SOC Code" AS onet_soc_code,
		"Element ID" AS element_id,
		"Element Name" AS skill_name,
		CASE "Scale ID" 
			WHEN 'IM' THEN "Data Value"
		END AS importance_value,
		CASE "Scale ID"
			WHEN 'LV' THEN "Data Value"
		END AS level_value,
	FROM {{ ref("stg_onet__skills") }}
	WHERE "Recommend Suppress" != 'Y' -- Do not include rows where suppress is recommended
), cleaned AS ( 
  -- Trim Text Values
	SELECT 
		onet_soc_code,
		TRIM(element_id) AS element_id,
		TRIM(skill_name) AS skill_name,
		importance_value,
		level_value
	FROM renamed_pivoted
), remove_dup_columns AS (
  -- Use lead as a helper for the next transformation
	SELECT 
		*,
		LEAD(level_value) OVER(PARTITION BY onet_soc_code, element_id) AS level_value_new
	FROM cleaned
), source AS (
  -- Move importance_value and level_value to the same row, remove duplicated columns
	SELECT
		onet_soc_code, 
		element_id,
		skill_name,
		importance_value,
    level_value_new AS level_value
	FROM remove_dup_columns
	WHERE level_value_new IS NOT NULL
)

SELECT *
FROM source
