WITH renamed_pivoted AS (
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
	FROM {{ ref("stg_onet__knowledge") }}
	WHERE "Recommend Suppress" != 'Y' -- Do not include suppressed rows
), cleaned AS (
	SELECT 
		onet_soc_code,
		TRIM(element_id) AS element_id,
		TRIM(skill_name) AS skill_name,
		importance_value,
		level_value
	FROM renamed_pivoted
), remove_dup_columns AS (
	SELECT 
		*,
		LEAD(level_value) OVER(PARTITION BY onet_soc_code, element_id) AS level_value_new
	FROM cleaned
), source AS (
	SELECT
		onet_soc_code, 
		element_id,
		skill_name,
		importance_value,
		level_value_new AS level_value
	FROM remove_dup_columns
	WHERE level_value_new IS NOT NULL
)

SELECT 
	*
FROM source
