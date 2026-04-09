
WITH renamed_recasted AS (
	SELECT
		"O*NET-SOC Code" AS onet_soc_code,
		"Example" AS technology_name,
		"Commodity Code" AS commodity_code,
		TRIM("Commodity Title") AS commodity_title,
		CAST(CASE "Hot Technology" 
			WHEN 'Y' THEN 1
			WHEN 'N' THEN 0
		END AS BOOLEAN) AS is_hot_technology,
		CAST(CASE "In Demand"
			WHEN 'Y' THEN 1
			WHEN 'N' THEN 0
		END AS BOOLEAN) AS is_in_demand
	FROM {{ ref("stg_onet__technology_skills") }}
)

SELECT
	*
FROM renamed_recasted
