
WITH joined_renamed AS (
	SELECT 
		"O*NET-SOC Code" AS onet_soc_code,
		TRIM("Title") AS occupation_title,
		TRIM("Description") AS occupation_description,
		"Job Zone" AS job_zone
	FROM {{ ref("stg_onet__occupations") }}
	INNER JOIN {{ ref("stg_onet__job_zones") }}
	USING("O*NET-SOC Code")
)

SELECT
	*
FROM joined_renamed
