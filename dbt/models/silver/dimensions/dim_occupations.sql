
WITH joined_renamed AS (
    SELECT 
        o.{{ col("O*NET-SOC Code") }} AS onet_soc_code,
        TRIM(o.{{ col("Title") }}) AS occupation_title,
        TRIM(o.{{ col("Description") }}) AS occupation_description,
        j.{{ col("Job Zone") }} AS job_zone
    FROM {{ ref("stg_onet__occupations") }} o
    INNER JOIN {{ ref("stg_onet__job_zones") }} j
        ON o.{{ col("O*NET-SOC Code") }} = j.{{ col("O*NET-SOC Code") }}
)
SELECT * FROM joined_renamed
