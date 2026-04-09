WITH source AS (
  SELECT * FROM {{ ref("seed_onet__ete_categories") }}
)

SELECT *
FROM source
