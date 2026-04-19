
WITH education_renamed AS (
	-- Rename columns, trim text columns, do not include rows where the data source is unreliable 'Recommend Supress' <> 'Y'
	SELECT
		"O*NET-SOC Code" AS onet_soc_code,
		TRIM("Element ID") AS element_id,
		TRIM("Element Name") AS element_name,
		TRIM("Scale ID") AS scale_id,
    {{ col("Category") }} AS category,
		"Data Value" AS data_value
	FROM {{ ref("stg_onet__education_training_experience") }}
	WHERE "Recommend Suppress" <> 'Y'
), ete_renamed AS (
-- Rename the columns for the table we are going to join to make it easier
  SELECT
      TRIM("Element ID") AS element_id,
      TRIM("Element Name")AS element_name,
      TRIM("Scale ID") AS scale_id,
      {{ safe_numeric_cast(col("Category"), "INT") }} AS category,
      TRIM({{ col("Category Description") }}) AS category_description
  FROM {{ ref("stg_onet__ete_categories") }}
), joined_tables AS (
-- We are solely getting the category descriptions to help us pivot the scale_id, filtering for education_requirements only
  SELECT
      edu.onet_soc_code,
      edu.element_id,
      edu.element_name,
      edu.scale_id,
      edu.category,
      edu.data_value,
      ete.category_description
  FROM education_renamed AS edu
  INNER JOIN ete_renamed AS ete
      ON edu.element_id = ete.element_id
     AND edu.element_name = ete.element_name
     AND edu.scale_id = ete.scale_id
     AND edu.category = ete.category
  WHERE edu.scale_id = 'RL'
), pivoted AS (
-- Pivot categories into their respective percentages
	SELECT
		onet_soc_code,
		CASE category WHEN 1  THEN data_value END AS pct_less_than_high_school,
		CASE category WHEN 2  THEN data_value END AS pct_high_school_diploma,
		CASE category WHEN 3  THEN data_value END AS pct_postsecondary_certificate,
		CASE category WHEN 4  THEN data_value END AS pct_some_college,
		CASE category WHEN 5  THEN data_value END AS pct_associates_degree,
		CASE category WHEN 6  THEN data_value END AS pct_bachelors_degree,
		CASE category WHEN 7  THEN data_value END AS pct_post_baccalaureate_certificate,
		CASE category WHEN 8  THEN data_value END AS pct_masters_degree,
		CASE category WHEN 9  THEN data_value END AS pct_post_masters_certificate,
		CASE category WHEN 10 THEN data_value END AS pct_first_professional_degree,
		CASE category WHEN 11 THEN data_value END AS pct_doctoral_degree,
		CASE category WHEN 12 THEN data_value END AS pct_post_doctoral_training
	FROM joined_tables
), pivoted_collapsed AS (
-- Collapse the pivoted columns using SUM() + GROUP BY
-- We use SUM() just to be safe in case there are duplicate entries per onet_soc_code another option is MAX()
-- We use COALESCE to remove NULL entries 
	SELECT 
		onet_soc_code,
		SUM(COALESCE(pct_less_than_high_school, 0)) pct_less_than_high_school,
		SUM(COALESCE(pct_high_school_diploma, 0)) pct_high_school_diploma,
		SUM(COALESCE(pct_postsecondary_certificate, 0)) pct_postsecondary_certificate,
		SUM(COALESCE(pct_some_college, 0)) pct_some_college,
		SUM(COALESCE(pct_associates_degree, 0)) pct_associates_degree,
		SUM(COALESCE(pct_bachelors_degree, 0)) pct_bachelors_degree,
		SUM(COALESCE(pct_post_baccalaureate_certificate, 0)) pct_post_baccalaureate_certificate,
		SUM(COALESCE(pct_masters_degree, 0)) pct_masters_degree,
		SUM(COALESCE(pct_post_masters_certificate, 0)) pct_post_masters_certificate,
		SUM(COALESCE(pct_first_professional_degree, 0)) pct_first_professional_degree,
		SUM(COALESCE(pct_doctoral_degree, 0)) pct_doctoral_degree,
		SUM(COALESCE(pct_post_doctoral_training, 0)) pct_post_doctoral_training
	FROM pivoted
	GROUP BY onet_soc_code
)


SELECT
	*
FROM pivoted_collapsed





