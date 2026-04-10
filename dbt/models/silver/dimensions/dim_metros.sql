-- Utilized https://api.census.gov/data/2023/acs/acs5/profile/variables.html as reference
WITH renamed_casted AS (
	SELECT
	CAST("metropolitan statistical area/micropolitan statistical area" AS VARCHAR(5)) AS cbsa_fips,
	TRIM("NAME") AS metro_name,
	CAST("DP03_0003PE" 	AS DOUBLE) 	AS pct_civilian_labor_force,
	CAST("DP03_0004PE" 	AS DOUBLE) 	AS pct_employed,
	CAST("DP03_0005PE" 	AS DOUBLE) 	AS pct_unemployed,
	CAST("DP03_0009PE" 	AS DOUBLE) 	AS unemployment_rate,
	CAST("DP03_0021PE" 	AS DOUBLE) 	AS pct_public_transportation_commute,
	CAST("DP03_0024PE" 	AS DOUBLE) 	AS pct_work_from_home,
	CAST("DP03_0025E" 	AS DOUBLE) 	AS mean_commute_minutes,
	CAST("DP03_0027PE" 	AS DOUBLE) 	AS pct_mgmt_business_science_arts,
	CAST("DP03_0039PE"  AS DOUBLE) 	AS pct_information_industry,
	CAST("DP03_0040PE"  AS DOUBLE) 	AS pct_finance_real_estate_rental_leasing,
	CAST("DP03_0041PE"  AS DOUBLE) 	AS pct_professional_scientific_management_admin_waste,
	CAST("DP02_0067PE"	AS DOUBLE) 	AS pct_high_school_or_higher,
	CAST("DP02_0068PE"  AS DOUBLE) 	AS pct_bachelors_or_higher,
	CAST("DP02_0154PE" 	AS DOUBLE) 	AS pct_broadband,
	CAST("DP03_0062E" 	AS INT) 	AS median_household_income,
	CAST("DP03_0093E" 	AS INT) 	AS median_earnings_male_ft_yr_round,
	CAST("DP03_0094E" 	AS INT) 	AS median_earnings_female_ft_yr_round,
	CAST("DP03_0119PE" 	AS DOUBLE) 	AS poverty_rate_families,
	CAST("DP03_0128PE" 	AS DOUBLE)	AS poverty_rate_all_people
	FROM {{ ref("stg_acs__metro_profiles") }}
)

SELECT
	*
FROM renamed_casted
