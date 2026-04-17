-- Utilized https://api.census.gov/data/2023/acs/acs5/profile/variables.html as reference
WITH renamed_casted AS (
    SELECT
        CAST({{ col("metropolitan statistical area/micropolitan statistical area") }} AS {{ dbt.type_string() }}) AS cbsa_fips,
        TRIM({{ col("NAME") }}) AS metro_name,
        {{ safe_numeric_cast(col("DP03_0003PE"), "DOUBLE") }} AS pct_civilian_labor_force,
        {{ safe_numeric_cast(col("DP03_0004PE"), "DOUBLE") }} AS pct_employed,
        {{ safe_numeric_cast(col("DP03_0005PE"), "DOUBLE") }} AS pct_unemployed,
        {{ safe_numeric_cast(col("DP03_0009PE"), "DOUBLE") }} AS unemployment_rate,
        {{ safe_numeric_cast(col("DP03_0021PE"), "DOUBLE") }} AS pct_public_transportation_commute,
        {{ safe_numeric_cast(col("DP03_0024PE"), "DOUBLE") }} AS pct_work_from_home,
        {{ safe_numeric_cast(col("DP03_0025E"), "DOUBLE") }} AS mean_commute_minutes,
        {{ safe_numeric_cast(col("DP03_0027PE"), "DOUBLE") }} AS pct_mgmt_business_science_arts,
        {{ safe_numeric_cast(col("DP03_0039PE"), "DOUBLE") }} AS pct_information_industry,
        {{ safe_numeric_cast(col("DP03_0040PE"), "DOUBLE") }} AS pct_finance_real_estate_rental_leasing,
        {{ safe_numeric_cast(col("DP03_0041PE"), "DOUBLE") }} AS pct_professional_scientific_management_admin_waste,
        {{ safe_numeric_cast(col("DP02_0067PE"), "DOUBLE") }} AS pct_high_school_or_higher,
        {{ safe_numeric_cast(col("DP02_0068PE"), "DOUBLE") }} AS pct_bachelors_or_higher,
        {{ safe_numeric_cast(col("DP02_0154PE"), "DOUBLE") }} AS pct_broadband,
        {{ safe_numeric_cast(col("DP03_0062E"), "INT") }} AS median_household_income,
        {{ safe_numeric_cast(col("DP03_0093E"), "INT") }} AS median_earnings_male_ft_yr_round,
        {{ safe_numeric_cast(col("DP03_0094E"), "INT") }} AS median_earnings_female_ft_yr_round,
        {{ safe_numeric_cast(col("DP03_0119PE"), "DOUBLE") }} AS poverty_rate_families,
        {{ safe_numeric_cast(col("DP03_0128PE"), "DOUBLE") }} AS poverty_rate_all_people
    FROM {{ ref("stg_acs__metro_profiles") }}
)

SELECT *
FROM renamed_casted
