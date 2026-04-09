WITH date_spine AS (

  {{ dbt_utils.date_spine(
    datepart="DAY",
    start_date="CAST('2015-01-01' AS DATE)",
    end_date="CAST(CURRENT_DATE + 1 AS DATE)"
  ) }}

)

SELECT
  CAST(date_day AS date) AS full_date,
  CAST({{ date_format_crossdb("CAST(date_day AS DATE)", "date_key") }} AS INTEGER) AS date_key,
  EXTRACT(YEAR FROM date_day) AS year,
  EXTRACT(MONTH FROM date_day) AS month,
  EXTRACT(QUARTER FROM date_day) AS quarter,
  {{ day_of_week_iso("CAST(date_day AS DATE)") }} AS day_of_week,
  {{ is_weekend("CAST(date_day AS DATE)") }} AS is_weekend,
  {{ date_format_crossdb("cast(date_day as date)", "month_name") }} as month_name
FROM date_spine
ORDER BY full_date
