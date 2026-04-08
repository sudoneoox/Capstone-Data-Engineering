with source as (
  select * from {{ parquet_path('kaggle/linkedin_jobs/companies', 'linkedin_employee_counts.parquet') }}
)

select *
from source
