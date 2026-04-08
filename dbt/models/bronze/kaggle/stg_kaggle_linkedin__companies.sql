with source as (
  select * from {{ parquet_path('kaggle/linkedin_jobs/companies', 'linkedin_companies.parquet') }}
)

select *
from source
