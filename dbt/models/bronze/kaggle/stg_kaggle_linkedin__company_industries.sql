with source as (
  select * from {{ parquet_path('kaggle/linkedin_jobs/companies', 'linkedin_company_industries.parquet') }}
)

select *
from source
