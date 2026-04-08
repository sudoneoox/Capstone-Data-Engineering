with source as (
  select * from {{ parquet_path('kaggle/linkedin_jobs/jobs', 'linkedin_job_industries.parquet') }}
)

select * 
from source
