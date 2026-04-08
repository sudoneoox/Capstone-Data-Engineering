with source as (
  select * from {{ parquet_path('kaggle/linkedin_jobs/jobs', 'linkedin_job_skills.parquet') }}
)

select * 
from source
