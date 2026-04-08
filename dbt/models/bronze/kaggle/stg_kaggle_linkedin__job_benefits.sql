with source as (
  select * from {{ parquet_path('kaggle/linkedin_jobs/jobs', 'linkedin_benefits.parquet') }}
)

select * 
from source
