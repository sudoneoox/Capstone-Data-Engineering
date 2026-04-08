with source as (
  select * from {{ parquet_path('kaggle/linkedin_jobs_2024_large', 'linkedin_1_3m_job_postings.parquet') }}
)

select *
from source
