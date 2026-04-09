with source as (
  select * from {{ parquet_path('kaggle/linkedin_jobs', 'linkedin_postings.parquet') }}
)

select *
from source
