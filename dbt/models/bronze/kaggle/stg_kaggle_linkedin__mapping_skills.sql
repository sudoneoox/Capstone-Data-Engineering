with source as (
  select * from {{ parquet_path('kaggle/linkedin_jobs/mappings', 'linkedin_mapping_skills.parquet') }}
)

select * 
from source
