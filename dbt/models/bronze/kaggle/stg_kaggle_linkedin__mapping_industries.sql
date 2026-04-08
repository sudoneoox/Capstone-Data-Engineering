with source as (
  select * from {{ parquet_path('kaggle/linkedin_jobs/mappings', 'linkedin_mapping_industries.parquet') }}
)

select * 
from source
