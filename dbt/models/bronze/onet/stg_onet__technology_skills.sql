with source as (
  select * from {{ parquet_path('onet', 'technology_skills.parquet') }}
)

select *
from source
