with source as (
  select * from {{ parquet_path('onet', 'job_zones.parquet') }}
)

select *
from source
