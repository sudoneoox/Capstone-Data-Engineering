with source as (
  select * from {{ parquet_path('onet', 'emerging_tasks.parquet') }}
)

select * 
from source
